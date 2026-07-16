//! Durable local state for the `tl fs` snapshot pipeline.
//!
//! This database is the local authority for mutation discovery and snapshot lifecycle state.
//! Correctness-changing transactions use [`Durability::Immediate`], so a successful mutation
//! record, generation freeze, prepare transition, publish request, or retirement has reached
//! durable storage before the method returns.
//!
//! The store intentionally persists opaque prepared-candidate bytes. Merkle-tree construction and
//! upload details belong to the preparer; this module owns the crash-safe lifecycle around them.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use redb::{
    Database, Durability, ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const SCHEMA_VERSION: u16 = 5;
const RECORD_VERSION: u16 = 1;
const COMPLETED_REQUEST_LIMIT: usize = 64;
/// Bounds immutable captures/prepared metadata while allowing scan/upload to overlap publication.
pub(crate) const MAX_INFLIGHT_GENERATIONS: usize = 4;

/// Default database filename when the caller places the store in a mount state directory.
pub(crate) const LOCAL_STATE_FILE: &str = "snapshot-state.redb";

const META_KEY: &str = "state";

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("local_state_meta_v1");
const GENERATIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("local_state_generations_v1");
const DIRTY: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("local_state_dirty_v1");
const RENAMES: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("local_state_renames_v1");
const INTENTS: TableDefinition<(u64, u64), &[u8]> =
    TableDefinition::new("local_state_mutation_intents_v1");
const FROZEN_CAPTURES: TableDefinition<u64, &[u8]> =
    TableDefinition::new("local_state_frozen_captures_v1");
const PREPARED: TableDefinition<u64, &[u8]> = TableDefinition::new("local_state_prepared_v1");
const REQUESTS: TableDefinition<&str, &[u8]> = TableDefinition::new("local_state_requests_v1");
const COMPLETED_REQUESTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("local_state_completed_requests_v1");
const RESTORE_OPERATIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("local_state_restore_operations_v1");
const SEALED: TableDefinition<&str, &[u8]> = TableDefinition::new("local_state_sealed_baseline_v1");
const ARTIFACTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("local_state_artifact_ownership_v1");

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalStateError {
    #[error("local snapshot state does not exist at {0}")]
    Missing(PathBuf),
    #[error("local snapshot state storage error: {0}")]
    Storage(String),
    #[error("local snapshot state is corrupt: {0}")]
    Corrupt(String),
    #[error(
        "local snapshot state belongs to a different mount (expected {expected:?}, found {found:?})"
    )]
    IdentityMismatch {
        expected: LocalStateIdentity,
        found: LocalStateIdentity,
    },
    #[error("unsupported local snapshot state schema version {found}; expected {expected}")]
    UnsupportedSchema { expected: u16, found: u16 },
    #[error("invalid repository path `{0}`")]
    InvalidPath(String),
    #[error("invalid local snapshot state transition: {0}")]
    InvalidTransition(String),
    #[error("conflicting durable record: {0}")]
    Conflict(String),
}

pub(crate) type Result<T> = std::result::Result<T, LocalStateError>;

/// Stable binding between one database file and one local writer.
///
/// `store_uuid` is minted once when the mount/binding is created and persisted alongside the
/// surrounding mount state. It prevents an accidentally copied/reused database from being adopted
/// merely because the human-readable filesystem name happens to match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LocalStateIdentity {
    pub project_id: String,
    pub filesystem: String,
    pub workspace_id: String,
    pub store_uuid: String,
}

impl LocalStateIdentity {
    #[cfg(test)]
    pub(crate) fn fresh(
        project_id: impl Into<String>,
        filesystem: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            filesystem: filesystem.into(),
            workspace_id: workspace_id.into(),
            store_uuid: Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MetaRecord {
    format_ver: u16,
    schema_ver: u16,
    identity: LocalStateIdentity,
    active_generation: u64,
    last_retired_generation: u64,
    next_mutation_sequence: u64,
    #[serde(default)]
    legacy_import_completed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum GenerationState {
    Open,
    Frozen,
    Prepared,
    PublishRequested,
    Published,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GenerationRecord {
    pub format_ver: u16,
    pub generation: u64,
    pub state: GenerationState,
    pub base_snapshot: Option<String>,
    pub published_snapshot: Option<String>,
    #[serde(default)]
    pub preparation_operation_id: Option<String>,
}

impl GenerationRecord {
    fn open(generation: u64, base_snapshot: Option<String>) -> Self {
        Self {
            format_ver: RECORD_VERSION,
            generation,
            state: GenerationState::Open,
            base_snapshot,
            published_snapshot: None,
            preparation_operation_id: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DirtyKind {
    Upsert,
    Delete,
}

/// Last mutation for one path in one snapshot generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DirtyPath {
    pub format_ver: u16,
    pub generation: u64,
    pub sequence: u64,
    pub path: String,
    pub kind: DirtyKind,
    /// Lowest modified byte. Structural changes and deletes use zero.
    pub min_write_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RenameIntent {
    pub format_ver: u16,
    pub generation: u64,
    pub sequence: u64,
    /// True lower source used by the metadata-only native rename.
    pub from: String,
    /// Overlay path that must be moved locally if the process crashes after the write-ahead
    /// record but before the namespace mutation completes. This differs from `from` for a
    /// composed chain such as `a -> b -> c`.
    pub local_from: String,
    pub to: String,
    /// Set only after the overlay rename/redirect operation succeeds and before the syscall
    /// returns. Unapplied write-ahead rows remain conservative dirty-path hints; recovery never
    /// replays them as namespace operations.
    pub applied: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MutationReceipt {
    pub generation: u64,
    pub sequence: u64,
}

/// One mutation fact to commit to the durable journal.
///
/// A batch is applied in slice order in one Immediate redb transaction. In particular, a rename
/// remains one indivisible source-delete/destination-upsert record, and mutations before or after
/// it in the batch observe the same ordering they had at the overlay boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MutationIntent {
    Upsert { path: String, min_write_offset: u64 },
    Delete { path: String },
    Rename { source: String, destination: String },
}

/// Exact, ordered write-ahead fact retained until its generation is durably retired.
///
/// The coalesced dirty-path and rename tables are preparation indexes. This row is the recovery
/// authority for namespace ordering and support inspection. Content mutations deliberately remain
/// conservative false positives after their WAL commit; namespace mutations additionally record
/// whether the overlay operation reached its post-mutation durability barrier.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MutationJournalRecord {
    pub format_ver: u16,
    pub generation: u64,
    pub sequence: u64,
    pub intent: MutationIntent,
    pub namespace_applied: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactOwnership {
    pub format_ver: u16,
    pub artifact_id: String,
    pub generation: u64,
    /// Store-relative path only. Absolute user paths and credentials are never persisted here.
    pub relative_path: String,
    pub kind: String,
    pub bytes: u64,
}

/// One mutation imported from the pre-database overlay state during the one-time cutover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LegacyMutation {
    Upsert { path: String, min_write_offset: u64 },
    Delete { path: String },
    Rename { from: String, to: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LegacyImport {
    pub base_snapshot: Option<String>,
    pub mutations: Vec<LegacyMutation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DirtyGeneration {
    pub generation: u64,
    pub state: GenerationState,
    pub base_snapshot: Option<String>,
    pub paths: Vec<DirtyPath>,
    pub renames: Vec<RenameIntent>,
}

/// Startup image used to seed the in-memory overlay cache without walking `upper/` or `wh/`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryDirtyState {
    pub active_generation: u64,
    pub maximum_mutation_sequence: u64,
    pub paths: Vec<DirtyPath>,
    pub renames: Vec<RenameIntent>,
    pub intents: Vec<MutationJournalRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FrozenGeneration {
    pub generation: u64,
    pub next_generation: u64,
    pub base_snapshot: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FrozenCaptureRecord {
    format_ver: u16,
    generation: u64,
    capture: Vec<u8>,
}

/// Opaque, fully prepared snapshot candidate for a frozen generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PreparedGeneration {
    pub format_ver: u16,
    pub generation: u64,
    pub base_snapshot: Option<String>,
    pub root_id: String,
    /// Fingerprint over the resolved dirty intent and source file identities.
    pub source_fingerprint: String,
    /// Versioned serialization owned by the native snapshot preparer.
    pub candidate: Vec<u8>,
}

impl PreparedGeneration {
    pub(crate) fn new(
        generation: u64,
        base_snapshot: Option<String>,
        root_id: impl Into<String>,
        source_fingerprint: impl Into<String>,
        candidate: Vec<u8>,
    ) -> Self {
        Self {
            format_ver: RECORD_VERSION,
            generation,
            base_snapshot,
            root_id: root_id.into(),
            source_fingerprint: source_fingerprint.into(),
            candidate,
        }
    }
}

/// Durable publication intent. `request_id` is also the server idempotency key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublishRequest {
    pub format_ver: u16,
    /// Stable caller receipt identity. This never changes when the exact change set is rebased.
    pub request_id: String,
    /// Idempotency key for the current server-side snapshot submission. A CAS-losing immutable
    /// snapshot is preserved, then a deterministic new key is committed together with the
    /// metadata-only rebased candidate before the next network attempt.
    pub publish_operation_id: String,
    pub publish_attempt: u32,
    pub generation: u64,
    pub message: String,
    pub clear_after_publish: bool,
    pub created_at_ms: u64,
    /// External callers need an exact response-loss receipt until the daemon confirms delivery.
    /// Background autosaves have no waiting caller and are immediately acknowledgeable.
    pub requires_ack: bool,
    #[serde(default)]
    pub failure: Option<String>,
}

impl PublishRequest {
    pub(crate) fn new(
        request_id: impl Into<String>,
        generation: u64,
        message: impl Into<String>,
        clear_after_publish: bool,
        created_at_ms: u64,
    ) -> Self {
        let request_id = request_id.into();
        Self {
            format_ver: RECORD_VERSION,
            publish_operation_id: request_id.clone(),
            request_id,
            publish_attempt: 0,
            generation,
            message: message.into(),
            clear_after_publish,
            created_at_ms,
            requires_ack: true,
            failure: None,
        }
    }

    pub(crate) fn background(mut self) -> Self {
        self.requires_ack = false;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CompletedPublishRequest {
    pub format_ver: u16,
    pub request: PublishRequest,
    pub snapshot_id: String,
    /// Opaque, caller-owned successful response. The daemon stores a serialized `SealReport`
    /// here so reconnecting the same control request returns the original success after the live
    /// generation rows have been retired.
    pub response: Vec<u8>,
    /// Long-lived one-shot clients set this only after delivering the receipt to the user. The
    /// record remains as durable evidence that this local store has adopted at least one remote
    /// snapshot, including the important empty-tree case where there are no sealed path rows.
    #[serde(default)]
    pub acknowledged: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RestoreOperation {
    pub format_ver: u16,
    pub request_id: String,
    pub target_snapshot_id: String,
    pub expected_snapshot_id: String,
    pub discard_local: bool,
    pub created_at_ms: u64,
    pub completed_snapshot_id: Option<String>,
    pub locally_adopted: bool,
    #[serde(default)]
    pub failure: Option<String>,
}

/// Replaceable durability boundary used by the common native publication state machine.
///
/// Mutation discovery and preparation are intentionally workflow adapters above this interface:
/// managed mounts consume authoritative intents, while tracked directories first reconcile an
/// unmanaged namespace. Once they have a prepared candidate, both depend only on these
/// transactional operations. The executable in-memory recovery model exercises the same state
/// transitions independently of redb.
pub(crate) trait LocalSnapshotStore {
    fn fail_publish_request(&self, request_id: &str, reason: &str) -> Result<()>;

    fn replace_prepared_for_rebase(
        &self,
        prepared: PreparedGeneration,
        next_publish_operation_id: &str,
    ) -> Result<PublishRequest>;

    fn mark_published(&self, generation: u64, snapshot_id: &str) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileIdentity {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nanos: i64,
    pub ctime_secs: i64,
    pub ctime_nanos: i64,
    pub mode: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum SealedPathState {
    Upsert {
        identity: FileIdentity,
        content_ref: Option<String>,
    },
    Delete,
}

/// Last published local identity for a path. This distinguishes retained byte-cache entries from
/// unsnapshotted changes after restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SealedBaseline {
    pub format_ver: u16,
    pub path: String,
    pub snapshot_id: String,
    pub state: SealedPathState,
    /// Start of the local reconciliation scan that proved this identity. Direct-directory
    /// pushes use this to apply the same racy-window rule as Git's index: an mtime/ctime inside
    /// the observation window is never sufficient proof that the bytes stayed unchanged.
    #[serde(default)]
    pub observed_at_secs: Option<i64>,
    #[serde(default)]
    pub observed_at_nanos: Option<i64>,
}

impl SealedBaseline {
    pub(crate) fn upsert(
        path: impl Into<String>,
        snapshot_id: impl Into<String>,
        identity: FileIdentity,
        content_ref: Option<String>,
    ) -> Self {
        Self {
            format_ver: RECORD_VERSION,
            path: path.into(),
            snapshot_id: snapshot_id.into(),
            state: SealedPathState::Upsert {
                identity,
                content_ref,
            },
            observed_at_secs: None,
            observed_at_nanos: None,
        }
    }

    pub(crate) fn upsert_observed(
        path: impl Into<String>,
        snapshot_id: impl Into<String>,
        identity: FileIdentity,
        content_ref: Option<String>,
        observed_at_secs: i64,
        observed_at_nanos: i64,
    ) -> Self {
        let mut baseline = Self::upsert(path, snapshot_id, identity, content_ref);
        baseline.observed_at_secs = Some(observed_at_secs);
        baseline.observed_at_nanos = Some(observed_at_nanos);
        baseline
    }

    pub(crate) fn delete(path: impl Into<String>, snapshot_id: impl Into<String>) -> Self {
        Self {
            format_ver: RECORD_VERSION,
            path: path.into(),
            snapshot_id: snapshot_id.into(),
            state: SealedPathState::Delete,
            observed_at_secs: None,
            observed_at_nanos: None,
        }
    }
}

/// Crash-safe embedded state store. Clones share one redb database handle.
#[derive(Clone)]
pub(crate) struct LocalState {
    db: Arc<Database>,
    identity: LocalStateIdentity,
    #[cfg(test)]
    was_created: bool,
}

impl LocalState {
    /// Opens an existing database through redb's strict read-only backend.
    ///
    /// This never creates, initializes, repairs, or logically mutates the file. It is intended for
    /// offline inspection such as `tl fs doctor`; redb deliberately rejects a read-only open while
    /// another process owns the writable database lock.
    pub(crate) fn open_existing(
        path: impl AsRef<Path>,
        identity: LocalStateIdentity,
    ) -> Result<LocalStateReader> {
        LocalStateReader::open(path, identity)
    }

    /// Opens or creates a database and validates its exact mount identity.
    ///
    /// Existing unreadable/malformed state fails closed. The durable legacy-import marker, rather
    /// than a transient "new file" result, decides whether the one-time conservative import runs.
    pub(crate) fn open(path: impl AsRef<Path>, identity: LocalStateIdentity) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(storage)?;
        }
        let db = Database::create(&path).map_err(storage)?;
        let db = Arc::new(db);

        let mut write = db.begin_write().map_err(storage)?;
        write
            .set_durability(Durability::Immediate)
            .map_err(storage)?;

        let existing_meta = {
            let mut table = write.open_table(META).map_err(storage)?;
            let existing = table
                .get(META_KEY)
                .map_err(storage)?
                .map(|raw| raw.value().to_vec());
            match existing {
                Some(raw) => Some(decode::<MetaRecord>(&raw, "metadata")?),
                None => {
                    let meta = MetaRecord {
                        format_ver: RECORD_VERSION,
                        schema_ver: SCHEMA_VERSION,
                        identity: identity.clone(),
                        active_generation: 1,
                        last_retired_generation: 0,
                        next_mutation_sequence: 1,
                        legacy_import_completed: false,
                    };
                    let encoded = encode(&meta)?;
                    table
                        .insert(META_KEY, encoded.as_slice())
                        .map_err(storage)?;
                    None
                }
            }
        };

        let created = existing_meta.is_none();
        if let Some(meta) = &existing_meta {
            validate_record_version(meta.format_ver, "metadata")?;
            if meta.schema_ver != SCHEMA_VERSION {
                return Err(LocalStateError::UnsupportedSchema {
                    expected: SCHEMA_VERSION,
                    found: meta.schema_ver,
                });
            }
            if meta.identity != identity {
                return Err(LocalStateError::IdentityMismatch {
                    expected: identity,
                    found: meta.identity.clone(),
                });
            }
        }

        // Create the complete schema only for a new database. Existing state is validated through
        // a read transaction after this commit: WriteTransaction::open_table creates absent
        // tables, which would otherwise turn a partially corrupt database into an apparently clean
        // one during open.
        if created {
            let mut generations = write.open_table(GENERATIONS).map_err(storage)?;
            let generation = GenerationRecord::open(1, None);
            let encoded = encode(&generation)?;
            generations.insert(1, encoded.as_slice()).map_err(storage)?;
            drop(generations);
            write.open_table(DIRTY).map_err(storage)?;
            write.open_table(RENAMES).map_err(storage)?;
            write.open_table(INTENTS).map_err(storage)?;
            write.open_table(FROZEN_CAPTURES).map_err(storage)?;
            write.open_table(PREPARED).map_err(storage)?;
            write.open_table(REQUESTS).map_err(storage)?;
            write.open_table(COMPLETED_REQUESTS).map_err(storage)?;
            write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
            write.open_table(SEALED).map_err(storage)?;
            write.open_table(ARTIFACTS).map_err(storage)?;
        }
        write.commit().map_err(storage)?;
        validate_existing_database(db.as_ref(), &identity)?;

        Ok(Self {
            db,
            identity,
            #[cfg(test)]
            was_created: created,
        })
    }

    pub(crate) fn identity(&self) -> &LocalStateIdentity {
        &self.identity
    }

    #[cfg(test)]
    pub(crate) fn was_created(&self) -> bool {
        self.was_created
    }

    /// Whether the durable one-time cutover marker is still absent.
    ///
    /// Unlike [`Self::was_created`], this survives a crash after database initialization but before
    /// legacy overlay import. Startup must use this durable predicate when deciding whether the
    /// exceptional reconstruction walk is still required.
    pub(crate) fn needs_legacy_import(&self) -> Result<bool> {
        Ok(!self.read_meta()?.legacy_import_completed)
    }

    /// Atomically imports the conservative pre-database dirty image exactly once.
    ///
    /// The transaction sets the active generation's base and records every supplied mutation
    /// before marking the cutover complete. A crash therefore leaves either the untouched,
    /// retryable new database or the complete imported image—never a partially clean state.
    ///
    /// Returns `true` when this call performed the import and `false` when an earlier call already
    /// completed it. Normal mutations before the import are rejected rather than silently mixed
    /// with an ambiguous legacy image.
    pub(crate) fn import_legacy_once(&self, import: LegacyImport) -> Result<bool> {
        let normalized = import
            .mutations
            .into_iter()
            .map(|mutation| match mutation {
                LegacyMutation::Upsert {
                    path,
                    min_write_offset,
                } => Ok(LegacyMutation::Upsert {
                    path: validate_path(&path)?,
                    min_write_offset,
                }),
                LegacyMutation::Delete { path } => Ok(LegacyMutation::Delete {
                    path: validate_path(&path)?,
                }),
                LegacyMutation::Rename { from, to } => {
                    let from = validate_path(&from)?;
                    let to = validate_path(&to)?;
                    if from == to {
                        return Err(LocalStateError::InvalidPath(from));
                    }
                    Ok(LegacyMutation::Rename { from, to })
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        if meta.legacy_import_completed {
            return Ok(false);
        }
        if meta.active_generation != 1
            || meta.last_retired_generation != 0
            || meta.next_mutation_sequence != 1
            || generation_has_dirty(&write, meta.active_generation)?
            || has_inflight_before(&write, meta.active_generation)?
        {
            return Err(LocalStateError::InvalidTransition(
                "legacy import must run before any local mutation".to_string(),
            ));
        }

        let mut generation = generation_from_write(&write, meta.active_generation)?;
        ensure_state(&generation, GenerationState::Open)?;
        if generation.base_snapshot.is_some() {
            return Err(LocalStateError::InvalidTransition(
                "legacy import must seed the initial base snapshot".to_string(),
            ));
        }
        generation.base_snapshot = import.base_snapshot;

        {
            let mut dirty = write.open_table(DIRTY).map_err(storage)?;
            let mut renames = write.open_table(RENAMES).map_err(storage)?;
            let mut intents = write.open_table(INTENTS).map_err(storage)?;
            for mutation in normalized {
                let receipt = next_receipt(&mut meta)?;
                let generation = receipt.generation;
                let (intent, namespace_applied) = match &mutation {
                    LegacyMutation::Upsert {
                        path,
                        min_write_offset,
                    } => (
                        MutationIntent::Upsert {
                            path: path.clone(),
                            min_write_offset: *min_write_offset,
                        },
                        None,
                    ),
                    LegacyMutation::Delete { path } => {
                        (MutationIntent::Delete { path: path.clone() }, None)
                    }
                    LegacyMutation::Rename { from, to } => (
                        MutationIntent::Rename {
                            source: from.clone(),
                            destination: to.clone(),
                        },
                        Some(true),
                    ),
                };
                let journal = MutationJournalRecord {
                    format_ver: RECORD_VERSION,
                    generation,
                    sequence: receipt.sequence,
                    intent,
                    namespace_applied,
                };
                let encoded = encode(&journal)?;
                intents
                    .insert(&(generation, receipt.sequence), encoded.as_slice())
                    .map_err(storage)?;
                match mutation {
                    LegacyMutation::Upsert {
                        path,
                        min_write_offset,
                    } => {
                        let key = (generation, path.as_str());
                        let previous = dirty
                            .get(&key)
                            .map_err(storage)?
                            .map(|raw| decode::<DirtyPath>(raw.value(), "dirty path"))
                            .transpose()?;
                        let min_write_offset = previous
                            .filter(|row| row.kind == DirtyKind::Upsert)
                            .map_or(min_write_offset, |row| {
                                row.min_write_offset.min(min_write_offset)
                            });
                        let row = DirtyPath {
                            format_ver: RECORD_VERSION,
                            generation,
                            sequence: receipt.sequence,
                            path: path.clone(),
                            kind: DirtyKind::Upsert,
                            min_write_offset,
                        };
                        let encoded = encode(&row)?;
                        dirty.insert(&key, encoded.as_slice()).map_err(storage)?;
                        renames.remove(&key).map_err(storage)?;
                    }
                    LegacyMutation::Delete { path } => {
                        let key = (generation, path.as_str());
                        let row = DirtyPath {
                            format_ver: RECORD_VERSION,
                            generation,
                            sequence: receipt.sequence,
                            path: path.clone(),
                            kind: DirtyKind::Delete,
                            min_write_offset: 0,
                        };
                        let encoded = encode(&row)?;
                        dirty.insert(&key, encoded.as_slice()).map_err(storage)?;
                        renames.remove(&key).map_err(storage)?;
                    }
                    LegacyMutation::Rename { from, to } => {
                        let source_key = (generation, from.as_str());
                        let true_source = renames
                            .get(&source_key)
                            .map_err(storage)?
                            .map(|raw| decode::<RenameIntent>(raw.value(), "rename intent"))
                            .transpose()?
                            .map_or_else(|| from.clone(), |rename| rename.from);
                        renames.remove(&source_key).map_err(storage)?;
                        renames
                            .remove(&(generation, to.as_str()))
                            .map_err(storage)?;
                        let rename = RenameIntent {
                            format_ver: RECORD_VERSION,
                            generation,
                            sequence: receipt.sequence,
                            from: true_source,
                            local_from: from.clone(),
                            to: to.clone(),
                            applied: true,
                        };
                        let encoded = encode(&rename)?;
                        renames
                            .insert(&(generation, to.as_str()), encoded.as_slice())
                            .map_err(storage)?;
                        for (path, kind) in [
                            (from.as_str(), DirtyKind::Delete),
                            (to.as_str(), DirtyKind::Upsert),
                        ] {
                            let row = DirtyPath {
                                format_ver: RECORD_VERSION,
                                generation,
                                sequence: receipt.sequence,
                                path: path.to_string(),
                                kind,
                                min_write_offset: 0,
                            };
                            let encoded = encode(&row)?;
                            dirty
                                .insert(&(generation, path), encoded.as_slice())
                                .map_err(storage)?;
                        }
                    }
                }
            }
        }

        meta.legacy_import_completed = true;
        put_generation(&write, &generation)?;
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)?;
        Ok(true)
    }

    pub(crate) fn active_generation(&self) -> Result<u64> {
        Ok(self.read_meta()?.active_generation)
    }

    pub(crate) fn generation(&self, generation: u64) -> Result<Option<GenerationRecord>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(GENERATIONS).map_err(storage)?;
        let Some(raw) = table.get(generation).map_err(storage)? else {
            return Ok(None);
        };
        let record = decode::<GenerationRecord>(raw.value(), "generation")?;
        validate_generation(&record, generation)?;
        Ok(Some(record))
    }

    /// Mint and durably bind the one preparation idempotency key for an open generation.
    pub(crate) fn ensure_preparation_operation_id(&self, generation: u64) -> Result<String> {
        let write = self.immediate_write()?;
        let mut record = generation_from_write(&write, generation)?;
        ensure_state(&record, GenerationState::Open)?;
        if let Some(operation_id) = record.preparation_operation_id.clone() {
            return Ok(operation_id);
        }
        let operation_id = Uuid::new_v4().to_string();
        record.preparation_operation_id = Some(operation_id.clone());
        put_generation(&write, &record)?;
        write.commit().map_err(storage)?;
        Ok(operation_id)
    }

    pub(crate) fn generations(&self) -> Result<Vec<GenerationRecord>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(GENERATIONS).map_err(storage)?;
        let mut records = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let generation = key.value();
            let record = decode::<GenerationRecord>(value.value(), "generation")?;
            validate_generation(&record, generation)?;
            records.push(record);
        }
        Ok(records)
    }

    pub(crate) fn active_restore(&self) -> Result<Option<RestoreOperation>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(RESTORE_OPERATIONS).map_err(storage)?;
        let operation = table
            .get("active")
            .map_err(storage)?
            .map(|raw| decode::<RestoreOperation>(raw.value(), "restore operation"))
            .transpose()?;
        if let Some(operation) = operation.as_ref() {
            validate_restore_operation(operation)?;
        }
        Ok(operation)
    }

    pub(crate) fn failed_restore(&self) -> Result<Option<RestoreOperation>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(RESTORE_OPERATIONS).map_err(storage)?;
        let operation = table
            .get("failed")
            .map_err(storage)?
            .map(|raw| decode::<RestoreOperation>(raw.value(), "failed restore"))
            .transpose()?;
        if let Some(operation) = operation.as_ref() {
            validate_restore_operation(operation)?;
        }
        Ok(operation)
    }

    /// Updates the base only when there is no dirty or frozen work whose lineage would be changed.
    #[cfg(test)]
    pub(crate) fn set_base_snapshot(&self, base_snapshot: Option<String>) -> Result<()> {
        let write = self.immediate_write()?;
        let meta = read_meta_from_write(&write)?;
        let active = meta.active_generation;
        if generation_has_dirty(&write, active)? {
            return Err(LocalStateError::InvalidTransition(
                "cannot change the base of a dirty generation".to_string(),
            ));
        }
        if has_inflight_before(&write, active)? {
            return Err(LocalStateError::InvalidTransition(
                "cannot change the base while an earlier generation is in flight".to_string(),
            ));
        }
        {
            let mut table = write.open_table(GENERATIONS).map_err(storage)?;
            let mut generation = get_generation(&table, active)?.ok_or_else(|| {
                LocalStateError::Corrupt("active generation record is missing".to_string())
            })?;
            ensure_state(&generation, GenerationState::Open)?;
            generation.base_snapshot = base_snapshot;
            let encoded = encode(&generation)?;
            table.insert(active, encoded.as_slice()).map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    pub(crate) fn begin_restore(
        &self,
        target_snapshot_id: &str,
        discard_local: bool,
        created_at_ms: u64,
    ) -> Result<RestoreOperation> {
        if target_snapshot_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "restore target cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        let meta = read_meta_from_write(&write)?;
        if has_inflight_before(&write, meta.active_generation)? {
            return Err(LocalStateError::InvalidTransition(
                "cannot restore while an earlier snapshot generation is in flight".to_string(),
            ));
        }
        let active_generation = generation_from_write(&write, meta.active_generation)?;
        ensure_state(&active_generation, GenerationState::Open)?;
        if !discard_local && generation_has_dirty(&write, meta.active_generation)? {
            return Err(LocalStateError::InvalidTransition(
                "cannot start a non-discarding restore while local mutations are dirty".to_string(),
            ));
        }
        let expected_snapshot_id = active_generation.base_snapshot.clone().ok_or_else(|| {
            LocalStateError::InvalidTransition(
                "cannot restore a workspace with no adopted base snapshot".to_string(),
            )
        })?;
        let mut table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
        if let Some(raw) = table.get("active").map_err(storage)? {
            let operation = decode::<RestoreOperation>(raw.value(), "restore operation")?;
            validate_restore_operation(&operation)?;
            if operation.target_snapshot_id != target_snapshot_id
                || operation.discard_local != discard_local
            {
                return Err(LocalStateError::Conflict(format!(
                    "restore {} is already in flight for target {}; resume it before requesting \
                     target {target_snapshot_id}",
                    operation.request_id, operation.target_snapshot_id
                )));
            }
            if operation.expected_snapshot_id != expected_snapshot_id {
                return Err(LocalStateError::Conflict(format!(
                    "restore {} expected base {}, but the active generation now names {}",
                    operation.request_id, operation.expected_snapshot_id, expected_snapshot_id
                )));
            }
            return Ok(operation);
        }
        let completed = {
            let raw = table.get("completed").map_err(storage)?;
            raw.map(|raw| decode::<RestoreOperation>(raw.value(), "completed restore"))
                .transpose()?
        };
        if let Some(completed) = completed {
            validate_restore_operation(&completed)?;
            if completed.target_snapshot_id == target_snapshot_id
                && completed.discard_local == discard_local
                && completed.completed_snapshot_id.as_deref()
                    == active_generation.base_snapshot.as_deref()
            {
                return Ok(completed);
            }
            return Err(LocalStateError::Conflict(format!(
                "restore {} completed locally but its result has not been acknowledged; retry \
                 target {} with the same policy before starting another restore",
                completed.request_id, completed.target_snapshot_id
            )));
        }
        let operation = RestoreOperation {
            format_ver: RECORD_VERSION,
            request_id: Uuid::new_v4().to_string(),
            target_snapshot_id: target_snapshot_id.to_string(),
            expected_snapshot_id,
            discard_local,
            created_at_ms,
            completed_snapshot_id: None,
            locally_adopted: false,
            failure: None,
        };
        let encoded = encode(&operation)?;
        table
            .insert("active", encoded.as_slice())
            .map_err(storage)?;
        drop(table);
        write.commit().map_err(storage)?;
        Ok(operation)
    }

    pub(crate) fn record_restore_server_result(
        &self,
        request_id: &str,
        snapshot_id: &str,
    ) -> Result<()> {
        if snapshot_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "restore result cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        let mut table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
        let mut operation = match table.get("active").map_err(storage)? {
            Some(raw) => decode::<RestoreOperation>(raw.value(), "restore operation")?,
            None => {
                let completed = table
                    .get("completed")
                    .map_err(storage)?
                    .map(|raw| decode::<RestoreOperation>(raw.value(), "completed restore"))
                    .transpose()?
                    .ok_or_else(|| {
                        LocalStateError::InvalidTransition(
                            "no restore operation is active".to_string(),
                        )
                    })?;
                if completed.request_id == request_id
                    && completed.completed_snapshot_id.as_deref() == Some(snapshot_id)
                    && completed.locally_adopted
                {
                    return Ok(());
                }
                return Err(LocalStateError::Conflict(format!(
                    "completed restore {} does not match {request_id}",
                    completed.request_id
                )));
            }
        };
        validate_restore_operation(&operation)?;
        if operation.request_id != request_id {
            return Err(LocalStateError::Conflict(format!(
                "active restore is {}, not {request_id}",
                operation.request_id
            )));
        }
        match operation.completed_snapshot_id.as_deref() {
            Some(existing) if existing != snapshot_id => {
                return Err(LocalStateError::Conflict(format!(
                    "restore {request_id} already recorded result {existing}, not {snapshot_id}"
                )));
            }
            Some(_) => return Ok(()),
            None => operation.completed_snapshot_id = Some(snapshot_id.to_string()),
        }
        let encoded = encode(&operation)?;
        table
            .insert("active", encoded.as_slice())
            .map_err(storage)?;
        drop(table);
        write.commit().map_err(storage)
    }

    /// Atomically adopts the server result and converts the active restore into a replayable
    /// completion receipt. If recovery observes that the base already equals the result, it only
    /// completes the receipt and preserves writes that arrived after the first adoption.
    pub(crate) fn finish_restore(&self, request_id: &str) -> Result<u64> {
        macro_rules! clear_table {
            ($write:expr, $definition:expr) => {{
                let mut table = ($write).open_table($definition).map_err(storage)?;
                table.retain(|_, _| false).map_err(storage)?;
            }};
        }
        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        let mut operation = {
            let table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
            match table.get("active").map_err(storage)? {
                Some(raw) => decode::<RestoreOperation>(raw.value(), "restore operation")?,
                None => {
                    let completed = table
                        .get("completed")
                        .map_err(storage)?
                        .map(|raw| decode::<RestoreOperation>(raw.value(), "completed restore"))
                        .transpose()?
                        .ok_or_else(|| {
                            LocalStateError::InvalidTransition(
                                "no restore operation is active".to_string(),
                            )
                        })?;
                    if completed.request_id == request_id && completed.locally_adopted {
                        return Ok(meta.active_generation);
                    }
                    return Err(LocalStateError::Conflict(format!(
                        "completed restore {} does not match {request_id}",
                        completed.request_id
                    )));
                }
            }
        };
        validate_restore_operation(&operation)?;
        if operation.request_id != request_id {
            return Err(LocalStateError::Conflict(format!(
                "active restore is {}, not {request_id}",
                operation.request_id
            )));
        }
        let snapshot_id = operation.completed_snapshot_id.as_deref().ok_or_else(|| {
            LocalStateError::InvalidTransition(format!(
                "restore {request_id} has no recorded server result"
            ))
        })?;
        if has_inflight_before(&write, meta.active_generation)? {
            return Err(LocalStateError::InvalidTransition(
                "cannot adopt restore while an earlier snapshot generation is in flight"
                    .to_string(),
            ));
        }
        let current = generation_from_write(&write, meta.active_generation)?;
        ensure_state(&current, GenerationState::Open)?;
        let resulting_generation = if current.base_snapshot.as_deref() == Some(snapshot_id) {
            meta.active_generation
        } else {
            let next_generation = meta.active_generation.checked_add(1).ok_or_else(|| {
                LocalStateError::InvalidTransition("generation counter overflow".to_string())
            })?;
            meta.next_mutation_sequence =
                meta.next_mutation_sequence.checked_add(1).ok_or_else(|| {
                    LocalStateError::InvalidTransition(
                        "mutation sequence counter overflow".to_string(),
                    )
                })?;
            clear_table!(&write, DIRTY);
            clear_table!(&write, RENAMES);
            clear_table!(&write, INTENTS);
            clear_table!(&write, FROZEN_CAPTURES);
            clear_table!(&write, PREPARED);
            clear_table!(&write, REQUESTS);
            clear_table!(&write, COMPLETED_REQUESTS);
            clear_table!(&write, SEALED);
            clear_table!(&write, ARTIFACTS);
            {
                let mut table = write.open_table(GENERATIONS).map_err(storage)?;
                table.retain(|_, _| false).map_err(storage)?;
                let generation =
                    GenerationRecord::open(next_generation, Some(snapshot_id.to_string()));
                let encoded = encode(&generation)?;
                table
                    .insert(next_generation, encoded.as_slice())
                    .map_err(storage)?;
            }
            meta.active_generation = next_generation;
            meta.last_retired_generation = meta
                .last_retired_generation
                .max(next_generation.saturating_sub(1));
            meta.legacy_import_completed = true;
            next_generation
        };
        operation.locally_adopted = true;
        {
            let mut table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
            let encoded = encode(&operation)?;
            table
                .insert("completed", encoded.as_slice())
                .map_err(storage)?;
            table.remove("active").map_err(storage)?;
        }
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)?;
        Ok(resulting_generation)
    }

    pub(crate) fn acknowledge_restore(&self, request_id: &str) -> Result<()> {
        let write = self.immediate_write()?;
        {
            let mut table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
            let completed = {
                let Some(raw) = table.get("completed").map_err(storage)? else {
                    return Ok(());
                };
                decode::<RestoreOperation>(raw.value(), "completed restore")?
            };
            validate_restore_operation(&completed)?;
            if completed.request_id != request_id {
                return Err(LocalStateError::Conflict(format!(
                    "completed restore is {}, not {request_id}",
                    completed.request_id
                )));
            }
            if !completed.locally_adopted {
                return Err(LocalStateError::Corrupt(format!(
                    "completed restore {request_id} was not locally adopted"
                )));
            }
            table.remove("completed").map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    /// Dead-letter a restore whose server result is known to be permanently unusable.
    ///
    /// The failed record remains inspectable, while removing `active` releases the write fence so
    /// the operator can retry from the now-current head under a new idempotency key.
    pub(crate) fn fail_restore(&self, request_id: &str, reason: &str) -> Result<()> {
        if reason.trim().is_empty() {
            return Err(LocalStateError::Conflict(
                "restore failure reason cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        {
            let mut table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
            let mut operation = {
                let raw = table.get("active").map_err(storage)?.ok_or_else(|| {
                    LocalStateError::InvalidTransition("no restore operation is active".to_string())
                })?;
                decode::<RestoreOperation>(raw.value(), "restore operation")?
            };
            validate_restore_operation(&operation)?;
            if operation.request_id != request_id {
                return Err(LocalStateError::Conflict(format!(
                    "active restore is {}, not {request_id}",
                    operation.request_id
                )));
            }
            if operation.completed_snapshot_id.is_some() {
                return Err(LocalStateError::InvalidTransition(format!(
                    "restore {request_id} already has an adoptable server result"
                )));
            }
            operation.failure = Some(reason.to_string());
            let encoded = encode(&operation)?;
            table
                .insert("failed", encoded.as_slice())
                .map_err(storage)?;
            table.remove("active").map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    /// Replaces all local lifecycle state after an explicit restore has durably selected
    /// `snapshot_id` on the server and the caller has passed the destructive-data gate.
    ///
    /// Generation and mutation counters advance instead of resetting, so stale in-process
    /// receipts/captures can never alias the post-restore world. Every old dirty intent, prepared
    /// candidate, request idempotency key, and sealed baseline is removed in the same Immediate
    /// transaction that creates the fresh open generation.
    #[cfg(test)]
    pub(crate) fn reset_after_restore(&self, snapshot_id: &str) -> Result<u64> {
        if snapshot_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "restored snapshot id cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        let next_generation = meta.active_generation.checked_add(1).ok_or_else(|| {
            LocalStateError::InvalidTransition("generation counter overflow".to_string())
        })?;
        meta.next_mutation_sequence =
            meta.next_mutation_sequence.checked_add(1).ok_or_else(|| {
                LocalStateError::InvalidTransition("mutation sequence counter overflow".to_string())
            })?;

        {
            let mut table = write.open_table(DIRTY).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(RENAMES).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(INTENTS).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(FROZEN_CAPTURES).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(PREPARED).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(REQUESTS).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(COMPLETED_REQUESTS).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(SEALED).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(ARTIFACTS).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut table = write.open_table(GENERATIONS).map_err(storage)?;
            table.retain(|_, _| false).map_err(storage)?;
            let generation = GenerationRecord::open(next_generation, Some(snapshot_id.to_string()));
            let encoded = encode(&generation)?;
            table
                .insert(next_generation, encoded.as_slice())
                .map_err(storage)?;
        }

        meta.active_generation = next_generation;
        meta.last_retired_generation = meta
            .last_retired_generation
            .max(next_generation.saturating_sub(1));
        meta.legacy_import_completed = true;
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)?;
        Ok(next_generation)
    }

    /// Rebase the current open dirty generation after a server-side restore raced local writes.
    ///
    /// The caller must first remove retained cache entries from the old base while preserving
    /// paths covered by this generation. There may be no frozen/prepared/publication work: only
    /// then can every still-dirty upper entry be interpreted unambiguously as a change to apply
    /// over `snapshot_id`. Old sealed baselines and completed-request receipts belong to the
    /// abandoned timeline and are cleared atomically with the base change.
    #[cfg(test)]
    pub(crate) fn adopt_restored_base_preserving_open_dirty(
        &self,
        snapshot_id: &str,
    ) -> Result<()> {
        if snapshot_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "restored snapshot id cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        let meta = read_meta_from_write(&write)?;
        if has_inflight_before(&write, meta.active_generation)? {
            return Err(LocalStateError::InvalidTransition(
                "cannot adopt a restored base while an earlier generation is in flight".to_string(),
            ));
        }
        {
            let mut generations = write.open_table(GENERATIONS).map_err(storage)?;
            let mut active =
                get_generation(&generations, meta.active_generation)?.ok_or_else(|| {
                    LocalStateError::Corrupt("active generation record is missing".to_string())
                })?;
            ensure_state(&active, GenerationState::Open)?;
            active.base_snapshot = Some(snapshot_id.to_string());
            active.published_snapshot = None;
            let encoded = encode(&active)?;
            generations
                .insert(active.generation, encoded.as_slice())
                .map_err(storage)?;
        }
        {
            let mut sealed = write.open_table(SEALED).map_err(storage)?;
            sealed.retain(|_, _| false).map_err(storage)?;
        }
        {
            let mut completed = write.open_table(COMPLETED_REQUESTS).map_err(storage)?;
            completed.retain(|_, _| false).map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    /// Durably marks an upsert before the filesystem mutation becomes visible.
    pub(crate) fn record_upsert(
        &self,
        path: &str,
        min_write_offset: u64,
    ) -> Result<MutationReceipt> {
        self.record_mutations(&[MutationIntent::Upsert {
            path: path.to_string(),
            min_write_offset,
        }])?
        .into_iter()
        .next()
        .ok_or_else(|| LocalStateError::Corrupt("missing mutation receipt".to_string()))
    }

    /// Durably marks a delete before the namespace mutation becomes visible.
    pub(crate) fn record_delete(&self, path: &str) -> Result<MutationReceipt> {
        self.record_mutations(&[MutationIntent::Delete {
            path: path.to_string(),
        }])?
        .into_iter()
        .next()
        .ok_or_else(|| LocalStateError::Corrupt("missing mutation receipt".to_string()))
    }

    /// Atomically records the source delete, destination upsert, and rename optimization.
    #[cfg(test)]
    pub(crate) fn record_rename(&self, from: &str, to: &str) -> Result<MutationReceipt> {
        self.record_mutations(&[MutationIntent::Rename {
            source: from.to_string(),
            destination: to.to_string(),
        }])?
        .into_iter()
        .next()
        .ok_or_else(|| LocalStateError::Corrupt("missing mutation receipt".to_string()))
    }

    /// Durably commits an ordered group of mutation intents in one Immediate transaction.
    ///
    /// The overlay waits for this transaction before allowing any corresponding filesystem
    /// mutation to start. A storage error therefore acknowledges none of the group, while a
    /// successful return means every receipt is crash-visible in the returned order.
    pub(crate) fn record_mutations(
        &self,
        intents: &[MutationIntent],
    ) -> Result<Vec<MutationReceipt>> {
        if intents.is_empty() {
            return Ok(Vec::new());
        }
        let intents = intents
            .iter()
            .map(|intent| match intent {
                MutationIntent::Upsert {
                    path,
                    min_write_offset,
                } => Ok(MutationIntent::Upsert {
                    path: validate_path(path)?,
                    min_write_offset: *min_write_offset,
                }),
                MutationIntent::Delete { path } => Ok(MutationIntent::Delete {
                    path: validate_path(path)?,
                }),
                MutationIntent::Rename {
                    source,
                    destination,
                } => {
                    let source = validate_path(source)?;
                    let destination = validate_path(destination)?;
                    if source == destination {
                        return Err(LocalStateError::InvalidPath(source));
                    }
                    Ok(MutationIntent::Rename {
                        source,
                        destination,
                    })
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        if let Some(operation) = active_restore_from_write(&write)? {
            return Err(LocalStateError::InvalidTransition(format!(
                "restore {} is changing the filesystem base; retry the mutation after restore \
                 completes",
                operation.request_id
            )));
        }
        ensure_state(
            &generation_from_write(&write, meta.active_generation)?,
            GenerationState::Open,
        )?;
        let mut receipts = Vec::with_capacity(intents.len());
        for intent in intents {
            let receipt = next_receipt(&mut meta)?;
            let generation = receipt.generation;
            {
                let journal = MutationJournalRecord {
                    format_ver: RECORD_VERSION,
                    generation,
                    sequence: receipt.sequence,
                    namespace_applied: matches!(&intent, MutationIntent::Rename { .. })
                        .then_some(false),
                    intent: intent.clone(),
                };
                let encoded = encode(&journal)?;
                let mut table = write.open_table(INTENTS).map_err(storage)?;
                table
                    .insert(&(generation, receipt.sequence), encoded.as_slice())
                    .map_err(storage)?;
            }
            match intent {
                MutationIntent::Upsert {
                    path,
                    min_write_offset,
                } => {
                    let key = (generation, path.as_str());
                    {
                        let mut dirty = write.open_table(DIRTY).map_err(storage)?;
                        let min_write_offset = {
                            let previous = dirty.get(&key).map_err(storage)?;
                            match previous {
                                Some(raw) => {
                                    let previous = decode::<DirtyPath>(raw.value(), "dirty path")?;
                                    if previous.kind == DirtyKind::Upsert {
                                        previous.min_write_offset.min(min_write_offset)
                                    } else {
                                        min_write_offset
                                    }
                                }
                                None => min_write_offset,
                            }
                        };
                        let record = DirtyPath {
                            format_ver: RECORD_VERSION,
                            generation,
                            sequence: receipt.sequence,
                            path: path.clone(),
                            kind: DirtyKind::Upsert,
                            min_write_offset,
                        };
                        let encoded = encode(&record)?;
                        dirty.insert(&key, encoded.as_slice()).map_err(storage)?;
                    }
                    let mut renames = write.open_table(RENAMES).map_err(storage)?;
                    renames.remove(&key).map_err(storage)?;
                }
                MutationIntent::Delete { path } => {
                    let key = (generation, path.as_str());
                    {
                        let mut dirty = write.open_table(DIRTY).map_err(storage)?;
                        let descendants = dirty
                            .range((generation, "")..=(generation, "\u{10ffff}"))
                            .map_err(storage)?
                            .map(|row| {
                                let (key, _) = row.map_err(storage)?;
                                Ok(key.value().1.to_string())
                            })
                            .collect::<Result<Vec<_>>>()?;
                        for descendant in descendants {
                            if is_path_at_or_below(&descendant, &path) {
                                dirty
                                    .remove(&(generation, descendant.as_str()))
                                    .map_err(storage)?;
                            }
                        }
                        let record = DirtyPath {
                            format_ver: RECORD_VERSION,
                            generation,
                            sequence: receipt.sequence,
                            path: path.clone(),
                            kind: DirtyKind::Delete,
                            min_write_offset: 0,
                        };
                        let encoded = encode(&record)?;
                        dirty.insert(&key, encoded.as_slice()).map_err(storage)?;
                    }
                    let mut renames = write.open_table(RENAMES).map_err(storage)?;
                    let descendants = renames
                        .range((generation, "")..=(generation, "\u{10ffff}"))
                        .map_err(storage)?
                        .map(|row| {
                            let (key, _) = row.map_err(storage)?;
                            Ok(key.value().1.to_string())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for descendant in descendants {
                        if is_path_at_or_below(&descendant, &path) {
                            renames
                                .remove(&(generation, descendant.as_str()))
                                .map_err(storage)?;
                        }
                    }
                }
                MutationIntent::Rename {
                    source,
                    destination,
                } => {
                    let local_source = source.clone();
                    // Compose a same-generation rename chain. `a -> b; b -> c` persists `a -> c`,
                    // while the delete of `b` remains because an original lower `b` was
                    // overwritten by the first rename.
                    let true_source = {
                        let mut renames = write.open_table(RENAMES).map_err(storage)?;
                        let source_key = (generation, source.as_str());
                        let source = match renames.get(&source_key).map_err(storage)? {
                            Some(raw) => decode::<RenameIntent>(raw.value(), "rename intent")?.from,
                            None => source.clone(),
                        };
                        renames.remove(&source_key).map_err(storage)?;
                        renames
                            .remove(&(generation, destination.as_str()))
                            .map_err(storage)?;
                        let rename = RenameIntent {
                            format_ver: RECORD_VERSION,
                            generation,
                            sequence: receipt.sequence,
                            from: source.clone(),
                            local_from: local_source,
                            to: destination.clone(),
                            applied: false,
                        };
                        let encoded = encode(&rename)?;
                        renames
                            .insert(&(generation, destination.as_str()), encoded.as_slice())
                            .map_err(storage)?;
                        source
                    };
                    {
                        let mut dirty = write.open_table(DIRTY).map_err(storage)?;
                        let existing = dirty
                            .range((generation, "")..=(generation, "\u{10ffff}"))
                            .map_err(storage)?
                            .map(|row| {
                                let (key, value) = row.map_err(storage)?;
                                let path = key.value().1.to_string();
                                let record = decode::<DirtyPath>(value.value(), "dirty path")?;
                                Ok((path, record))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let mut moved = Vec::new();
                        for (path, mut record) in existing {
                            if let Some(remapped) =
                                remap_path_at_or_below(&path, &source, &destination)
                            {
                                dirty
                                    .remove(&(generation, path.as_str()))
                                    .map_err(storage)?;
                                if path != source {
                                    record.path = remapped;
                                    record.sequence = receipt.sequence;
                                    moved.push(record);
                                }
                            } else if is_path_at_or_below(&path, &destination) {
                                // Rename replacement discards the destination's prior local
                                // namespace. Source descendants are installed below after the
                                // destination prefix has been cleared.
                                dirty
                                    .remove(&(generation, path.as_str()))
                                    .map_err(storage)?;
                            }
                        }
                        for record in moved {
                            let encoded = encode(&record)?;
                            dirty
                                .insert(&(generation, record.path.as_str()), encoded.as_slice())
                                .map_err(storage)?;
                        }
                        for (path, kind) in [
                            (source, DirtyKind::Delete),
                            (destination, DirtyKind::Upsert),
                        ] {
                            let record = DirtyPath {
                                format_ver: RECORD_VERSION,
                                generation,
                                sequence: receipt.sequence,
                                path: path.clone(),
                                kind,
                                min_write_offset: 0,
                            };
                            let encoded = encode(&record)?;
                            dirty
                                .insert(&(generation, path.as_str()), encoded.as_slice())
                                .map_err(storage)?;
                        }
                    }
                    debug_assert!(!true_source.is_empty());
                }
            }
            receipts.push(receipt);
        }
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)?;
        Ok(receipts)
    }

    pub(crate) fn mark_rename_applied(&self, source: &str, destination: &str) -> Result<()> {
        let source = validate_path(source)?;
        let destination = validate_path(destination)?;
        let write = self.immediate_write()?;
        let meta = read_meta_from_write(&write)?;
        let mut table = write.open_table(RENAMES).map_err(storage)?;
        let key = (meta.active_generation, destination.as_str());
        let mut rename = {
            let raw = table.get(&key).map_err(storage)?.ok_or_else(|| {
                LocalStateError::InvalidTransition(format!(
                    "active generation has no rename into {destination}"
                ))
            })?;
            decode::<RenameIntent>(raw.value(), "rename intent")?
        };
        validate_rename(&rename, meta.active_generation, &destination)?;
        if rename.local_from != source {
            return Err(LocalStateError::Conflict(format!(
                "rename into {destination} was prepared from {}, not {source}",
                rename.local_from
            )));
        }
        if rename.applied {
            return Ok(());
        }
        rename.applied = true;
        let encoded = encode(&rename)?;
        table.insert(&key, encoded.as_slice()).map_err(storage)?;
        drop(table);
        {
            let mut intents = write.open_table(INTENTS).map_err(storage)?;
            let intent_key = (meta.active_generation, rename.sequence);
            let mut journal = {
                let raw = intents.get(&intent_key).map_err(storage)?.ok_or_else(|| {
                    LocalStateError::Corrupt(format!(
                        "rename {} -> {destination} has no ordered mutation intent",
                        rename.local_from
                    ))
                })?;
                decode::<MutationJournalRecord>(raw.value(), "mutation intent")?
            };
            validate_mutation_journal(&journal, intent_key.0, intent_key.1)?;
            match &journal.intent {
                MutationIntent::Rename {
                    source: recorded_source,
                    destination: recorded_destination,
                } if recorded_source == &source && recorded_destination == &destination => {}
                other => {
                    return Err(LocalStateError::Corrupt(format!(
                        "rename index into {destination} points at incompatible intent {other:?}"
                    )));
                }
            }
            journal.namespace_applied = Some(true);
            let encoded = encode(&journal)?;
            intents
                .insert(&intent_key, encoded.as_slice())
                .map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    /// Freezes the active generation and opens the next generation in the same transaction.
    ///
    /// Returns `None` without advancing the generation clock when there is no dirty work.
    /// A small fixed number of earlier generations may be in flight; publication and retirement
    /// remain strictly ordered, and later prepared candidates metadata-rebase after a CAS loss.
    #[cfg(test)]
    pub(crate) fn freeze_current(&self) -> Result<Option<FrozenGeneration>> {
        self.freeze_current_internal(None)
    }

    /// Freezes the active generation together with the exact resolved source capture that
    /// preparation must consume.
    ///
    /// The opaque capture is committed in the same Immediate transaction as the generation
    /// transition. Consequently, a successful return is an indivisible snapshot boundary:
    /// subsequent filesystem mutations belong to the next generation and preparation never has
    /// to reconstruct the frozen generation from a newer live overlay view.
    pub(crate) fn freeze_current_with_capture(
        &self,
        capture: Vec<u8>,
    ) -> Result<Option<FrozenGeneration>> {
        self.freeze_current_internal(Some(capture))
    }

    fn freeze_current_internal(
        &self,
        capture: Option<Vec<u8>>,
    ) -> Result<Option<FrozenGeneration>> {
        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        if let Some(operation) = active_restore_from_write(&write)? {
            return Err(LocalStateError::InvalidTransition(format!(
                "restore {} is changing the filesystem base; snapshot freeze is blocked",
                operation.request_id
            )));
        }
        let current = meta.active_generation;
        if inflight_generation_count(&write, current)? >= MAX_INFLIGHT_GENERATIONS {
            return Err(LocalStateError::InvalidTransition(format!(
                "the maximum of {MAX_INFLIGHT_GENERATIONS} local snapshot generations is \
                     already in flight"
            )));
        }
        if !generation_has_dirty(&write, current)? {
            return Ok(None);
        }

        let base_snapshot = {
            let mut generations = write.open_table(GENERATIONS).map_err(storage)?;
            let mut record = get_generation(&generations, current)?.ok_or_else(|| {
                LocalStateError::Corrupt("active generation record is missing".to_string())
            })?;
            ensure_state(&record, GenerationState::Open)?;
            record.state = GenerationState::Frozen;
            let base = record.base_snapshot.clone();
            let encoded = encode(&record)?;
            generations
                .insert(current, encoded.as_slice())
                .map_err(storage)?;

            let next = current.checked_add(1).ok_or_else(|| {
                LocalStateError::InvalidTransition("generation counter overflow".to_string())
            })?;
            let next_record = GenerationRecord::open(next, base.clone());
            let encoded = encode(&next_record)?;
            generations
                .insert(next, encoded.as_slice())
                .map_err(storage)?;
            base
        };

        if let Some(capture) = capture {
            let record = FrozenCaptureRecord {
                format_ver: RECORD_VERSION,
                generation: current,
                capture,
            };
            let encoded = encode(&record)?;
            let mut captures = write.open_table(FROZEN_CAPTURES).map_err(storage)?;
            if captures.get(current).map_err(storage)?.is_some() {
                return Err(LocalStateError::Corrupt(format!(
                    "open generation {current} already has a frozen capture"
                )));
            }
            captures
                .insert(current, encoded.as_slice())
                .map_err(storage)?;
        }

        meta.active_generation = current + 1;
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)?;
        Ok(Some(FrozenGeneration {
            generation: current,
            next_generation: current + 1,
            base_snapshot,
        }))
    }

    pub(crate) fn frozen_capture(&self, generation: u64) -> Result<Option<Vec<u8>>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(FROZEN_CAPTURES).map_err(storage)?;
        let Some(raw) = table.get(generation).map_err(storage)? else {
            return Ok(None);
        };
        let record = decode::<FrozenCaptureRecord>(raw.value(), "frozen capture")?;
        validate_frozen_capture(&record, generation)?;
        Ok(Some(record.capture))
    }

    pub(crate) fn dirty_generation(&self, generation: u64) -> Result<DirtyGeneration> {
        let generation_record = self.generation(generation)?.ok_or_else(|| {
            LocalStateError::InvalidTransition(format!("generation {generation} does not exist"))
        })?;
        let read = self.db.begin_read().map_err(storage)?;
        let dirty = read.open_table(DIRTY).map_err(storage)?;
        let renames = read.open_table(RENAMES).map_err(storage)?;
        Ok(DirtyGeneration {
            generation,
            state: generation_record.state,
            base_snapshot: generation_record.base_snapshot,
            paths: dirty_rows(&dirty, generation)?,
            renames: rename_rows(&renames, generation)?,
        })
    }

    pub(crate) fn recovery_dirty_state(&self) -> Result<RecoveryDirtyState> {
        let meta = self.read_meta()?;
        let read = self.db.begin_read().map_err(storage)?;
        let dirty = read.open_table(DIRTY).map_err(storage)?;
        let renames = read.open_table(RENAMES).map_err(storage)?;
        let intents = read.open_table(INTENTS).map_err(storage)?;
        let mut paths = Vec::new();
        let mut rename_rows_out = Vec::new();
        let mut intent_rows_out = Vec::new();
        let mut maximum = meta.next_mutation_sequence.saturating_sub(1);
        for row in dirty.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let (generation, path) = key.value();
            let record = decode::<DirtyPath>(value.value(), "dirty path")?;
            validate_dirty(&record, generation, path)?;
            maximum = maximum.max(record.sequence);
            paths.push(record);
        }
        for row in renames.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let (generation, path) = key.value();
            let record = decode::<RenameIntent>(value.value(), "rename intent")?;
            validate_rename(&record, generation, path)?;
            maximum = maximum.max(record.sequence);
            rename_rows_out.push(record);
        }
        for row in intents.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let (generation, sequence) = key.value();
            let record = decode::<MutationJournalRecord>(value.value(), "mutation intent")?;
            validate_mutation_journal(&record, generation, sequence)?;
            maximum = maximum.max(record.sequence);
            intent_rows_out.push(record);
        }
        Ok(RecoveryDirtyState {
            active_generation: meta.active_generation,
            maximum_mutation_sequence: maximum,
            paths,
            renames: rename_rows_out,
            intents: intent_rows_out,
        })
    }

    /// Abandons a generation that is provably still local-only.
    ///
    /// `Frozen` is safe even when it has a queued request because publication cannot begin before
    /// preparation. `Prepared` is safe only while no request exists. Once the durable state is
    /// `PublishRequested`, recovery must use the idempotency key and server observations instead of
    /// guessing that submission did not occur.
    pub(crate) fn abort_unpublished_generation(&self, generation: u64) -> Result<()> {
        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        let record = generation_from_write(&write, generation)?;
        if generation != meta.last_retired_generation.saturating_add(1) {
            return Err(LocalStateError::InvalidTransition(format!(
                "can only abort the oldest unretired generation (last retired is {})",
                meta.last_retired_generation
            )));
        }
        match record.state {
            GenerationState::Frozen => {}
            GenerationState::Prepared
                if publish_request_for_generation(&write, generation)?.is_none() => {}
            GenerationState::Prepared => {
                return Err(LocalStateError::InvalidTransition(format!(
                    "prepared generation {generation} already has a publish request"
                )));
            }
            other => {
                return Err(LocalStateError::InvalidTransition(format!(
                    "cannot abort generation {generation} in state {other:?}"
                )));
            }
        }

        remove_generation_rows(&write, generation)?;
        {
            let mut prepared = write.open_table(PREPARED).map_err(storage)?;
            prepared.remove(generation).map_err(storage)?;
        }
        {
            let mut captures = write.open_table(FROZEN_CAPTURES).map_err(storage)?;
            captures.remove(generation).map_err(storage)?;
        }
        remove_publish_requests_for_generation(&write, generation)?;
        {
            let mut generations = write.open_table(GENERATIONS).map_err(storage)?;
            generations.remove(generation).map_err(storage)?;
            let mut active =
                get_generation(&generations, meta.active_generation)?.ok_or_else(|| {
                    LocalStateError::Corrupt("active generation record is missing".to_string())
                })?;
            ensure_state(&active, GenerationState::Open)?;
            active.base_snapshot = record.base_snapshot;
            let encoded = encode(&active)?;
            generations
                .insert(active.generation, encoded.as_slice())
                .map_err(storage)?;
        }
        meta.last_retired_generation = generation;
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)
    }

    pub(crate) fn mark_prepared(&self, prepared: PreparedGeneration) -> Result<()> {
        validate_record_version(prepared.format_ver, "prepared generation")?;
        let write = self.immediate_write()?;
        let mut generation = generation_from_write(&write, prepared.generation)?;
        match generation.state {
            GenerationState::Frozen => {}
            GenerationState::Prepared | GenerationState::PublishRequested => {
                let table = write.open_table(PREPARED).map_err(storage)?;
                let existing = table
                    .get(prepared.generation)
                    .map_err(storage)?
                    .ok_or_else(|| {
                        LocalStateError::Corrupt(
                            "prepared generation record is missing its payload".to_string(),
                        )
                    })?;
                let existing =
                    decode::<PreparedGeneration>(existing.value(), "prepared generation")?;
                if existing == prepared {
                    return Ok(());
                }
                return Err(LocalStateError::Conflict(format!(
                    "generation {} was already prepared with a different candidate",
                    prepared.generation
                )));
            }
            other => {
                return Err(LocalStateError::InvalidTransition(format!(
                    "cannot prepare generation {} in state {other:?}",
                    prepared.generation
                )));
            }
        }
        if generation.base_snapshot != prepared.base_snapshot {
            return Err(LocalStateError::Conflict(format!(
                "prepared generation {} base does not match its frozen base",
                prepared.generation
            )));
        }
        {
            let mut table = write.open_table(PREPARED).map_err(storage)?;
            let encoded = encode(&prepared)?;
            table
                .insert(prepared.generation, encoded.as_slice())
                .map_err(storage)?;
        }
        generation.state = if publish_request_for_generation(&write, prepared.generation)?.is_some()
        {
            GenerationState::PublishRequested
        } else {
            GenerationState::Prepared
        };
        put_generation(&write, &generation)?;
        write.commit().map_err(storage)
    }

    pub(crate) fn prepared(&self, generation: u64) -> Result<Option<PreparedGeneration>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(PREPARED).map_err(storage)?;
        let Some(raw) = table.get(generation).map_err(storage)? else {
            return Ok(None);
        };
        let prepared = decode::<PreparedGeneration>(raw.value(), "prepared generation")?;
        validate_record_version(prepared.format_ver, "prepared generation")?;
        if prepared.generation != generation {
            return Err(LocalStateError::Corrupt(format!(
                "prepared generation key {generation} contains generation {}",
                prepared.generation
            )));
        }
        Ok(Some(prepared))
    }

    /// Replace a prepared candidate after metadata-only rebasing onto a serialized winner.
    ///
    /// The caller-visible request id remains stable, but a losing immutable snapshot has already
    /// consumed the previous server idempotency key. Commit the rebased candidate and its next
    /// deterministic server operation id in one Immediate transaction before retrying.
    pub(crate) fn replace_prepared_for_rebase(
        &self,
        prepared: PreparedGeneration,
        next_publish_operation_id: &str,
    ) -> Result<PublishRequest> {
        validate_record_version(prepared.format_ver, "prepared generation")?;
        if next_publish_operation_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "rebased publish operation id cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        let mut generation = generation_from_write(&write, prepared.generation)?;
        if !matches!(
            generation.state,
            GenerationState::Prepared | GenerationState::PublishRequested
        ) {
            return Err(LocalStateError::InvalidTransition(format!(
                "cannot rebase prepared generation {} in state {:?}",
                prepared.generation, generation.state
            )));
        }
        generation.base_snapshot = prepared.base_snapshot.clone();
        put_generation(&write, &generation)?;
        let encoded = encode(&prepared)?;
        let mut table = write.open_table(PREPARED).map_err(storage)?;
        table
            .insert(prepared.generation, encoded.as_slice())
            .map_err(storage)?;
        drop(table);
        let mut request =
            publish_request_for_generation(&write, prepared.generation)?.ok_or_else(|| {
                LocalStateError::Corrupt(format!(
                    "rebased generation {} has no publish request",
                    prepared.generation
                ))
            })?;
        request.publish_attempt = request.publish_attempt.checked_add(1).ok_or_else(|| {
            LocalStateError::InvalidTransition("publish attempt counter overflow".to_string())
        })?;
        request.publish_operation_id = next_publish_operation_id.to_string();
        {
            let encoded = encode(&request)?;
            let mut requests = write.open_table(REQUESTS).map_err(storage)?;
            requests
                .insert(request.request_id.as_str(), encoded.as_slice())
                .map_err(storage)?;
        }
        write.commit().map_err(storage)?;
        Ok(request)
    }

    /// Persists a server idempotency key before the first publish attempt.
    pub(crate) fn put_publish_request(&self, request: PublishRequest) -> Result<()> {
        validate_record_version(request.format_ver, "publish request")?;
        if request.request_id.is_empty() || request.publish_operation_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "publish request and operation ids cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        {
            let completed = write.open_table(COMPLETED_REQUESTS).map_err(storage)?;
            if completed
                .get(request.request_id.as_str())
                .map_err(storage)?
                .is_some()
            {
                return Err(LocalStateError::Conflict(format!(
                    "publish request id {} is already completed",
                    request.request_id
                )));
            }
            if request.requires_ack {
                let mut undelivered = 0usize;
                for row in completed.iter().map_err(storage)? {
                    let (_, value) = row.map_err(storage)?;
                    let record = decode::<CompletedPublishRequest>(
                        value.value(),
                        "completed publish request",
                    )?;
                    if !record.acknowledged {
                        undelivered += 1;
                    }
                }
                if undelivered >= COMPLETED_REQUEST_LIMIT {
                    return Err(LocalStateError::Conflict(format!(
                        "{undelivered} completed snapshot responses are still undelivered; \
                         retry/inspect them before accepting another external request"
                    )));
                }
            }
        }
        let mut generation = generation_from_write(&write, request.generation)?;
        if let Some(existing) = publish_request_for_generation(&write, request.generation)? {
            if existing == request {
                return Ok(());
            }
            return Err(LocalStateError::Conflict(format!(
                "generation {} already has publish request {}",
                request.generation, existing.request_id
            )));
        }
        match generation.state {
            GenerationState::Frozen | GenerationState::Prepared => {}
            GenerationState::PublishRequested => {
                return Err(LocalStateError::Corrupt(format!(
                    "generation {} is publish-requested but has no request record",
                    request.generation
                )));
            }
            other => {
                return Err(LocalStateError::InvalidTransition(format!(
                    "cannot request publish for generation {} in state {other:?}",
                    request.generation
                )));
            }
        }
        {
            let mut table = write.open_table(REQUESTS).map_err(storage)?;
            if let Some(existing) = table.get(request.request_id.as_str()).map_err(storage)? {
                let existing = decode::<PublishRequest>(existing.value(), "publish request")?;
                if existing != request {
                    return Err(LocalStateError::Conflict(format!(
                        "publish request id {} is already used",
                        request.request_id
                    )));
                }
            }
            let encoded = encode(&request)?;
            table
                .insert(request.request_id.as_str(), encoded.as_slice())
                .map_err(storage)?;
        }
        if generation.state == GenerationState::Prepared {
            generation.state = GenerationState::PublishRequested;
            put_generation(&write, &generation)?;
        }
        write.commit().map_err(storage)
    }

    #[cfg(test)]
    pub(crate) fn publish_request(&self, request_id: &str) -> Result<Option<PublishRequest>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(REQUESTS).map_err(storage)?;
        let Some(raw) = table.get(request_id).map_err(storage)? else {
            return Ok(None);
        };
        let request = decode::<PublishRequest>(raw.value(), "publish request")?;
        validate_record_version(request.format_ver, "publish request")?;
        Ok(Some(request))
    }

    pub(crate) fn publish_requests(&self) -> Result<Vec<PublishRequest>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(REQUESTS).map_err(storage)?;
        let mut requests = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (_, value) = row.map_err(storage)?;
            let request = decode::<PublishRequest>(value.value(), "publish request")?;
            validate_record_version(request.format_ver, "publish request")?;
            requests.push(request);
        }
        Ok(requests)
    }

    pub(crate) fn fail_publish_request(&self, request_id: &str, reason: &str) -> Result<()> {
        if reason.trim().is_empty() {
            return Err(LocalStateError::Conflict(
                "publish failure reason cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        {
            let mut table = write.open_table(REQUESTS).map_err(storage)?;
            let mut request = {
                let raw = table.get(request_id).map_err(storage)?.ok_or_else(|| {
                    LocalStateError::InvalidTransition(format!(
                        "publish request {request_id} does not exist"
                    ))
                })?;
                decode::<PublishRequest>(raw.value(), "publish request")?
            };
            if request.failure.as_deref() == Some(reason) {
                return Ok(());
            }
            if request.failure.is_some() {
                return Err(LocalStateError::Conflict(format!(
                    "publish request {request_id} is already failed"
                )));
            }
            request.failure = Some(reason.to_string());
            let encoded = encode(&request)?;
            table
                .insert(request_id, encoded.as_slice())
                .map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    pub(crate) fn completed_publish_request(
        &self,
        request_id: &str,
    ) -> Result<Option<CompletedPublishRequest>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(COMPLETED_REQUESTS).map_err(storage)?;
        let Some(raw) = table.get(request_id).map_err(storage)? else {
            return Ok(None);
        };
        let completed =
            decode::<CompletedPublishRequest>(raw.value(), "completed publish request")?;
        validate_completed_request(&completed, request_id)?;
        Ok(Some(completed))
    }

    pub(crate) fn completed_publish_requests(&self) -> Result<Vec<CompletedPublishRequest>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(COMPLETED_REQUESTS).map_err(storage)?;
        let mut completed = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let request_id = key.value();
            let record =
                decode::<CompletedPublishRequest>(value.value(), "completed publish request")?;
            validate_completed_request(&record, request_id)?;
            completed.push(record);
        }
        completed.sort_by_key(|record| {
            (
                record.request.created_at_ms,
                record.request.request_id.clone(),
            )
        });
        Ok(completed)
    }

    /// Acknowledge that the caller delivered a durable completion receipt to its user.
    ///
    /// Retirement retains the receipt so response loss is recoverable. Long-lived direct-push
    /// stores mark it delivered only after printing the successful result; a crash before this
    /// call may repeat a success message, but can never repeat the publication.
    pub(crate) fn acknowledge_completed_publish_request(&self, request_id: &str) -> Result<()> {
        let write = self.immediate_write()?;
        {
            let mut table = write.open_table(COMPLETED_REQUESTS).map_err(storage)?;
            let mut completed = {
                let Some(raw) = table.get(request_id).map_err(storage)? else {
                    return Ok(());
                };
                decode::<CompletedPublishRequest>(raw.value(), "completed publish request")?
            };
            validate_completed_request(&completed, request_id)?;
            if completed.acknowledged {
                return Ok(());
            }
            completed.acknowledged = true;
            let encoded = encode(&completed)?;
            table
                .insert(request_id, encoded.as_slice())
                .map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    /// Monotonically upgrades an already queued generation to perform generation-safe cleanup.
    ///
    /// The first request keeps its attribution/message and idempotency key; a later explicit
    /// `snapshot --clear` may only strengthen the cleanup policy. This remains retry-stable and
    /// cannot turn a destructive request back into a non-clearing one.
    pub(crate) fn require_clear_after_publish(&self, generation: u64) -> Result<PublishRequest> {
        let write = self.immediate_write()?;
        let mut request = publish_request_for_generation(&write, generation)?.ok_or_else(|| {
            LocalStateError::InvalidTransition(format!(
                "generation {generation} has no publish request to upgrade"
            ))
        })?;
        if !request.clear_after_publish {
            request.clear_after_publish = true;
            let encoded = encode(&request)?;
            let mut table = write.open_table(REQUESTS).map_err(storage)?;
            table
                .insert(request.request_id.as_str(), encoded.as_slice())
                .map_err(storage)?;
        }
        write.commit().map_err(storage)?;
        Ok(request)
    }

    /// Records the immutable snapshot returned by the server while retaining all local evidence
    /// until retirement completes.
    pub(crate) fn mark_published(&self, generation: u64, snapshot_id: &str) -> Result<()> {
        if snapshot_id.is_empty() {
            return Err(LocalStateError::Conflict(
                "published snapshot id cannot be empty".to_string(),
            ));
        }
        let write = self.immediate_write()?;
        let mut record = generation_from_write(&write, generation)?;
        match record.state {
            GenerationState::PublishRequested => {
                record.state = GenerationState::Published;
                record.published_snapshot = Some(snapshot_id.to_string());
            }
            GenerationState::Published
                if record.published_snapshot.as_deref() == Some(snapshot_id) =>
            {
                return Ok(());
            }
            other => {
                return Err(LocalStateError::InvalidTransition(format!(
                    "cannot mark generation {generation} published in state {other:?}"
                )));
            }
        }
        put_generation(&write, &record)?;
        write.commit().map_err(storage)
    }

    pub(crate) fn claim_generation_capture(&self, generation: u64) -> Result<ArtifactOwnership> {
        let artifact_id = format!("generation-capture:{generation}");
        let relative_path = format!("staging/generations/{generation}");
        let write = self.immediate_write()?;
        generation_from_write(&write, generation)?;
        let mut table = write.open_table(ARTIFACTS).map_err(storage)?;
        if let Some(raw) = table.get(artifact_id.as_str()).map_err(storage)? {
            let record = decode::<ArtifactOwnership>(raw.value(), "artifact ownership")?;
            validate_artifact(&record, artifact_id.as_str())?;
            if record.generation != generation || record.relative_path != relative_path {
                return Err(LocalStateError::Conflict(format!(
                    "artifact {artifact_id} is already owned by incompatible state"
                )));
            }
            return Ok(record);
        }
        let record = ArtifactOwnership {
            format_ver: RECORD_VERSION,
            artifact_id: artifact_id.clone(),
            generation,
            relative_path,
            kind: "immutable-generation-capture".to_string(),
            bytes: 0,
        };
        let encoded = encode(&record)?;
        table
            .insert(artifact_id.as_str(), encoded.as_slice())
            .map_err(storage)?;
        drop(table);
        write.commit().map_err(storage)?;
        Ok(record)
    }

    pub(crate) fn set_generation_capture_bytes(&self, generation: u64, bytes: u64) -> Result<()> {
        let artifact_id = format!("generation-capture:{generation}");
        let write = self.immediate_write()?;
        {
            let mut table = write.open_table(ARTIFACTS).map_err(storage)?;
            let mut record = {
                let raw = table
                    .get(artifact_id.as_str())
                    .map_err(storage)?
                    .ok_or_else(|| {
                        LocalStateError::InvalidTransition(format!(
                            "generation {generation} has no capture ownership row"
                        ))
                    })?;
                decode::<ArtifactOwnership>(raw.value(), "artifact ownership")?
            };
            validate_artifact(&record, artifact_id.as_str())?;
            record.bytes = bytes;
            let encoded = encode(&record)?;
            table
                .insert(artifact_id.as_str(), encoded.as_slice())
                .map_err(storage)?;
        }
        write.commit().map_err(storage)
    }

    pub(crate) fn artifacts(&self) -> Result<Vec<ArtifactOwnership>> {
        artifact_rows(self.db.as_ref())
    }

    #[cfg(test)]
    pub(crate) fn sealed_baseline(&self, path: &str) -> Result<Option<SealedBaseline>> {
        let path = validate_path(path)?;
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(SEALED).map_err(storage)?;
        let Some(raw) = table.get(path.as_str()).map_err(storage)? else {
            return Ok(None);
        };
        let baseline = decode::<SealedBaseline>(raw.value(), "sealed baseline")?;
        validate_baseline(&baseline, path.as_str())?;
        Ok(Some(baseline))
    }

    pub(crate) fn sealed_baselines(&self) -> Result<Vec<SealedBaseline>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(SEALED).map_err(storage)?;
        let mut baselines = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let path = key.value();
            let baseline = decode::<SealedBaseline>(value.value(), "sealed baseline")?;
            validate_baseline(&baseline, path)?;
            baselines.push(baseline);
        }
        Ok(baselines)
    }

    /// Atomically adopts a published snapshot, updates changed sealed baselines, and retires every
    /// local lifecycle row for that generation. Later-generation mutations remain untouched.
    pub(crate) fn retire_published(
        &self,
        generation: u64,
        snapshot_id: &str,
        baseline_updates: &[SealedBaseline],
        baseline_removals: &[String],
        completed_response: Vec<u8>,
    ) -> Result<()> {
        for baseline in baseline_updates {
            validate_baseline(baseline, &baseline.path)?;
            if baseline.snapshot_id != snapshot_id {
                return Err(LocalStateError::Conflict(format!(
                    "sealed baseline {} names snapshot {}, expected {snapshot_id}",
                    baseline.path, baseline.snapshot_id
                )));
            }
        }
        let baseline_removals = baseline_removals
            .iter()
            .map(|path| validate_path(path))
            .collect::<Result<Vec<_>>>()?;

        let write = self.immediate_write()?;
        let mut meta = read_meta_from_write(&write)?;
        let record = generation_from_write(&write, generation)?;
        ensure_state(&record, GenerationState::Published)?;
        if record.published_snapshot.as_deref() != Some(snapshot_id) {
            return Err(LocalStateError::Conflict(format!(
                "generation {generation} published snapshot does not match {snapshot_id}"
            )));
        }
        if generation <= meta.last_retired_generation {
            return Ok(());
        }
        if generation != meta.last_retired_generation.saturating_add(1) {
            return Err(LocalStateError::InvalidTransition(format!(
                "cannot retire generation {generation} before older generation {}",
                meta.last_retired_generation.saturating_add(1)
            )));
        }
        let request = publish_request_for_generation(&write, generation)?.ok_or_else(|| {
            LocalStateError::Corrupt(format!(
                "published generation {generation} has no durable publish request"
            ))
        })?;

        {
            let mut sealed = write.open_table(SEALED).map_err(storage)?;
            for baseline in baseline_updates {
                let encoded = encode(baseline)?;
                sealed
                    .insert(baseline.path.as_str(), encoded.as_slice())
                    .map_err(storage)?;
            }
            for path in &baseline_removals {
                sealed.remove(path.as_str()).map_err(storage)?;
            }
        }
        {
            let completed = CompletedPublishRequest {
                format_ver: RECORD_VERSION,
                request: request.clone(),
                snapshot_id: snapshot_id.to_string(),
                response: completed_response,
                acknowledged: !request.requires_ack,
            };
            let encoded = encode(&completed)?;
            let mut table = write.open_table(COMPLETED_REQUESTS).map_err(storage)?;
            table
                .insert(request.request_id.as_str(), encoded.as_slice())
                .map_err(storage)?;
            let mut rows = table
                .iter()
                .map_err(storage)?
                .map(|row| {
                    let (key, value) = row.map_err(storage)?;
                    let request_id = key.value().to_string();
                    let record = decode::<CompletedPublishRequest>(
                        value.value(),
                        "completed publish request",
                    )?;
                    validate_completed_request(&record, &request_id)?;
                    Ok((record.request.created_at_ms, request_id))
                })
                .collect::<Result<Vec<_>>>()?;
            rows.sort();
            let mut remove_count = rows.len().saturating_sub(COMPLETED_REQUEST_LIMIT);
            for (_, request_id) in rows {
                if remove_count == 0 {
                    break;
                }
                let acknowledged = {
                    let Some(raw) = table.get(request_id.as_str()).map_err(storage)? else {
                        continue;
                    };
                    decode::<CompletedPublishRequest>(raw.value(), "completed publish request")?
                        .acknowledged
                };
                if !acknowledged {
                    continue;
                }
                table.remove(request_id.as_str()).map_err(storage)?;
                remove_count -= 1;
            }
        }
        remove_generation_rows(&write, generation)?;
        {
            let mut prepared = write.open_table(PREPARED).map_err(storage)?;
            prepared.remove(generation).map_err(storage)?;
        }
        {
            let mut captures = write.open_table(FROZEN_CAPTURES).map_err(storage)?;
            captures.remove(generation).map_err(storage)?;
        }
        remove_publish_requests_for_generation(&write, generation)?;
        {
            let mut generations = write.open_table(GENERATIONS).map_err(storage)?;
            generations.remove(generation).map_err(storage)?;
            let mut active =
                get_generation(&generations, meta.active_generation)?.ok_or_else(|| {
                    LocalStateError::Corrupt("active generation record is missing".to_string())
                })?;
            ensure_state(&active, GenerationState::Open)?;
            active.base_snapshot = Some(snapshot_id.to_string());
            let encoded = encode(&active)?;
            generations
                .insert(active.generation, encoded.as_slice())
                .map_err(storage)?;
        }
        meta.last_retired_generation = generation;
        write_meta(&write, &meta)?;
        write.commit().map_err(storage)
    }

    /// Removes retained-byte baselines after a generation-safe clean trim. This never changes the
    /// adopted server base or any dirty-generation row.
    pub(crate) fn remove_sealed_baselines(&self, paths: &[String]) -> Result<()> {
        let paths = paths
            .iter()
            .map(|path| validate_path(path))
            .collect::<Result<Vec<_>>>()?;
        if paths.is_empty() {
            return Ok(());
        }
        let write = self.immediate_write()?;
        {
            let mut table = write.open_table(SEALED).map_err(storage)?;
            for path in paths {
                table.remove(path.as_str()).map_err(storage)?;
            }
        }
        write.commit().map_err(storage)
    }

    fn immediate_write(&self) -> Result<redb::WriteTransaction> {
        let mut write = self.db.begin_write().map_err(storage)?;
        write
            .set_durability(Durability::Immediate)
            .map_err(storage)?;
        Ok(write)
    }

    fn read_meta(&self) -> Result<MetaRecord> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(META).map_err(storage)?;
        let raw = table
            .get(META_KEY)
            .map_err(storage)?
            .ok_or_else(|| LocalStateError::Corrupt("metadata record is missing".to_string()))?;
        let meta = decode::<MetaRecord>(raw.value(), "metadata")?;
        validate_meta(&meta, &self.identity)?;
        Ok(meta)
    }
}

impl LocalSnapshotStore for LocalState {
    fn fail_publish_request(&self, request_id: &str, reason: &str) -> Result<()> {
        LocalState::fail_publish_request(self, request_id, reason)
    }

    fn replace_prepared_for_rebase(
        &self,
        prepared: PreparedGeneration,
        next_publish_operation_id: &str,
    ) -> Result<PublishRequest> {
        LocalState::replace_prepared_for_rebase(self, prepared, next_publish_operation_id)
    }

    fn mark_published(&self, generation: u64, snapshot_id: &str) -> Result<()> {
        LocalState::mark_published(self, generation, snapshot_id)
    }
}

/// Strictly read-only view of an existing local state database.
pub(crate) struct LocalStateReader {
    db: Arc<ReadOnlyDatabase>,
    identity: LocalStateIdentity,
}

impl LocalStateReader {
    fn open(path: impl AsRef<Path>, identity: LocalStateIdentity) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        match std::fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => {
                return Err(LocalStateError::Corrupt(format!(
                    "{} is not a regular database file",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(LocalStateError::Missing(path));
            }
            Err(error) => return Err(storage(error)),
        }
        let db = Arc::new(ReadOnlyDatabase::open(&path).map_err(storage)?);
        validate_existing_database(db.as_ref(), &identity)?;
        Ok(Self { db, identity })
    }

    pub(crate) fn identity(&self) -> &LocalStateIdentity {
        &self.identity
    }

    pub(crate) fn active_generation(&self) -> Result<u64> {
        Ok(self.read_meta()?.active_generation)
    }

    pub(crate) fn generation(&self, generation: u64) -> Result<Option<GenerationRecord>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(GENERATIONS).map_err(storage)?;
        let Some(raw) = table.get(generation).map_err(storage)? else {
            return Ok(None);
        };
        let record = decode::<GenerationRecord>(raw.value(), "generation")?;
        validate_generation(&record, generation)?;
        Ok(Some(record))
    }

    pub(crate) fn generations(&self) -> Result<Vec<GenerationRecord>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(GENERATIONS).map_err(storage)?;
        let mut records = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let generation = key.value();
            let record = decode::<GenerationRecord>(value.value(), "generation")?;
            validate_generation(&record, generation)?;
            records.push(record);
        }
        Ok(records)
    }

    pub(crate) fn active_restore(&self) -> Result<Option<RestoreOperation>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(RESTORE_OPERATIONS).map_err(storage)?;
        let operation = table
            .get("active")
            .map_err(storage)?
            .map(|raw| decode::<RestoreOperation>(raw.value(), "restore operation"))
            .transpose()?;
        if let Some(operation) = operation.as_ref() {
            validate_restore_operation(operation)?;
        }
        Ok(operation)
    }

    pub(crate) fn failed_restore(&self) -> Result<Option<RestoreOperation>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(RESTORE_OPERATIONS).map_err(storage)?;
        let operation = table
            .get("failed")
            .map_err(storage)?
            .map(|raw| decode::<RestoreOperation>(raw.value(), "failed restore"))
            .transpose()?;
        if let Some(operation) = operation.as_ref() {
            validate_restore_operation(operation)?;
        }
        Ok(operation)
    }

    pub(crate) fn recovery_dirty_state(&self) -> Result<RecoveryDirtyState> {
        let meta = self.read_meta()?;
        let read = self.db.begin_read().map_err(storage)?;
        let dirty = read.open_table(DIRTY).map_err(storage)?;
        let renames = read.open_table(RENAMES).map_err(storage)?;
        let intents = read.open_table(INTENTS).map_err(storage)?;
        let mut paths = Vec::new();
        let mut rename_rows_out = Vec::new();
        let mut intent_rows_out = Vec::new();
        let mut maximum = meta.next_mutation_sequence.saturating_sub(1);
        for row in dirty.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let (generation, path) = key.value();
            let record = decode::<DirtyPath>(value.value(), "dirty path")?;
            validate_dirty(&record, generation, path)?;
            maximum = maximum.max(record.sequence);
            paths.push(record);
        }
        for row in renames.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let (generation, path) = key.value();
            let record = decode::<RenameIntent>(value.value(), "rename intent")?;
            validate_rename(&record, generation, path)?;
            maximum = maximum.max(record.sequence);
            rename_rows_out.push(record);
        }
        for row in intents.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let (generation, sequence) = key.value();
            let record = decode::<MutationJournalRecord>(value.value(), "mutation intent")?;
            validate_mutation_journal(&record, generation, sequence)?;
            maximum = maximum.max(record.sequence);
            intent_rows_out.push(record);
        }
        Ok(RecoveryDirtyState {
            active_generation: meta.active_generation,
            maximum_mutation_sequence: maximum,
            paths,
            renames: rename_rows_out,
            intents: intent_rows_out,
        })
    }

    #[cfg(test)]
    pub(crate) fn frozen_capture(&self, generation: u64) -> Result<Option<Vec<u8>>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(FROZEN_CAPTURES).map_err(storage)?;
        let Some(raw) = table.get(generation).map_err(storage)? else {
            return Ok(None);
        };
        let record = decode::<FrozenCaptureRecord>(raw.value(), "frozen capture")?;
        validate_frozen_capture(&record, generation)?;
        Ok(Some(record.capture))
    }

    pub(crate) fn prepared(&self, generation: u64) -> Result<Option<PreparedGeneration>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(PREPARED).map_err(storage)?;
        let Some(raw) = table.get(generation).map_err(storage)? else {
            return Ok(None);
        };
        let prepared = decode::<PreparedGeneration>(raw.value(), "prepared generation")?;
        validate_record_version(prepared.format_ver, "prepared generation")?;
        if prepared.generation != generation {
            return Err(LocalStateError::Corrupt(format!(
                "prepared generation key {generation} contains generation {}",
                prepared.generation
            )));
        }
        Ok(Some(prepared))
    }

    pub(crate) fn publish_requests(&self) -> Result<Vec<PublishRequest>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(REQUESTS).map_err(storage)?;
        let mut requests = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (_, value) = row.map_err(storage)?;
            let request = decode::<PublishRequest>(value.value(), "publish request")?;
            validate_record_version(request.format_ver, "publish request")?;
            requests.push(request);
        }
        Ok(requests)
    }

    pub(crate) fn completed_publish_requests(&self) -> Result<Vec<CompletedPublishRequest>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(COMPLETED_REQUESTS).map_err(storage)?;
        let mut completed = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let request_id = key.value();
            let record =
                decode::<CompletedPublishRequest>(value.value(), "completed publish request")?;
            validate_completed_request(&record, request_id)?;
            completed.push(record);
        }
        completed.sort_by_key(|record| {
            (
                record.request.created_at_ms,
                record.request.request_id.clone(),
            )
        });
        Ok(completed)
    }

    pub(crate) fn sealed_baselines(&self) -> Result<Vec<SealedBaseline>> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(SEALED).map_err(storage)?;
        let mut baselines = Vec::new();
        for row in table.iter().map_err(storage)? {
            let (key, value) = row.map_err(storage)?;
            let path = key.value();
            let baseline = decode::<SealedBaseline>(value.value(), "sealed baseline")?;
            validate_baseline(&baseline, path)?;
            baselines.push(baseline);
        }
        Ok(baselines)
    }

    pub(crate) fn artifacts(&self) -> Result<Vec<ArtifactOwnership>> {
        artifact_rows(self.db.as_ref())
    }

    fn read_meta(&self) -> Result<MetaRecord> {
        let read = self.db.begin_read().map_err(storage)?;
        let table = read.open_table(META).map_err(storage)?;
        let raw = table
            .get(META_KEY)
            .map_err(storage)?
            .ok_or_else(|| LocalStateError::Corrupt("metadata record is missing".to_string()))?;
        let meta = decode::<MetaRecord>(raw.value(), "metadata")?;
        validate_meta(&meta, &self.identity)?;
        Ok(meta)
    }
}

fn validate_existing_database(
    db: &impl ReadableDatabase,
    identity: &LocalStateIdentity,
) -> Result<()> {
    let read = db.begin_read().map_err(storage)?;
    let meta = {
        let table = read.open_table(META).map_err(|error| {
            LocalStateError::Corrupt(format!("metadata table is unavailable: {error}"))
        })?;
        let raw = table
            .get(META_KEY)
            .map_err(storage)?
            .ok_or_else(|| LocalStateError::Corrupt("metadata record is missing".to_string()))?;
        decode::<MetaRecord>(raw.value(), "metadata")?
    };
    validate_meta(&meta, identity)?;
    {
        let generations = read.open_table(GENERATIONS).map_err(|error| {
            LocalStateError::Corrupt(format!("generation table is unavailable: {error}"))
        })?;
        let raw = generations
            .get(meta.active_generation)
            .map_err(storage)?
            .ok_or_else(|| {
                LocalStateError::Corrupt("active generation record is missing".to_string())
            })?;
        let active = decode::<GenerationRecord>(raw.value(), "generation")?;
        validate_generation(&active, meta.active_generation)?;
        ensure_state(&active, GenerationState::Open)?;
    }
    // All tables are mandatory in schema v5. A missing table is corruption, not an empty view.
    read.open_table(DIRTY).map_err(|error| {
        LocalStateError::Corrupt(format!("dirty table is unavailable: {error}"))
    })?;
    read.open_table(RENAMES).map_err(|error| {
        LocalStateError::Corrupt(format!("rename table is unavailable: {error}"))
    })?;
    read.open_table(INTENTS).map_err(|error| {
        LocalStateError::Corrupt(format!("mutation intent table is unavailable: {error}"))
    })?;
    read.open_table(FROZEN_CAPTURES).map_err(|error| {
        LocalStateError::Corrupt(format!("frozen capture table is unavailable: {error}"))
    })?;
    read.open_table(PREPARED).map_err(|error| {
        LocalStateError::Corrupt(format!("prepared table is unavailable: {error}"))
    })?;
    read.open_table(REQUESTS).map_err(|error| {
        LocalStateError::Corrupt(format!("request table is unavailable: {error}"))
    })?;
    read.open_table(COMPLETED_REQUESTS).map_err(|error| {
        LocalStateError::Corrupt(format!("completed request table is unavailable: {error}"))
    })?;
    read.open_table(RESTORE_OPERATIONS).map_err(|error| {
        LocalStateError::Corrupt(format!("restore operation table is unavailable: {error}"))
    })?;
    read.open_table(SEALED).map_err(|error| {
        LocalStateError::Corrupt(format!("sealed table is unavailable: {error}"))
    })?;
    read.open_table(ARTIFACTS).map_err(|error| {
        LocalStateError::Corrupt(format!("artifact ownership table is unavailable: {error}"))
    })?;
    Ok(())
}

fn artifact_rows(db: &impl ReadableDatabase) -> Result<Vec<ArtifactOwnership>> {
    let read = db.begin_read().map_err(storage)?;
    let table = read.open_table(ARTIFACTS).map_err(storage)?;
    let mut records = Vec::new();
    for row in table.iter().map_err(storage)? {
        let (key, value) = row.map_err(storage)?;
        let record = decode::<ArtifactOwnership>(value.value(), "artifact ownership")?;
        validate_artifact(&record, key.value())?;
        records.push(record);
    }
    records.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
    Ok(records)
}

fn next_receipt(meta: &mut MetaRecord) -> Result<MutationReceipt> {
    let receipt = MutationReceipt {
        generation: meta.active_generation,
        sequence: meta.next_mutation_sequence,
    };
    meta.next_mutation_sequence = meta.next_mutation_sequence.checked_add(1).ok_or_else(|| {
        LocalStateError::InvalidTransition("mutation sequence counter overflow".to_string())
    })?;
    Ok(receipt)
}

fn validate_path(path: &str) -> Result<String> {
    let candidate = Path::new(path);
    if path.is_empty() || candidate.is_absolute() || path.contains('\0') {
        return Err(LocalStateError::InvalidPath(path.to_string()));
    }
    for component in candidate.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(LocalStateError::InvalidPath(path.to_string()));
        }
    }
    let normalized = candidate.to_string_lossy().replace('\\', "/");
    if normalized != path {
        return Err(LocalStateError::InvalidPath(path.to_string()));
    }
    Ok(normalized)
}

fn is_path_at_or_below(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn remap_path_at_or_below(path: &str, source: &str, destination: &str) -> Option<String> {
    if path == source {
        return Some(destination.to_string());
    }
    path.strip_prefix(source)
        .filter(|suffix| suffix.starts_with('/'))
        .map(|suffix| format!("{destination}{suffix}"))
}

fn storage(error: impl std::fmt::Display) -> LocalStateError {
    LocalStateError::Storage(error.to_string())
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| LocalStateError::Corrupt(error.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8], kind: &str) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| LocalStateError::Corrupt(format!("invalid {kind}: {error}")))
}

fn validate_record_version(version: u16, kind: &str) -> Result<()> {
    if version != RECORD_VERSION {
        return Err(LocalStateError::Corrupt(format!(
            "{kind} has unsupported record version {version}"
        )));
    }
    Ok(())
}

fn validate_meta(meta: &MetaRecord, expected: &LocalStateIdentity) -> Result<()> {
    validate_record_version(meta.format_ver, "metadata")?;
    if meta.schema_ver != SCHEMA_VERSION {
        return Err(LocalStateError::UnsupportedSchema {
            expected: SCHEMA_VERSION,
            found: meta.schema_ver,
        });
    }
    if &meta.identity != expected {
        return Err(LocalStateError::IdentityMismatch {
            expected: expected.clone(),
            found: meta.identity.clone(),
        });
    }
    Ok(())
}

fn validate_generation(record: &GenerationRecord, key: u64) -> Result<()> {
    validate_record_version(record.format_ver, "generation")?;
    if record.generation != key {
        return Err(LocalStateError::Corrupt(format!(
            "generation key {key} contains generation {}",
            record.generation
        )));
    }
    Ok(())
}

fn validate_dirty(record: &DirtyPath, generation: u64, path: &str) -> Result<()> {
    validate_record_version(record.format_ver, "dirty path")?;
    if record.generation != generation || record.path != path {
        return Err(LocalStateError::Corrupt(format!(
            "dirty row ({generation}, {path}) contains ({}, {})",
            record.generation, record.path
        )));
    }
    Ok(())
}

fn validate_rename(record: &RenameIntent, generation: u64, destination: &str) -> Result<()> {
    validate_record_version(record.format_ver, "rename intent")?;
    if record.generation != generation || record.to != destination {
        return Err(LocalStateError::Corrupt(format!(
            "rename row ({generation}, {destination}) contains ({}, {})",
            record.generation, record.to
        )));
    }
    validate_path(&record.from)?;
    validate_path(&record.local_from)?;
    validate_path(&record.to)?;
    Ok(())
}

fn validate_mutation_journal(
    record: &MutationJournalRecord,
    generation: u64,
    sequence: u64,
) -> Result<()> {
    validate_record_version(record.format_ver, "mutation intent")?;
    if record.generation != generation || record.sequence != sequence {
        return Err(LocalStateError::Corrupt(format!(
            "mutation intent row ({generation}, {sequence}) contains ({}, {})",
            record.generation, record.sequence
        )));
    }
    match &record.intent {
        MutationIntent::Upsert { path, .. } | MutationIntent::Delete { path } => {
            validate_path(path)?;
            if record.namespace_applied.is_some() {
                return Err(LocalStateError::Corrupt(format!(
                    "content mutation ({generation}, {sequence}) has a namespace barrier"
                )));
            }
        }
        MutationIntent::Rename {
            source,
            destination,
        } => {
            validate_path(source)?;
            validate_path(destination)?;
            if source == destination || record.namespace_applied.is_none() {
                return Err(LocalStateError::Corrupt(format!(
                    "rename mutation ({generation}, {sequence}) has invalid application state"
                )));
            }
        }
    }
    Ok(())
}

fn validate_frozen_capture(record: &FrozenCaptureRecord, generation: u64) -> Result<()> {
    validate_record_version(record.format_ver, "frozen capture")?;
    if record.generation != generation {
        return Err(LocalStateError::Corrupt(format!(
            "frozen capture key {generation} contains generation {}",
            record.generation
        )));
    }
    Ok(())
}

fn validate_baseline(record: &SealedBaseline, key: &str) -> Result<()> {
    validate_record_version(record.format_ver, "sealed baseline")?;
    let path = validate_path(&record.path)?;
    if path != key {
        return Err(LocalStateError::Corrupt(format!(
            "sealed baseline key {key} contains path {}",
            record.path
        )));
    }
    Ok(())
}

fn validate_artifact(record: &ArtifactOwnership, key: &str) -> Result<()> {
    validate_record_version(record.format_ver, "artifact ownership")?;
    if record.artifact_id != key
        || record.artifact_id.is_empty()
        || record.kind.is_empty()
        || record.kind.len() > 64
    {
        return Err(LocalStateError::Corrupt(format!(
            "artifact ownership key {key} has incompatible identity"
        )));
    }
    validate_path(&record.relative_path)?;
    Ok(())
}

fn validate_completed_request(record: &CompletedPublishRequest, key: &str) -> Result<()> {
    validate_record_version(record.format_ver, "completed publish request")?;
    validate_record_version(record.request.format_ver, "publish request")?;
    if record.request.request_id != key {
        return Err(LocalStateError::Corrupt(format!(
            "completed request key {key} contains request {}",
            record.request.request_id
        )));
    }
    if record.snapshot_id.is_empty() {
        return Err(LocalStateError::Corrupt(format!(
            "completed request {key} has an empty snapshot id"
        )));
    }
    Ok(())
}

fn validate_restore_operation(operation: &RestoreOperation) -> Result<()> {
    validate_record_version(operation.format_ver, "restore operation")?;
    if operation.request_id.is_empty()
        || operation.target_snapshot_id.is_empty()
        || operation.expected_snapshot_id.is_empty()
        || operation
            .completed_snapshot_id
            .as_deref()
            .is_some_and(str::is_empty)
    {
        return Err(LocalStateError::Corrupt(
            "restore operation contains an empty identifier".to_string(),
        ));
    }
    if operation.locally_adopted && operation.completed_snapshot_id.is_none() {
        return Err(LocalStateError::Corrupt(format!(
            "restore {} is locally adopted without a server result",
            operation.request_id
        )));
    }
    if operation.failure.as_deref().is_some_and(str::is_empty)
        || (operation.failure.is_some()
            && (operation.completed_snapshot_id.is_some() || operation.locally_adopted))
    {
        return Err(LocalStateError::Corrupt(format!(
            "restore {} has incompatible failure/result state",
            operation.request_id
        )));
    }
    Ok(())
}

fn read_meta_from_write(write: &redb::WriteTransaction) -> Result<MetaRecord> {
    let table = write.open_table(META).map_err(storage)?;
    let raw = table
        .get(META_KEY)
        .map_err(storage)?
        .ok_or_else(|| LocalStateError::Corrupt("metadata record is missing".to_string()))?;
    let meta = decode::<MetaRecord>(raw.value(), "metadata")?;
    validate_record_version(meta.format_ver, "metadata")?;
    if meta.schema_ver != SCHEMA_VERSION {
        return Err(LocalStateError::UnsupportedSchema {
            expected: SCHEMA_VERSION,
            found: meta.schema_ver,
        });
    }
    Ok(meta)
}

fn active_restore_from_write(write: &redb::WriteTransaction) -> Result<Option<RestoreOperation>> {
    let table = write.open_table(RESTORE_OPERATIONS).map_err(storage)?;
    let operation = table
        .get("active")
        .map_err(storage)?
        .map(|raw| decode::<RestoreOperation>(raw.value(), "restore operation"))
        .transpose()?;
    if let Some(operation) = operation.as_ref() {
        validate_restore_operation(operation)?;
    }
    Ok(operation)
}

fn write_meta(write: &redb::WriteTransaction, meta: &MetaRecord) -> Result<()> {
    let mut table = write.open_table(META).map_err(storage)?;
    let encoded = encode(meta)?;
    table
        .insert(META_KEY, encoded.as_slice())
        .map_err(storage)?;
    Ok(())
}

fn get_generation(
    table: &impl ReadableTable<u64, &'static [u8]>,
    generation: u64,
) -> Result<Option<GenerationRecord>> {
    let Some(raw) = table.get(generation).map_err(storage)? else {
        return Ok(None);
    };
    let record = decode::<GenerationRecord>(raw.value(), "generation")?;
    validate_generation(&record, generation)?;
    Ok(Some(record))
}

fn generation_from_write(
    write: &redb::WriteTransaction,
    generation: u64,
) -> Result<GenerationRecord> {
    let table = write.open_table(GENERATIONS).map_err(storage)?;
    get_generation(&table, generation)?.ok_or_else(|| {
        LocalStateError::InvalidTransition(format!("generation {generation} does not exist"))
    })
}

fn put_generation(write: &redb::WriteTransaction, generation: &GenerationRecord) -> Result<()> {
    let mut table = write.open_table(GENERATIONS).map_err(storage)?;
    let encoded = encode(generation)?;
    table
        .insert(generation.generation, encoded.as_slice())
        .map_err(storage)?;
    Ok(())
}

fn ensure_state(record: &GenerationRecord, expected: GenerationState) -> Result<()> {
    if record.state != expected {
        return Err(LocalStateError::InvalidTransition(format!(
            "generation {} is {:?}, expected {expected:?}",
            record.generation, record.state
        )));
    }
    Ok(())
}

fn generation_has_dirty(write: &redb::WriteTransaction, generation: u64) -> Result<bool> {
    let table = write.open_table(DIRTY).map_err(storage)?;
    let mut rows = table
        .range((generation, "")..=(generation, "\u{10ffff}"))
        .map_err(storage)?;
    Ok(rows.next().transpose().map_err(storage)?.is_some())
}

fn has_inflight_before(write: &redb::WriteTransaction, active: u64) -> Result<bool> {
    let table = write.open_table(GENERATIONS).map_err(storage)?;
    for row in table.range(..active).map_err(storage)? {
        let (_, value) = row.map_err(storage)?;
        let record = decode::<GenerationRecord>(value.value(), "generation")?;
        if record.state != GenerationState::Open {
            return Ok(true);
        }
    }
    Ok(false)
}

fn inflight_generation_count(write: &redb::WriteTransaction, active: u64) -> Result<usize> {
    let table = write.open_table(GENERATIONS).map_err(storage)?;
    let mut count = 0usize;
    for row in table.range(..active).map_err(storage)? {
        let (_, value) = row.map_err(storage)?;
        let record = decode::<GenerationRecord>(value.value(), "generation")?;
        if record.state != GenerationState::Open {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

fn dirty_rows(
    table: &impl ReadableTable<(u64, &'static str), &'static [u8]>,
    generation: u64,
) -> Result<Vec<DirtyPath>> {
    let mut rows = Vec::new();
    for row in table
        .range((generation, "")..=(generation, "\u{10ffff}"))
        .map_err(storage)?
    {
        let (key, value) = row.map_err(storage)?;
        let (stored_generation, path) = key.value();
        let record = decode::<DirtyPath>(value.value(), "dirty path")?;
        validate_dirty(&record, stored_generation, path)?;
        rows.push(record);
    }
    Ok(rows)
}

fn rename_rows(
    table: &impl ReadableTable<(u64, &'static str), &'static [u8]>,
    generation: u64,
) -> Result<Vec<RenameIntent>> {
    let mut rows = Vec::new();
    for row in table
        .range((generation, "")..=(generation, "\u{10ffff}"))
        .map_err(storage)?
    {
        let (key, value) = row.map_err(storage)?;
        let (stored_generation, destination) = key.value();
        let record = decode::<RenameIntent>(value.value(), "rename intent")?;
        validate_rename(&record, stored_generation, destination)?;
        rows.push(record);
    }
    Ok(rows)
}

fn publish_request_for_generation(
    write: &redb::WriteTransaction,
    generation: u64,
) -> Result<Option<PublishRequest>> {
    let table = write.open_table(REQUESTS).map_err(storage)?;
    for row in table.iter().map_err(storage)? {
        let (_, value) = row.map_err(storage)?;
        let request = decode::<PublishRequest>(value.value(), "publish request")?;
        if request.generation == generation {
            return Ok(Some(request));
        }
    }
    Ok(None)
}

fn remove_generation_rows(write: &redb::WriteTransaction, generation: u64) -> Result<()> {
    {
        let mut table = write.open_table(ARTIFACTS).map_err(storage)?;
        let keys = table
            .iter()
            .map_err(storage)?
            .filter_map(|row| match row {
                Ok((key, value)) => {
                    let key = key.value().to_string();
                    match decode::<ArtifactOwnership>(value.value(), "artifact ownership") {
                        Ok(record) if record.generation == generation => Some(Ok(key)),
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    }
                }
                Err(error) => Some(Err(storage(error))),
            })
            .collect::<Result<Vec<_>>>()?;
        for key in keys {
            table.remove(key.as_str()).map_err(storage)?;
        }
    }
    {
        let mut table = write.open_table(INTENTS).map_err(storage)?;
        let keys = table
            .range((generation, 0)..=(generation, u64::MAX))
            .map_err(storage)?
            .map(|row| row.map(|(key, _)| key.value()).map_err(storage))
            .collect::<Result<Vec<_>>>()?;
        for key in keys {
            table.remove(&key).map_err(storage)?;
        }
    }
    {
        let mut table = write.open_table(DIRTY).map_err(storage)?;
        let keys = table
            .range((generation, "")..=(generation, "\u{10ffff}"))
            .map_err(storage)?
            .map(|row| {
                row.map(|(key, _)| {
                    let (generation, path) = key.value();
                    (generation, path.to_string())
                })
                .map_err(storage)
            })
            .collect::<Result<Vec<_>>>()?;
        for (generation, path) in keys {
            table
                .remove(&(generation, path.as_str()))
                .map_err(storage)?;
        }
    }
    {
        let mut table = write.open_table(RENAMES).map_err(storage)?;
        let keys = table
            .range((generation, "")..=(generation, "\u{10ffff}"))
            .map_err(storage)?
            .map(|row| {
                row.map(|(key, _)| {
                    let (generation, path) = key.value();
                    (generation, path.to_string())
                })
                .map_err(storage)
            })
            .collect::<Result<Vec<_>>>()?;
        for (generation, path) in keys {
            table
                .remove(&(generation, path.as_str()))
                .map_err(storage)?;
        }
    }
    Ok(())
}

fn remove_publish_requests_for_generation(
    write: &redb::WriteTransaction,
    generation: u64,
) -> Result<()> {
    let mut table = write.open_table(REQUESTS).map_err(storage)?;
    let keys = table
        .iter()
        .map_err(storage)?
        .filter_map(|row| match row {
            Ok((key, value)) => match decode::<PublishRequest>(value.value(), "publish request") {
                Ok(request) if request.generation == generation => {
                    Some(Ok(key.value().to_string()))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            },
            Err(error) => Some(Err(storage(error))),
        })
        .collect::<Result<Vec<_>>>()?;
    for key in keys {
        table.remove(key.as_str()).map_err(storage)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "recovery_model_tests.rs"]
mod recovery_model_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn identity() -> LocalStateIdentity {
        LocalStateIdentity {
            project_id: "project-1".to_string(),
            filesystem: "filesystem-1".to_string(),
            workspace_id: "workspace-1".to_string(),
            store_uuid: "store-1".to_string(),
        }
    }

    fn open(temp: &TempDir) -> LocalState {
        LocalState::open(temp.path().join(LOCAL_STATE_FILE), identity()).unwrap()
    }

    #[test]
    fn reopen_preserves_dirty_and_lifecycle_state() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        assert!(store.was_created());
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        assert_eq!(store.record_upsert("src/lib.rs", 64).unwrap().generation, 1);
        store.record_delete("old.txt").unwrap();
        store.record_rename("before", "after").unwrap();
        let frozen = store.freeze_current().unwrap().unwrap();
        assert_eq!(frozen.generation, 1);
        assert!(store.frozen_capture(frozen.generation).unwrap().is_none());
        store.record_upsert("raced.txt", 0).unwrap();
        drop(store);

        let reopened = open(&temp);
        assert!(!reopened.was_created());
        let dirty = reopened.dirty_generation(1).unwrap();
        assert_eq!(dirty.state, GenerationState::Frozen);
        assert!(dirty.paths.iter().any(|row| row.path == "src/lib.rs"));
        assert!(dirty.paths.iter().any(|row| row.path == "old.txt"));
        assert!(dirty.renames.iter().any(|row| row.to == "after"));
        assert_eq!(
            reopened.dirty_generation(2).unwrap().paths[0].path,
            "raced.txt"
        );

        let prepared = PreparedGeneration::new(
            1,
            Some("base-0".to_string()),
            "root-1",
            "fingerprint-1",
            vec![1, 2, 3],
        );
        reopened.mark_prepared(prepared.clone()).unwrap();
        let request = PublishRequest::new("request-1", 1, "save", false, 123);
        reopened.put_publish_request(request.clone()).unwrap();
        reopened.mark_published(1, "snapshot-1").unwrap();
        drop(reopened);

        let reopened = open(&temp);
        assert_eq!(reopened.prepared(1).unwrap(), Some(prepared));
        assert_eq!(
            reopened.publish_request("request-1").unwrap(),
            Some(request)
        );
        assert_eq!(
            reopened
                .generation(1)
                .unwrap()
                .unwrap()
                .published_snapshot
                .as_deref(),
            Some("snapshot-1")
        );
    }

    #[test]
    fn ordered_mutation_batch_is_atomic_and_crash_visible_after_reopen() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        let receipts = store
            .record_mutations(&[
                MutationIntent::Upsert {
                    path: "a".to_string(),
                    min_write_offset: 64,
                },
                MutationIntent::Upsert {
                    path: "a".to_string(),
                    min_write_offset: 8,
                },
                MutationIntent::Rename {
                    source: "a".to_string(),
                    destination: "b".to_string(),
                },
                MutationIntent::Delete {
                    path: "gone".to_string(),
                },
            ])
            .unwrap();
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        drop(store);

        let reopened = open(&temp);
        let generation = reopened.dirty_generation(1).unwrap();
        assert_eq!(
            generation
                .paths
                .iter()
                .map(|path| (path.path.as_str(), path.kind, path.sequence))
                .collect::<Vec<_>>(),
            vec![
                ("a", DirtyKind::Delete, 3),
                ("b", DirtyKind::Upsert, 3),
                ("gone", DirtyKind::Delete, 4),
            ]
        );
        assert_eq!(generation.renames.len(), 1);
        assert_eq!(generation.renames[0].from, "a");
        assert_eq!(generation.renames[0].to, "b");
        assert_eq!(generation.renames[0].sequence, 3);
    }

    #[test]
    fn invalid_mutation_aborts_the_entire_group_before_any_receipt_is_allocated() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        assert!(matches!(
            store.record_mutations(&[
                MutationIntent::Upsert {
                    path: "would-have-landed".to_string(),
                    min_write_offset: 0,
                },
                MutationIntent::Delete {
                    path: "../invalid".to_string(),
                },
            ]),
            Err(LocalStateError::InvalidPath(_))
        ));
        assert!(store.dirty_generation(1).unwrap().paths.is_empty());
        assert_eq!(
            store.record_delete("next").unwrap().sequence,
            1,
            "the rejected group consumed no durable ordering slots"
        );
    }

    #[test]
    fn frozen_capture_is_atomic_durable_and_removed_with_its_generation() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.record_upsert("first.txt", 0).unwrap();
        let first_capture = br#"{"paths":["first.txt"],"format_ver":1}"#.to_vec();
        let first = store
            .freeze_current_with_capture(first_capture.clone())
            .unwrap()
            .unwrap();
        store.record_upsert("later.txt", 0).unwrap();
        drop(store);

        let store = open(&temp);
        assert_eq!(
            store.frozen_capture(first.generation).unwrap(),
            Some(first_capture.clone())
        );
        drop(store);
        let reader =
            LocalState::open_existing(temp.path().join(LOCAL_STATE_FILE), identity()).unwrap();
        assert_eq!(
            reader.frozen_capture(first.generation).unwrap(),
            Some(first_capture)
        );
        drop(reader);

        let store = open(&temp);
        store
            .abort_unpublished_generation(first.generation)
            .unwrap();
        assert!(store.frozen_capture(first.generation).unwrap().is_none());

        let second_capture = vec![7, 8, 9];
        let second = store
            .freeze_current_with_capture(second_capture.clone())
            .unwrap()
            .unwrap();
        assert_eq!(
            store.frozen_capture(second.generation).unwrap(),
            Some(second_capture)
        );
        store
            .mark_prepared(PreparedGeneration::new(
                second.generation,
                second.base_snapshot.clone(),
                "root",
                "fingerprint",
                vec![],
            ))
            .unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "capture-request",
                second.generation,
                "save",
                false,
                1,
            ))
            .unwrap();
        store
            .mark_published(second.generation, "snapshot-capture")
            .unwrap();
        store
            .retire_published(second.generation, "snapshot-capture", &[], &[], vec![])
            .unwrap();
        assert!(store.frozen_capture(second.generation).unwrap().is_none());
        drop(store);

        let store = open(&temp);
        assert!(store.frozen_capture(second.generation).unwrap().is_none());
    }

    #[test]
    fn generation_artifacts_are_owned_before_use_and_retired_or_reset_atomically() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        store.record_upsert("captured.txt", 0).unwrap();
        let frozen = store.freeze_current().unwrap().unwrap();

        assert!(matches!(
            store.set_generation_capture_bytes(frozen.generation, 128),
            Err(LocalStateError::InvalidTransition(_))
        ));
        let ownership = store.claim_generation_capture(frozen.generation).unwrap();
        assert_eq!(ownership.artifact_id, "generation-capture:1");
        assert_eq!(ownership.relative_path, "staging/generations/1");
        assert_eq!(ownership.bytes, 0);
        assert_eq!(
            store.claim_generation_capture(frozen.generation).unwrap(),
            ownership,
            "claiming before materialization is retry-idempotent"
        );
        store
            .set_generation_capture_bytes(frozen.generation, 128)
            .unwrap();
        drop(store);

        let store = open(&temp);
        assert_eq!(store.artifacts().unwrap()[0].bytes, 128);
        store
            .mark_prepared(PreparedGeneration::new(
                frozen.generation,
                frozen.base_snapshot,
                "root-1",
                "fingerprint-1",
                vec![],
            ))
            .unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "request-1",
                frozen.generation,
                "save",
                false,
                1,
            ))
            .unwrap();
        store
            .mark_published(frozen.generation, "snapshot-1")
            .unwrap();
        store
            .retire_published(frozen.generation, "snapshot-1", &[], &[], vec![])
            .unwrap();
        assert!(
            store.artifacts().unwrap().is_empty(),
            "generation retirement removes artifacts whose owner no longer exists"
        );

        let active = store.active_generation().unwrap();
        store.claim_generation_capture(active).unwrap();
        store.set_generation_capture_bytes(active, 64).unwrap();
        assert_eq!(store.artifacts().unwrap().len(), 1);
        store.reset_after_restore("restored-snapshot").unwrap();
        assert!(
            store.artifacts().unwrap().is_empty(),
            "an explicit world reset cannot leave stale local artifact ownership behind"
        );
    }

    #[test]
    fn retirement_is_atomic_and_preserves_next_generation() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        store.record_upsert("sealed.txt", 0).unwrap();
        let generation = store.freeze_current().unwrap().unwrap().generation;
        store.record_upsert("next.txt", 99).unwrap();
        store
            .mark_prepared(PreparedGeneration::new(
                generation,
                Some("base-0".to_string()),
                "root",
                "fingerprint",
                vec![],
            ))
            .unwrap();
        store
            .put_publish_request(PublishRequest::new("request", generation, "save", false, 0))
            .unwrap();
        store.mark_published(generation, "snapshot-1").unwrap();
        let baseline = SealedBaseline::delete("sealed.txt", "snapshot-1");
        store
            .retire_published(
                generation,
                "snapshot-1",
                &[baseline.clone()],
                &[],
                br#"{"ok":true}"#.to_vec(),
            )
            .unwrap();

        assert!(store.generation(generation).unwrap().is_none());
        assert!(store.prepared(generation).unwrap().is_none());
        assert!(store.publish_requests().unwrap().is_empty());
        assert_eq!(store.dirty_generation(2).unwrap().paths[0].path, "next.txt");
        assert_eq!(
            store
                .generation(2)
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("snapshot-1")
        );
        assert_eq!(store.sealed_baseline("sealed.txt").unwrap(), Some(baseline));
        let completed = store
            .completed_publish_request("request")
            .unwrap()
            .expect("completed request receipt");
        assert_eq!(completed.snapshot_id, "snapshot-1");
        assert_eq!(completed.response, br#"{"ok":true}"#);
        assert_eq!(completed.request.generation, generation);
        assert!(!completed.acknowledged);
        store
            .acknowledge_completed_publish_request("request")
            .unwrap();
        drop(store);
        let store = open(&temp);
        assert!(
            store
                .completed_publish_request("request")
                .unwrap()
                .unwrap()
                .acknowledged,
            "delivery acknowledgement is durable without discarding publication evidence"
        );
        assert!(matches!(
            store.put_publish_request(PublishRequest::new(
                "request",
                generation + 1,
                "reused",
                false,
                1,
            )),
            Err(LocalStateError::Conflict(_))
        ));
    }

    #[test]
    fn later_generations_freeze_prepare_and_retire_in_order() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();

        store.record_upsert("first.txt", 0).unwrap();
        let first = store.freeze_current().unwrap().unwrap();
        store.record_upsert("second.txt", 0).unwrap();
        let second = store.freeze_current().unwrap().unwrap();
        store.record_upsert("still-open.txt", 0).unwrap();

        for generation in [first.generation, second.generation] {
            store
                .mark_prepared(PreparedGeneration::new(
                    generation,
                    Some("base-0".to_string()),
                    format!("root-{generation}"),
                    format!("fingerprint-{generation}"),
                    vec![generation as u8],
                ))
                .unwrap();
        }

        store
            .put_publish_request(PublishRequest::new(
                "request-1",
                first.generation,
                "first",
                false,
                1,
            ))
            .unwrap();
        store
            .mark_published(first.generation, "snapshot-1")
            .unwrap();
        store
            .retire_published(first.generation, "snapshot-1", &[], &[], vec![])
            .unwrap();

        assert_eq!(
            store
                .generation(store.active_generation().unwrap())
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("snapshot-1"),
            "the open generation follows each serialized publication"
        );
        assert!(
            store.prepared(second.generation).unwrap().is_some(),
            "retiring N preserves the already-prepared N+1 candidate"
        );

        let rebased = PreparedGeneration::new(
            second.generation,
            Some("snapshot-1".to_string()),
            "root-2-rebased",
            "fingerprint-2",
            vec![2],
        );
        store
            .put_publish_request(PublishRequest::new(
                "request-2",
                second.generation,
                "second",
                false,
                2,
            ))
            .unwrap();
        store
            .replace_prepared_for_rebase(rebased, "request-2-rebase-1")
            .unwrap();
        store
            .mark_published(second.generation, "snapshot-2")
            .unwrap();
        store
            .retire_published(second.generation, "snapshot-2", &[], &[], vec![])
            .unwrap();

        let active = store.active_generation().unwrap();
        assert_eq!(
            store
                .generation(active)
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("snapshot-2")
        );
        assert!(
            store
                .dirty_generation(active)
                .unwrap()
                .paths
                .iter()
                .any(|path| path.path == "still-open.txt"),
            "serial retirement never consumes the open generation"
        );
    }

    #[test]
    fn completed_publish_receipts_are_durable_and_bounded() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        for n in 0..(COMPLETED_REQUEST_LIMIT + 3) {
            store.record_upsert(&format!("file-{n}"), 0).unwrap();
            let frozen = store.freeze_current().unwrap().unwrap();
            store
                .mark_prepared(PreparedGeneration::new(
                    frozen.generation,
                    frozen.base_snapshot,
                    format!("root-{n}"),
                    format!("fingerprint-{n}"),
                    vec![],
                ))
                .unwrap();
            let request_id = format!("request-{n:03}");
            store
                .put_publish_request(
                    PublishRequest::new(request_id, frozen.generation, "save", false, n as u64)
                        .background(),
                )
                .unwrap();
            let snapshot_id = format!("snapshot-{n}");
            store
                .mark_published(frozen.generation, &snapshot_id)
                .unwrap();
            store
                .retire_published(
                    frozen.generation,
                    &snapshot_id,
                    &[],
                    &[],
                    format!("response-{n}").into_bytes(),
                )
                .unwrap();
        }
        assert_eq!(
            store.completed_publish_requests().unwrap().len(),
            COMPLETED_REQUEST_LIMIT
        );
        assert!(
            store
                .completed_publish_request("request-000")
                .unwrap()
                .is_none()
        );
        let newest = store
            .completed_publish_request(&format!("request-{:03}", COMPLETED_REQUEST_LIMIT + 2))
            .unwrap()
            .expect("newest receipt retained");
        assert_eq!(
            newest.response,
            format!("response-{}", COMPLETED_REQUEST_LIMIT + 2).into_bytes()
        );
        drop(store);

        let reader =
            LocalState::open_existing(temp.path().join(LOCAL_STATE_FILE), identity()).unwrap();
        assert_eq!(
            reader.completed_publish_requests().unwrap().len(),
            COMPLETED_REQUEST_LIMIT
        );
    }

    #[test]
    fn permanent_publish_failure_stops_retry_selection_and_preserves_evidence() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        store.record_upsert("must-survive.txt", 0).unwrap();
        let frozen = store.freeze_current().unwrap().unwrap();
        store.claim_generation_capture(frozen.generation).unwrap();
        store
            .set_generation_capture_bytes(frozen.generation, 17)
            .unwrap();
        let prepared = PreparedGeneration::new(
            frozen.generation,
            frozen.base_snapshot,
            "root-failed",
            "fingerprint-failed",
            vec![4, 5, 6],
        );
        store.mark_prepared(prepared.clone()).unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "request-failed",
                frozen.generation,
                "save",
                false,
                10,
            ))
            .unwrap();
        store
            .fail_publish_request("request-failed", "server rejected declaration")
            .unwrap();
        store
            .fail_publish_request("request-failed", "server rejected declaration")
            .unwrap();
        assert!(matches!(
            store.fail_publish_request("request-failed", "different diagnosis"),
            Err(LocalStateError::Conflict(_))
        ));
        drop(store);

        let store = open(&temp);
        let failed = store
            .publish_request("request-failed")
            .unwrap()
            .expect("dead-letter remains durable");
        assert_eq!(
            failed.failure.as_deref(),
            Some("server rejected declaration")
        );
        assert!(
            store
                .publish_requests()
                .unwrap()
                .into_iter()
                .filter(|request| request.failure.is_none())
                .all(|request| request.request_id != "request-failed"),
            "recovery selects only requests without a terminal failure"
        );
        assert_eq!(
            store.generation(frozen.generation).unwrap().unwrap().state,
            GenerationState::PublishRequested
        );
        assert_eq!(store.prepared(frozen.generation).unwrap(), Some(prepared));
        assert_eq!(
            store.dirty_generation(frozen.generation).unwrap().paths[0].path,
            "must-survive.txt"
        );
        assert_eq!(store.artifacts().unwrap()[0].bytes, 17);
    }

    #[test]
    fn unacknowledged_completion_never_aliases_a_new_snapshot_request() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();

        store.record_upsert("first.txt", 0).unwrap();
        let first = store.freeze_current().unwrap().unwrap();
        store
            .mark_prepared(PreparedGeneration::new(
                first.generation,
                first.base_snapshot,
                "root-a",
                "fingerprint-a",
                vec![],
            ))
            .unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "request-a",
                first.generation,
                "same user message",
                false,
                1,
            ))
            .unwrap();
        store
            .mark_published(first.generation, "snapshot-a")
            .unwrap();
        store
            .retire_published(
                first.generation,
                "snapshot-a",
                &[],
                &[],
                b"response-a".to_vec(),
            )
            .unwrap();
        assert!(
            !store
                .completed_publish_request("request-a")
                .unwrap()
                .unwrap()
                .acknowledged,
            "model response loss: A remains replayable"
        );

        store.record_upsert("second.txt", 0).unwrap();
        let second = store.freeze_current().unwrap().unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "request-b",
                second.generation,
                "same user message",
                false,
                2,
            ))
            .unwrap();

        assert!(
            store
                .completed_publish_request("request-b")
                .unwrap()
                .is_none(),
            "only an exact request id may replay a durable completion"
        );
        assert_eq!(
            store
                .publish_request("request-b")
                .unwrap()
                .unwrap()
                .generation,
            second.generation,
            "the new dirty generation remains queued instead of receiving A's receipt"
        );
        assert_eq!(
            store
                .completed_publish_requests()
                .unwrap()
                .into_iter()
                .map(|receipt| receipt.request.request_id)
                .collect::<Vec<_>>(),
            vec!["request-a"],
            "message and clear policy are not completion identities"
        );
    }

    #[test]
    fn identity_mismatch_and_invalid_paths_fail_closed() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        drop(store);
        let mut other = identity();
        other.store_uuid = "different".to_string();
        assert!(matches!(
            LocalState::open(temp.path().join(LOCAL_STATE_FILE), other),
            Err(LocalStateError::IdentityMismatch { .. })
        ));

        let store = open(&temp);
        assert!(matches!(
            store.record_upsert("../escape", 0),
            Err(LocalStateError::InvalidPath(_))
        ));
        assert!(matches!(
            store.record_delete("/absolute"),
            Err(LocalStateError::InvalidPath(_))
        ));
    }

    #[test]
    fn upserts_coalesce_minimum_offset_and_rename_chains() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.record_upsert("file", 100).unwrap();
        store.record_upsert("file", 50).unwrap();
        store.record_rename("a", "b").unwrap();
        store.record_rename("b", "c").unwrap();

        let dirty = store.dirty_generation(1).unwrap();
        let file = dirty.paths.iter().find(|row| row.path == "file").unwrap();
        assert_eq!(file.min_write_offset, 50);
        assert_eq!(
            dirty.renames,
            vec![RenameIntent {
                format_ver: RECORD_VERSION,
                generation: 1,
                sequence: 4,
                from: "a".to_string(),
                local_from: "b".to_string(),
                to: "c".to_string(),
                applied: false,
            }]
        );
    }

    #[test]
    fn rename_application_barrier_is_durable_in_both_recovery_views() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        let receipt = store.record_rename("source", "destination").unwrap();
        let recovery = store.recovery_dirty_state().unwrap();
        assert!(!recovery.renames[0].applied);
        assert_eq!(
            recovery
                .intents
                .iter()
                .find(|intent| intent.sequence == receipt.sequence)
                .unwrap()
                .namespace_applied,
            Some(false),
            "write-ahead persistence alone is not permission to replay a namespace move"
        );

        store.mark_rename_applied("source", "destination").unwrap();
        drop(store);

        let reopened = open(&temp);
        let recovery = reopened.recovery_dirty_state().unwrap();
        assert!(recovery.renames[0].applied);
        assert_eq!(
            recovery
                .intents
                .iter()
                .find(|intent| intent.sequence == receipt.sequence)
                .unwrap()
                .namespace_applied,
            Some(true),
            "the post-syscall barrier survives a daemon crash in the ordered intent log"
        );
    }

    #[test]
    fn legacy_import_is_atomic_and_exactly_once() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        assert!(store.needs_legacy_import().unwrap());
        drop(store);

        // Crash after the empty database transaction but before the import. The durable marker,
        // not the process-local was_created bit, keeps the reconstruction walk required.
        let store = open(&temp);
        assert!(!store.was_created());
        assert!(store.needs_legacy_import().unwrap());
        let imported = store
            .import_legacy_once(LegacyImport {
                base_snapshot: Some("base-legacy".to_string()),
                mutations: vec![
                    LegacyMutation::Upsert {
                        path: "upper.txt".to_string(),
                        min_write_offset: 0,
                    },
                    LegacyMutation::Delete {
                        path: "whiteout.txt".to_string(),
                    },
                    LegacyMutation::Rename {
                        from: "old-dir".to_string(),
                        to: "new-dir".to_string(),
                    },
                ],
            })
            .unwrap();
        assert!(imported);
        assert!(!store.needs_legacy_import().unwrap());
        drop(store);

        let store = open(&temp);
        let generation = store.generation(1).unwrap().unwrap();
        assert_eq!(generation.base_snapshot.as_deref(), Some("base-legacy"));
        let dirty = store.dirty_generation(1).unwrap();
        assert!(dirty.paths.iter().any(|row| row.path == "upper.txt"));
        assert!(dirty.paths.iter().any(|row| row.path == "whiteout.txt"));
        assert_eq!(dirty.renames[0].from, "old-dir");
        assert_eq!(dirty.renames[0].to, "new-dir");
        assert!(
            !store
                .import_legacy_once(LegacyImport {
                    base_snapshot: Some("wrong".to_string()),
                    mutations: vec![LegacyMutation::Delete {
                        path: "must-not-appear".to_string(),
                    }],
                })
                .unwrap()
        );
        assert!(
            store
                .dirty_generation(1)
                .unwrap()
                .paths
                .iter()
                .all(|row| row.path != "must-not-appear")
        );
    }

    #[test]
    fn publish_request_can_wait_for_prepare() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        store.record_upsert("file", 0).unwrap();
        let generation = store.freeze_current().unwrap().unwrap().generation;
        let request = PublishRequest::new("queued-request", generation, "save", false, 1);
        store.put_publish_request(request.clone()).unwrap();
        assert_eq!(
            store.generation(generation).unwrap().unwrap().state,
            GenerationState::Frozen,
            "a queued request must not claim preparation completed"
        );
        drop(store);

        let store = open(&temp);
        assert_eq!(
            store.publish_request("queued-request").unwrap(),
            Some(request)
        );
        store
            .mark_prepared(PreparedGeneration::new(
                generation,
                Some("base-0".to_string()),
                "root",
                "fingerprint",
                vec![],
            ))
            .unwrap();
        assert_eq!(
            store.generation(generation).unwrap().unwrap().state,
            GenerationState::PublishRequested
        );
    }

    #[test]
    fn later_clear_request_monotonically_upgrades_existing_publication() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base".to_string())).unwrap();
        store.record_upsert("file", 0).unwrap();
        let generation = store.freeze_current().unwrap().unwrap().generation;
        let request = PublishRequest::new("request", generation, "autosave", false, 1);
        store.put_publish_request(request).unwrap();

        let upgraded = store.require_clear_after_publish(generation).unwrap();
        assert!(upgraded.clear_after_publish);
        assert_eq!(upgraded.request_id, "request");
        assert_eq!(upgraded.message, "autosave");
        assert!(
            store
                .require_clear_after_publish(generation)
                .unwrap()
                .clear_after_publish
        );
    }

    #[test]
    fn abort_unpublished_generation_keeps_later_writes() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();
        store.record_upsert("aborted.txt", 0).unwrap();
        let generation = store.freeze_current().unwrap().unwrap().generation;
        store.record_upsert("later.txt", 0).unwrap();
        store
            .put_publish_request(PublishRequest::new("queued", generation, "save", false, 1))
            .unwrap();
        store.abort_unpublished_generation(generation).unwrap();

        assert!(store.generation(generation).unwrap().is_none());
        assert!(store.publish_requests().unwrap().is_empty());
        assert!(store.completed_publish_requests().unwrap().is_empty());
        assert_eq!(
            store.dirty_generation(generation + 1).unwrap().paths[0].path,
            "later.txt"
        );
        assert_eq!(
            store
                .generation(generation + 1)
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("base-0")
        );
    }

    #[test]
    fn restore_reset_discards_old_world_without_reusing_counters() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store.set_base_snapshot(Some("base-0".to_string())).unwrap();

        store.record_upsert("published.txt", 0).unwrap();
        let published_generation = store.freeze_current().unwrap().unwrap().generation;
        store
            .mark_prepared(PreparedGeneration::new(
                published_generation,
                Some("base-0".to_string()),
                "root-1",
                "fingerprint-1",
                vec![],
            ))
            .unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "publish-1",
                published_generation,
                "save",
                false,
                1,
            ))
            .unwrap();
        store
            .mark_published(published_generation, "snapshot-1")
            .unwrap();
        store
            .retire_published(
                published_generation,
                "snapshot-1",
                &[SealedBaseline::delete("published.txt", "snapshot-1")],
                &[],
                vec![],
            )
            .unwrap();

        store.record_upsert("discarded.txt", 7).unwrap();
        let discarded_generation = store
            .freeze_current_with_capture(vec![4, 5, 6])
            .unwrap()
            .unwrap()
            .generation;
        assert_eq!(
            store.frozen_capture(discarded_generation).unwrap(),
            Some(vec![4, 5, 6])
        );
        store
            .put_publish_request(PublishRequest::new(
                "queued-before-restore",
                discarded_generation,
                "save",
                false,
                2,
            ))
            .unwrap();
        let before = store.recovery_dirty_state().unwrap();
        let old_active = before.active_generation;
        let old_maximum_sequence = before.maximum_mutation_sequence;

        let restored_generation = store.reset_after_restore("snapshot-restored").unwrap();
        assert!(restored_generation > old_active);
        assert_eq!(store.generations().unwrap().len(), 1);
        let restored = store.generation(restored_generation).unwrap().unwrap();
        assert_eq!(restored.state, GenerationState::Open);
        assert_eq!(restored.base_snapshot.as_deref(), Some("snapshot-restored"));
        assert!(store.recovery_dirty_state().unwrap().paths.is_empty());
        assert!(store.publish_requests().unwrap().is_empty());
        assert!(store.completed_publish_requests().unwrap().is_empty());
        assert!(store.prepared(discarded_generation).unwrap().is_none());
        assert!(
            store
                .frozen_capture(discarded_generation)
                .unwrap()
                .is_none()
        );
        assert!(store.sealed_baselines().unwrap().is_empty());
        assert!(!store.needs_legacy_import().unwrap());

        let receipt = store.record_upsert("post-restore.txt", 0).unwrap();
        assert_eq!(receipt.generation, restored_generation);
        assert!(receipt.sequence > old_maximum_sequence);
    }

    #[test]
    fn restore_race_rebases_the_open_generation_without_losing_dirty_paths() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store
            .import_legacy_once(LegacyImport {
                base_snapshot: Some("base-before-restore".to_string()),
                mutations: Vec::new(),
            })
            .unwrap();
        let first = store.record_upsert("raced.txt", 17).unwrap();
        store.record_delete("also-raced.txt").unwrap();

        store
            .adopt_restored_base_preserving_open_dirty("restored-base")
            .unwrap();

        let active = store.generation(first.generation).unwrap().unwrap();
        assert_eq!(active.state, GenerationState::Open);
        assert_eq!(active.base_snapshot.as_deref(), Some("restored-base"));
        let dirty = store.dirty_generation(first.generation).unwrap();
        assert_eq!(dirty.paths.len(), 2);
        assert!(
            dirty
                .paths
                .iter()
                .any(|path| path.path == "raced.txt" && path.kind == DirtyKind::Upsert)
        );
        assert!(
            dirty
                .paths
                .iter()
                .any(|path| path.path == "also-raced.txt" && path.kind == DirtyKind::Delete)
        );
    }

    #[test]
    fn durable_restore_fences_mutations_and_replays_each_crash_phase() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store
            .import_legacy_once(LegacyImport {
                base_snapshot: Some("base-before-restore".to_string()),
                mutations: Vec::new(),
            })
            .unwrap();
        let operation = store.begin_restore("historical", false, 1234).unwrap();
        assert_eq!(operation.expected_snapshot_id, "base-before-restore");
        assert_eq!(operation.created_at_ms, 1234);
        assert!(!operation.locally_adopted);
        assert!(matches!(
            store.record_upsert("blocked.txt", 0),
            Err(LocalStateError::InvalidTransition(_))
        ));
        assert!(matches!(
            store.freeze_current(),
            Err(LocalStateError::InvalidTransition(_))
        ));
        drop(store);

        let store = open(&temp);
        let resumed = store.begin_restore("historical", false, 9999).unwrap();
        assert_eq!(
            resumed, operation,
            "retry reuses the original request id and timestamp"
        );
        store
            .record_restore_server_result(&operation.request_id, "restored-save")
            .unwrap();
        drop(store);

        let store = open(&temp);
        let resumed = store.begin_restore("historical", false, 9999).unwrap();
        assert_eq!(
            resumed.completed_snapshot_id.as_deref(),
            Some("restored-save")
        );
        let generation = store.finish_restore(&operation.request_id).unwrap();
        assert_eq!(
            store
                .generation(generation)
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("restored-save")
        );
        assert!(store.active_restore().unwrap().is_none());
        drop(store);

        let store = open(&temp);
        let completed = store.begin_restore("historical", false, 9999).unwrap();
        assert!(completed.locally_adopted);
        assert_eq!(completed.request_id, operation.request_id);
        assert_eq!(
            store.finish_restore(&operation.request_id).unwrap(),
            generation,
            "duplicate local adoption is idempotent"
        );
        store.acknowledge_restore(&operation.request_id).unwrap();
        let next = store.begin_restore("historical", false, 2000).unwrap();
        assert_ne!(next.request_id, operation.request_id);
    }

    #[test]
    fn restore_requires_explicit_discard_for_dirty_state_and_clears_it_atomically() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store
            .import_legacy_once(LegacyImport {
                base_snapshot: Some("base".to_string()),
                mutations: Vec::new(),
            })
            .unwrap();
        store.record_upsert("dirty.txt", 0).unwrap();
        assert!(matches!(
            store.begin_restore("historical", false, 1),
            Err(LocalStateError::InvalidTransition(_))
        ));
        let operation = store.begin_restore("historical", true, 1).unwrap();
        store
            .record_restore_server_result(&operation.request_id, "restored")
            .unwrap();
        let generation = store.finish_restore(&operation.request_id).unwrap();
        assert!(store.dirty_generation(generation).unwrap().paths.is_empty());
        assert_eq!(
            store
                .generation(generation)
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("restored")
        );
    }

    #[test]
    fn permanent_restore_failure_releases_the_fence_and_preserves_diagnostics() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        store
            .set_base_snapshot(Some("base-before-restore".to_string()))
            .unwrap();
        let operation = store.begin_restore("historical", false, 77).unwrap();
        assert!(matches!(
            store.record_upsert("blocked.txt", 0),
            Err(LocalStateError::InvalidTransition(_))
        ));
        store
            .fail_restore(&operation.request_id, "target snapshot was rejected")
            .unwrap();
        assert!(store.active_restore().unwrap().is_none());
        assert_eq!(
            store.failed_restore().unwrap().unwrap().failure.as_deref(),
            Some("target snapshot was rejected")
        );
        store.record_upsert("allowed-after-failure.txt", 0).unwrap();
        drop(store);

        let store = open(&temp);
        let failed = store.failed_restore().unwrap().expect("failure evidence");
        assert_eq!(failed.request_id, operation.request_id);
        assert_eq!(failed.target_snapshot_id, "historical");
        assert_eq!(failed.expected_snapshot_id, "base-before-restore");
        assert_eq!(failed.created_at_ms, 77);
        assert_eq!(
            failed.failure.as_deref(),
            Some("target snapshot was rejected")
        );
        assert!(
            store
                .dirty_generation(store.active_generation().unwrap())
                .unwrap()
                .paths
                .iter()
                .any(|path| path.path == "allowed-after-failure.txt"),
            "releasing the restore fence does not discard subsequent local work"
        );
        let retry = store.begin_restore("historical", true, 88).unwrap();
        assert_ne!(retry.request_id, operation.request_id);
        assert_eq!(
            store.failed_restore().unwrap().unwrap().request_id,
            operation.request_id,
            "starting an explicit retry preserves the earlier diagnostic row"
        );
    }

    #[test]
    fn strict_read_only_open_never_creates_missing_state() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join(LOCAL_STATE_FILE);
        assert!(matches!(
            LocalState::open_existing(&path, identity()),
            Err(LocalStateError::Missing(_))
        ));
        assert!(!path.exists());

        let store = open(&temp);
        store.record_upsert("dirty.txt", 0).unwrap();
        drop(store);
        let reader = LocalState::open_existing(&path, identity()).unwrap();
        assert_eq!(reader.active_generation().unwrap(), 1);
        assert_eq!(
            reader.recovery_dirty_state().unwrap().paths[0].path,
            "dirty.txt"
        );
        assert_eq!(reader.identity(), &identity());
    }

    #[test]
    fn writable_open_does_not_heal_missing_tables() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join(LOCAL_STATE_FILE);
        drop(open(&temp));

        let db = Database::open(&path).unwrap();
        let mut write = db.begin_write().unwrap();
        write.set_durability(Durability::Immediate).unwrap();
        assert!(write.delete_table(DIRTY).unwrap());
        write.commit().unwrap();
        drop(db);

        assert!(matches!(
            LocalState::open(&path, identity()),
            Err(LocalStateError::Corrupt(_))
        ));
        assert!(
            LocalState::open_existing(&path, identity()).is_err(),
            "read-only inspection observes the same missing-table corruption"
        );
    }

    #[test]
    fn clean_freeze_does_not_advance_generation() {
        let temp = TempDir::new().unwrap();
        let store = open(&temp);
        assert!(store.freeze_current().unwrap().is_none());
        assert_eq!(store.active_generation().unwrap(), 1);
    }
}
