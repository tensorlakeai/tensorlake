//! Executable oracle for the durable local snapshot-state recovery contract.
//!
//! The disk-backed store tests run the same transitions against the real backend. Keeping the
//! state-machine oracle separate makes crash points and the server-observation decision table easy
//! to review without coupling them to an embedded database's key encoding.

#![cfg(test)]

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoreIdentity {
    pub project: &'static str,
    pub filesystem: &'static str,
    pub workspace: &'static str,
}

impl StoreIdentity {
    fn test() -> Self {
        Self {
            project: "project-1",
            filesystem: "filesystem-1",
            workspace: "workspace-1",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GenerationState {
    Open,
    Frozen,
    Prepared,
    PublishRequested,
    Published,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Generation {
    pub state: GenerationState,
    pub base_snapshot: String,
    pub dirty_paths: BTreeSet<String>,
    pub prepared_root: Option<String>,
    pub publish_operation: Option<String>,
    pub published_snapshot: Option<String>,
}

impl Generation {
    fn open(base_snapshot: impl Into<String>) -> Self {
        Self {
            state: GenerationState::Open,
            base_snapshot: base_snapshot.into(),
            dirty_paths: BTreeSet::new(),
            prepared_root: None,
            publish_operation: None,
            published_snapshot: None,
        }
    }
}

/// What durable evidence recovery obtained from the server after losing a publish response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ServerObservation {
    /// The idempotent operation was never accepted.
    NotAccepted,
    /// The immutable snapshot exists, but the pointer still names the expected base.
    SnapshotStored { snapshot: String, pointer: String },
    /// The pointer already names the requested immutable snapshot.
    PointerAtRequested { snapshot: String },
    /// Another operation won the pointer CAS.
    PointerAdvancedElsewhere { requested: String, actual: String },
    /// The available evidence cannot distinguish the outcomes above.
    Ambiguous,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryAction {
    RetrySubmit { operation: String },
    RetryPointerAdvance { operation: String, snapshot: String },
    Adopt { snapshot: String },
    Conflict { requested: String, actual: String },
    FailClosed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreparedCandidateAction {
    Reuse,
    FailClosed,
}

/// A persisted prepared root is publishable only while it still describes the exact local source
/// identities frozen for that generation. A mismatch may eventually be handled by an explicit
/// reprepare operation, but startup must not silently publish the stale candidate.
pub(crate) fn expected_prepared_candidate_action(
    persisted_source_fingerprint: &str,
    observed_source_fingerprint: &str,
) -> PreparedCandidateAction {
    if persisted_source_fingerprint == observed_source_fingerprint {
        PreparedCandidateAction::Reuse
    } else {
        PreparedCandidateAction::FailClosed
    }
}

/// The production recovery implementation must be equivalent to this decision table.
pub(crate) fn expected_recovery_action(
    publish_operation: &str,
    expected_base: &str,
    observation: ServerObservation,
) -> RecoveryAction {
    match observation {
        ServerObservation::NotAccepted => RecoveryAction::RetrySubmit {
            operation: publish_operation.to_string(),
        },
        ServerObservation::SnapshotStored { snapshot, pointer } if pointer == expected_base => {
            RecoveryAction::RetryPointerAdvance {
                operation: publish_operation.to_string(),
                snapshot,
            }
        }
        ServerObservation::PointerAtRequested { snapshot } => RecoveryAction::Adopt { snapshot },
        ServerObservation::PointerAdvancedElsewhere { requested, actual } => {
            RecoveryAction::Conflict { requested, actual }
        }
        // A stored snapshot with an unexpected pointer is deliberately not guessed into a
        // conflict: without a positively observed CAS result it may be a torn/inconsistent read.
        ServerObservation::SnapshotStored { .. } | ServerObservation::Ambiguous => {
            RecoveryAction::FailClosed
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OpenError {
    Corrupt,
    IdentityMismatch,
}

/// Small durable-image model used to make the lifecycle assertions executable before exercising
/// the same scenarios against a concrete backend. `crash_reopen` clones only this durable image:
/// there is intentionally no separate in-memory dirty authority.
#[derive(Clone, Debug)]
struct ModelStore {
    identity: StoreIdentity,
    active_generation: u64,
    generations: BTreeMap<u64, Generation>,
    corrupt: bool,
}

impl ModelStore {
    fn create(identity: StoreIdentity, base_snapshot: &str) -> Self {
        Self {
            identity,
            active_generation: 1,
            generations: [(1, Generation::open(base_snapshot))].into_iter().collect(),
            corrupt: false,
        }
    }

    fn crash_reopen(&self, identity: &StoreIdentity) -> Result<Self, OpenError> {
        if self.corrupt {
            return Err(OpenError::Corrupt);
        }
        if &self.identity != identity {
            return Err(OpenError::IdentityMismatch);
        }
        Ok(self.clone())
    }

    fn record_upsert(&mut self, path: &str) {
        let active = self.generations.get_mut(&self.active_generation).unwrap();
        assert_eq!(active.state, GenerationState::Open);
        active.dirty_paths.insert(path.to_string());
    }

    fn freeze(&mut self) -> u64 {
        let frozen = self.active_generation;
        let base = {
            let generation = self.generations.get_mut(&frozen).unwrap();
            assert_eq!(generation.state, GenerationState::Open);
            generation.state = GenerationState::Frozen;
            generation.base_snapshot.clone()
        };
        self.active_generation += 1;
        self.generations
            .insert(self.active_generation, Generation::open(base));
        frozen
    }

    fn mark_prepared(&mut self, generation: u64, root: &str) {
        let generation = self.generations.get_mut(&generation).unwrap();
        assert_eq!(generation.state, GenerationState::Frozen);
        generation.state = GenerationState::Prepared;
        generation.prepared_root = Some(root.to_string());
    }

    fn request_publish(&mut self, generation: u64, operation: &str) {
        let generation = self.generations.get_mut(&generation).unwrap();
        assert_eq!(generation.state, GenerationState::Prepared);
        generation.state = GenerationState::PublishRequested;
        generation.publish_operation = Some(operation.to_string());
    }

    fn mark_published(&mut self, generation: u64, snapshot: &str) {
        let generation = self.generations.get_mut(&generation).unwrap();
        assert_eq!(generation.state, GenerationState::PublishRequested);
        generation.state = GenerationState::Published;
        generation.published_snapshot = Some(snapshot.to_string());
    }

    fn retire(&mut self, generation: u64) {
        let retired = self.generations.get_mut(&generation).unwrap();
        assert_eq!(retired.state, GenerationState::Published);
        retired.state = GenerationState::Retired;
        retired.dirty_paths.clear();
        retired.prepared_root = None;
        retired.publish_operation = None;
    }
}

#[test]
fn reopen_preserves_each_unfinished_generation_state() {
    let identity = StoreIdentity::test();
    let mut store = ModelStore::create(identity.clone(), "base-0");
    store.record_upsert("open.txt");
    let reopened = store.crash_reopen(&identity).unwrap();
    assert_eq!(
        reopened.generations[&1].state,
        GenerationState::Open,
        "an open generation and its dirty set survive"
    );
    assert!(reopened.generations[&1].dirty_paths.contains("open.txt"));

    let generation = store.freeze();
    let reopened = store.crash_reopen(&identity).unwrap();
    assert_eq!(
        reopened.generations[&generation].state,
        GenerationState::Frozen
    );
    assert_eq!(reopened.active_generation, generation + 1);

    store.mark_prepared(generation, "root-1");
    let reopened = store.crash_reopen(&identity).unwrap();
    assert_eq!(
        reopened.generations[&generation].state,
        GenerationState::Prepared
    );
    assert_eq!(
        reopened.generations[&generation].prepared_root.as_deref(),
        Some("root-1")
    );

    store.request_publish(generation, "publish-op-1");
    let reopened = store.crash_reopen(&identity).unwrap();
    assert_eq!(
        reopened.generations[&generation].state,
        GenerationState::PublishRequested
    );
    assert_eq!(
        reopened.generations[&generation]
            .publish_operation
            .as_deref(),
        Some("publish-op-1")
    );

    store.mark_published(generation, "snapshot-1");
    let reopened = store.crash_reopen(&identity).unwrap();
    assert_eq!(
        reopened.generations[&generation].state,
        GenerationState::Published
    );
    assert_eq!(
        reopened.generations[&generation]
            .published_snapshot
            .as_deref(),
        Some("snapshot-1"),
        "published-but-unretired evidence must survive so cleanup can resume"
    );
}

#[test]
fn writes_after_freeze_are_owned_by_the_later_generation() {
    let identity = StoreIdentity::test();
    let mut store = ModelStore::create(identity.clone(), "base-0");
    store.record_upsert("both.txt");
    store.record_upsert("old-only.txt");

    let frozen = store.freeze();
    store.record_upsert("both.txt");
    store.record_upsert("new-only.txt");

    let reopened = store.crash_reopen(&identity).unwrap();
    assert_eq!(
        reopened.generations[&frozen].dirty_paths,
        ["both.txt".to_string(), "old-only.txt".to_string()]
            .into_iter()
            .collect()
    );
    assert_eq!(
        reopened.generations[&(frozen + 1)].dirty_paths,
        ["both.txt".to_string(), "new-only.txt".to_string()]
            .into_iter()
            .collect()
    );
}

#[test]
fn response_loss_uses_one_idempotency_and_adoption_decision_table() {
    let operation = "publish-op-1";
    let base = "snapshot-0";

    assert_eq!(
        expected_recovery_action(operation, base, ServerObservation::NotAccepted),
        RecoveryAction::RetrySubmit {
            operation: operation.to_string()
        }
    );
    assert_eq!(
        expected_recovery_action(
            operation,
            base,
            ServerObservation::SnapshotStored {
                snapshot: "snapshot-1".into(),
                pointer: base.into(),
            }
        ),
        RecoveryAction::RetryPointerAdvance {
            operation: operation.to_string(),
            snapshot: "snapshot-1".into(),
        }
    );
    assert_eq!(
        expected_recovery_action(
            operation,
            base,
            ServerObservation::PointerAtRequested {
                snapshot: "snapshot-1".into()
            }
        ),
        RecoveryAction::Adopt {
            snapshot: "snapshot-1".into()
        }
    );
    assert_eq!(
        expected_recovery_action(
            operation,
            base,
            ServerObservation::PointerAdvancedElsewhere {
                requested: "snapshot-1".into(),
                actual: "snapshot-2".into(),
            }
        ),
        RecoveryAction::Conflict {
            requested: "snapshot-1".into(),
            actual: "snapshot-2".into(),
        }
    );
    assert_eq!(
        expected_recovery_action(operation, base, ServerObservation::Ambiguous),
        RecoveryAction::FailClosed
    );
    assert_eq!(
        expected_recovery_action(
            operation,
            base,
            ServerObservation::SnapshotStored {
                snapshot: "snapshot-1".into(),
                pointer: "unexpected".into(),
            }
        ),
        RecoveryAction::FailClosed
    );
}

#[test]
fn corrupt_or_wrong_identity_store_never_reopens_as_clean() {
    let identity = StoreIdentity::test();
    let mut store = ModelStore::create(identity.clone(), "base-0");
    store.record_upsert("must-not-be-forgotten.txt");

    let wrong = StoreIdentity {
        workspace: "other-workspace",
        ..identity.clone()
    };
    assert_eq!(
        store.crash_reopen(&wrong).unwrap_err(),
        OpenError::IdentityMismatch
    );

    store.corrupt = true;
    assert_eq!(
        store.crash_reopen(&identity).unwrap_err(),
        OpenError::Corrupt
    );

    assert_eq!(
        expected_prepared_candidate_action("frozen-identity", "frozen-identity"),
        PreparedCandidateAction::Reuse
    );
    assert_eq!(
        expected_prepared_candidate_action("frozen-identity", "changed-after-crash"),
        PreparedCandidateAction::FailClosed,
        "a recovered prepared root must not publish after its source identity changed"
    );
}

#[test]
fn retirement_reclaims_only_the_published_generation() {
    let identity = StoreIdentity::test();
    let mut store = ModelStore::create(identity.clone(), "base-0");
    store.record_upsert("generation-1.txt");
    let published = store.freeze();
    store.record_upsert("generation-2.txt");
    store.mark_prepared(published, "root-1");
    store.request_publish(published, "publish-op-1");
    store.mark_published(published, "snapshot-1");
    store.retire(published);

    let reopened = store.crash_reopen(&identity).unwrap();
    let retired = &reopened.generations[&published];
    assert_eq!(retired.state, GenerationState::Retired);
    assert!(retired.dirty_paths.is_empty());
    assert!(retired.prepared_root.is_none());
    assert!(retired.publish_operation.is_none());
    assert_eq!(
        retired.published_snapshot.as_deref(),
        Some("snapshot-1"),
        "retain the adopted result even after transient preparation rows are reclaimed"
    );
    assert!(
        reopened.generations[&(published + 1)]
            .dirty_paths
            .contains("generation-2.txt"),
        "retirement must not delete writes from the generation opened by freeze"
    );
}

/// Run the crash/reopen contract against the real redb-backed store, not just the executable
/// state-machine oracle above. Each `drop` is a process-death boundary: no in-memory mirror is
/// available to the next `LocalState::open`.
mod disk_backend {
    use super::super::{
        GenerationState as DiskGenerationState, LOCAL_STATE_FILE, LocalState, LocalStateError,
        LocalStateIdentity, PreparedGeneration, PublishRequest, SealedBaseline,
    };

    fn identity() -> LocalStateIdentity {
        LocalStateIdentity {
            project_id: "project-1".to_string(),
            filesystem: "filesystem-1".to_string(),
            workspace_id: "workspace-1".to_string(),
            store_uuid: "store-1".to_string(),
        }
    }

    fn open(temp: &tempfile::TempDir) -> LocalState {
        LocalState::open(temp.path().join(LOCAL_STATE_FILE), identity()).unwrap()
    }

    #[test]
    fn real_store_reopens_open_frozen_prepared_and_publish_requested_states() {
        let temp = tempfile::tempdir().unwrap();

        // Crash with an open dirty generation.
        let store = open(&temp);
        store
            .set_base_snapshot(Some("snapshot-0".to_string()))
            .unwrap();
        store.record_upsert("open.txt", 17).unwrap();
        drop(store);

        let store = open(&temp);
        let recovered = store.recovery_dirty_state().unwrap();
        assert_eq!(recovered.active_generation, 1);
        assert_eq!(recovered.paths.len(), 1);
        assert_eq!(recovered.paths[0].path, "open.txt");
        assert_eq!(recovered.paths[0].min_write_offset, 17);

        // Freeze is atomic: after reopen generation 1 is immutable and generation 2 accepts
        // subsequent writes.
        let frozen = store.freeze_current().unwrap().unwrap();
        assert_eq!(frozen.generation, 1);
        store.record_upsert("later.txt", 0).unwrap();
        drop(store);

        let store = open(&temp);
        assert_eq!(store.active_generation().unwrap(), 2);
        assert_eq!(
            store.generation(1).unwrap().unwrap().state,
            DiskGenerationState::Frozen
        );
        assert_eq!(store.dirty_generation(1).unwrap().paths[0].path, "open.txt");
        assert_eq!(
            store.dirty_generation(2).unwrap().paths[0].path,
            "later.txt",
            "a write acknowledged after freeze must survive only in the later generation"
        );

        let prepared = PreparedGeneration::new(
            1,
            Some("snapshot-0".to_string()),
            "root-1",
            "source-fingerprint-1",
            vec![1, 2, 3],
        );
        store.mark_prepared(prepared.clone()).unwrap();
        drop(store);

        let store = open(&temp);
        assert_eq!(
            store.generation(1).unwrap().unwrap().state,
            DiskGenerationState::Prepared
        );
        assert_eq!(store.prepared(1).unwrap(), Some(prepared));

        let request = PublishRequest::new("publish-operation-1", 1, "durable save", false, 123);
        store.put_publish_request(request.clone()).unwrap();
        drop(store);

        let store = open(&temp);
        assert_eq!(
            store.generation(1).unwrap().unwrap().state,
            DiskGenerationState::PublishRequested
        );
        assert_eq!(
            store.publish_request("publish-operation-1").unwrap(),
            Some(request),
            "the server idempotency key must be durable before any retry"
        );
    }

    #[test]
    fn real_store_reopens_published_unretired_then_retires_without_losing_later_writes() {
        let temp = tempfile::tempdir().unwrap();
        let store = open(&temp);
        store
            .set_base_snapshot(Some("snapshot-0".to_string()))
            .unwrap();
        store.record_delete("gone.txt").unwrap();
        let generation = store.freeze_current().unwrap().unwrap().generation;
        store.record_upsert("later.txt", 9).unwrap();
        store
            .mark_prepared(PreparedGeneration::new(
                generation,
                Some("snapshot-0".to_string()),
                "root-1",
                "source-fingerprint-1",
                vec![4, 5, 6],
            ))
            .unwrap();
        store
            .put_publish_request(PublishRequest::new(
                "publish-operation-1",
                generation,
                "durable save",
                false,
                123,
            ))
            .unwrap();
        store.mark_published(generation, "snapshot-1").unwrap();
        drop(store);

        // This is the response-lost / crash-before-cleanup image. Both the adopted result and all
        // evidence needed for idempotent retirement remain present.
        let store = open(&temp);
        let published = store.generation(generation).unwrap().unwrap();
        assert_eq!(published.state, DiskGenerationState::Published);
        assert_eq!(published.published_snapshot.as_deref(), Some("snapshot-1"));
        assert!(store.prepared(generation).unwrap().is_some());
        assert!(
            store
                .publish_request("publish-operation-1")
                .unwrap()
                .is_some()
        );

        let baseline = SealedBaseline::delete("gone.txt", "snapshot-1");
        store
            .retire_published(generation, "snapshot-1", &[baseline.clone()], &[], vec![])
            .unwrap();
        drop(store);

        let store = open(&temp);
        assert!(store.generation(generation).unwrap().is_none());
        assert!(store.prepared(generation).unwrap().is_none());
        assert!(
            store
                .publish_request("publish-operation-1")
                .unwrap()
                .is_none()
        );
        assert_eq!(store.sealed_baseline("gone.txt").unwrap(), Some(baseline));
        assert_eq!(
            store.dirty_generation(generation + 1).unwrap().paths[0].path,
            "later.txt",
            "retiring N must not delete bytes or journal evidence owned by N+1"
        );
        assert_eq!(
            store
                .generation(generation + 1)
                .unwrap()
                .unwrap()
                .base_snapshot
                .as_deref(),
            Some("snapshot-1")
        );
    }

    #[test]
    fn real_store_fails_closed_on_identity_mismatch_and_corrupt_database() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(LOCAL_STATE_FILE);
        let store = open(&temp);
        store.record_upsert("must-not-be-forgotten.txt", 0).unwrap();
        drop(store);

        let mut wrong = identity();
        wrong.workspace_id = "other-workspace".to_string();
        assert!(matches!(
            LocalState::open(&path, wrong),
            Err(LocalStateError::IdentityMismatch { .. })
        ));

        std::fs::write(&path, b"not a redb database").unwrap();
        assert!(
            LocalState::open(&path, identity()).is_err(),
            "corrupt state must never be replaced with a new apparently-clean database"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"not a redb database",
            "failed open preserves corrupt evidence for doctor/export"
        );
    }
}
