mod auth;
mod cache;
mod commands;
mod config;
mod error;
mod http;
mod output;
mod project;
mod python_ast;

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::num::NonZeroUsize;

use auth::context::CliContext;
use auth::guard::{ensure_auth, ensure_auth_and_project};
use config::resolver;
use error::CliError;

#[derive(Parser)]
#[command(
    name = "tl",
    about = concat!("Tensorlake CLI v", env!("CARGO_PKG_VERSION")),
    version,
    infer_subcommands = true,
    after_help = "\
Authentication:
  Use --api-key or TENSORLAKE_API_KEY for API key authentication
  Use --pat or TENSORLAKE_PAT for Personal Access Token authentication
  Use 'tl login' to obtain a PAT interactively"
)]
struct Cli {
    /// Show detailed error information and stack traces
    #[arg(long, env = "TENSORLAKE_DEBUG")]
    debug: bool,

    /// The TensorLake API server URL
    #[arg(long, env = "TENSORLAKE_API_URL")]
    api_url: Option<String>,

    /// The Tensorlake Cloud URL
    #[arg(long, env = "TENSORLAKE_CLOUD_URL")]
    cloud_url: Option<String>,

    /// The Tensorlake API key
    #[arg(long, env = "TENSORLAKE_API_KEY")]
    api_key: Option<String>,

    /// The Tensorlake Personal Access Token
    #[arg(long = "pat", env = "TENSORLAKE_PAT")]
    personal_access_token: Option<String>,

    /// The namespace to use
    #[arg(long, env = "INDEXIFY_NAMESPACE")]
    namespace: Option<String>,

    /// The organization ID to use
    #[arg(long, env = "TENSORLAKE_ORGANIZATION_ID")]
    organization: Option<String>,

    /// The project ID to use
    #[arg(long, env = "TENSORLAKE_PROJECT_ID")]
    project: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(clap::ValueEnum, Clone)]
enum OutputFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Commands {
    /// Print the CLI version
    Version,

    /// Login to TensorLake
    Login,

    /// Print authentication status
    #[command(alias = "info")]
    Whoami {
        /// Output format
        #[arg(short, long, default_value = "text", value_enum)]
        output: OutputFormat,
    },

    /// Initialize TensorLake configuration for this project
    Init {
        /// Project directory (default: auto-detect)
        #[arg(short, long)]
        directory: Option<String>,

        /// Skip confirmation of detected project directory
        #[arg(short = 'y', long)]
        no_confirm: bool,
    },

    /// Create a new Tensorlake application
    #[command(hide = true)]
    New {
        /// Application name
        name: String,

        /// Overwrite existing files
        #[arg(short, long)]
        force: bool,
    },

    /// Deploy applications to Tensorlake Cloud
    #[command(hide = true)]
    Deploy {
        /// Arguments passed to the deploy Python module (use --build-env KEY=VALUE to inject ENV directives into generated Dockerfiles)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Build Docker images for applications defined in an application file
    #[clap(hide = true)]
    BuildImages {
        /// Path to the application .py file
        application_file_path: String,

        /// Remote registry to push images to (e.g. ghcr.io/myorg, 123456789012.dkr.ecr.region.amazonaws.com)
        #[arg(short, long)]
        repository: Option<String>,

        /// Tag to use for built images (overrides the tag defined in the image)
        #[arg(short, long)]
        tag: Option<String>,

        /// Build only the image with this name
        #[arg(short = 'i', long)]
        image_name: Option<String>,

        /// Name for the build stage added to the FROM directive as AS <stage> (default: tensorlake-image)
        #[arg(short, long, default_value = "tensorlake-image")]
        stage: String,

        /// Path to a MiniJinja template file; the variable `tensorlake_image` is set to the generated Dockerfile
        #[arg(short = 'T', long)]
        template: Option<String>,

        /// Push built images to the registry after building
        #[arg(short, long)]
        push: bool,

        /// Environment variable to inject into the generated Dockerfile as an ENV directive (KEY=VALUE, repeatable)
        #[arg(short = 'e', long = "build-env", value_name = "KEY=VALUE")]
        build_envs: Vec<String>,
    },

    /// Parse a document and print markdown
    #[command(hide = true)]
    Parse {
        /// Local file path or HTTP/HTTPS URL
        path_or_url: String,

        /// Pages to parse, e.g. '1', '1-5', or '1,2,10'. Default: all pages.
        #[arg(short, long)]
        pages: Option<String>,

        /// Ignore local cache and re-parse the document
        #[arg(short = 'I', long)]
        ignore_cache: bool,
    },

    /// Manage Orchestrate applications
    #[command(subcommand, name = "app", alias = "apps", alias = "orchestrate")]
    App(AppCommands),

    /// Manage cron schedules for applications
    #[command(hide = true)]
    #[command(subcommand)]
    Cron(CronCommands),

    /// Manage secrets
    #[command(subcommand)]
    Secrets(SecretsCommands),

    /// Manage Tensorlake workspaces (versioned file systems mounted over FUSE)
    #[command(subcommand, name = "fs")]
    Fs(FsCommands),

    /// Manage SSH public keys for sandbox SSH access
    #[command(subcommand, name = "ssh-keys", alias = "ssh-key", hide = true)]
    SshKeys(SshKeysCommands),

    /// Manage Artifact Storage Git repositories
    #[command(subcommand)]
    Git(GitCommands),

    /// List applications
    #[command(hide = true)]
    #[command(name = "ls")]
    Applications(ApplicationsArgs),

    /// Manage sandboxes
    #[command(subcommand)]
    Sbx(SbxCommands),
}

use std::path::PathBuf;

/// `--mode` for `tl fs mount`. Absent means writable, except a workspace already mounted
/// elsewhere defaults to read-only.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum MountWriteMode {
    Ro,
    Rw,
}

#[derive(Subcommand)]
enum FsCommands {
    /// Install, enable, and diagnose the mount prerequisites (macOS FSKit extension; Linux
    /// FUSE). Run it to fix mounts that won't come up, or with --check to only diagnose
    Setup {
        /// App bundle to install: a path to TLFS.app / a .zip, or an https URL. Defaults to
        /// the release asset matching this CLI version (macOS only)
        #[arg(long)]
        from: Option<String>,

        /// Only diagnose and report; change nothing
        #[arg(long)]
        check: bool,
    },

    /// Create and mount a workspace, or mount an existing one (FUSE): reads stream lazily,
    /// writes stay local until snapshotted
    Mount {
        /// `<file-system>[:<ref-or-commit>]` to create a new workspace, or a workspace id
        /// from `tl fs ls` to mount an existing one (resumes at its last snapshot)
        target: String,

        /// Mountpoint directory (created; must be empty)
        path: PathBuf,

        /// Write policy. Defaults to `rw`, but a workspace already mounted elsewhere defaults
        /// to `ro` — pass `--mode rw` to mount it writable anyway. With a
        /// `<file-system>[:<branch>]` target, `ro` is a read-only view that follows the branch
        #[arg(long, value_enum)]
        mode: Option<MountWriteMode>,

        /// Every snapshot automatically publishes to the mounted branch (server-ordered,
        /// one attributed commit per snapshot). Requires `<file-system>:<branch>`
        #[arg(long)]
        shared_rw: bool,

        /// Automatically seal local changes into a snapshot commit every N seconds (async,
        /// in the mount daemon). Local overlay state is kept; `tl fs snapshot` remains the
        /// on-demand seal. Requires a writable mount
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        auto_commit_interval_secs: Option<u64>,

        /// Run the mount daemon in the foreground (debugging)
        #[arg(long)]
        foreground: bool,

        /// Log every VFS operation the mount serves to stderr (macOS; requires --foreground)
        #[arg(long, requires = "foreground")]
        trace_ops: bool,

        /// Daemon log level (off, error, warn, info, debug, trace). Detached daemons log to
        /// `daemon.log` in the mount's state directory; foreground runs log to the terminal.
        #[arg(long, default_value = "info")]
        log_level: String,
    },

    /// List workspaces (all file systems, or one)
    #[command(name = "ls")]
    Ls {
        /// Limit the listing to one file system
        file_system: Option<String>,

        /// Print workspaces as JSON
        #[arg(long)]
        json: bool,
    },

    /// Delete a workspace (the only way a workspace dies)
    #[command(name = "rm")]
    Rm {
        /// Workspace id or unique prefix (see `tl fs ls`)
        workspace_id: String,
    },

    /// (internal) Run a mount daemon for an existing state directory
    #[command(hide = true)]
    Daemon {
        #[arg(long)]
        state_dir: PathBuf,

        /// Log level (off, error, warn, info, debug, trace)
        #[arg(long, default_value = "info")]
        log_level: String,
    },

    /// Seal local changes into a snapshot on the workspace ref (the local overlay is kept)
    Snapshot {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Snapshot message
        #[arg(short, long)]
        message: Option<String>,

        /// After sealing, drop the local overlay so the mount serves the snapshot commit
        /// directly (required before `tl fs sync`). Destructive: also deletes ignored files
        /// under the mount and any writes made while the snapshot was uploading — pause
        /// writers first.
        #[arg(long)]
        clear: bool,
    },

    /// Publish the workspace's snapshot onto a real branch (squash by default)
    #[command(allow_missing_positional = true)]
    Promote {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Target branch
        branch: String,

        /// Land the full checkpoint chain instead of a single squashed commit
        #[arg(long)]
        full_history: bool,

        /// Merge onto a moved target (two-parent merge commit); conflicts are reported
        /// and nothing is published
        #[arg(long, conflicts_with = "full_history")]
        merge: bool,

        /// Commit message for the squashed promote
        #[arg(short, long)]
        message: Option<String>,
    },

    /// Pull the target branch into the workspace (server-side rebase-style merge)
    Sync {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Branch to pull from (default: the branch the workspace was created from)
        #[arg(long)]
        target: Option<String>,

        /// Fail on conflicts instead of materializing diff3 markers into the workspace
        #[arg(long)]
        fail_on_conflict: bool,

        /// Commit message for the sync merge commit
        #[arg(short, long)]
        message: Option<String>,
    },

    /// Show workspace, lease, and local-change status for a mount
    Status {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Print status as JSON
        #[arg(long)]
        json: bool,
    },

    /// Restore a mount's tracked files to a snapshot or commit
    #[command(allow_missing_positional = true)]
    Restore {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Snapshot/commit hex, branch, or ref to restore to
        version: String,
    },

    /// List changed paths: local vs last snapshot, or between two snapshots
    Diff {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Older snapshot/commit (omit both for local vs last snapshot)
        a: Option<String>,

        /// Newer snapshot/commit
        b: Option<String>,
    },

    /// Unmount: detach and forget local state; the workspace stays until deleted
    Unmount {
        /// A mounted directory (default: the mount containing the current directory)
        path: Option<PathBuf>,

        /// Also delete the server-side workspace
        #[arg(long)]
        delete: bool,
    },
}

#[derive(Subcommand)]
enum SecretsCommands {
    /// List all secrets
    Ls,
    /// Set one or more secrets (KEY=VALUE)
    Set {
        /// Path to an env file containing KEY=VALUE entries
        #[arg(long = "env-file", value_name = "PATH")]
        env_file: Option<std::path::PathBuf>,
        /// Secret key-value pairs (KEY=VALUE)
        secrets: Vec<String>,
    },
    /// Remove one or more secrets
    Rm {
        /// Secret names to unset
        #[arg(required = true)]
        secret_names: Vec<String>,
    },
}

#[derive(Subcommand)]
enum SshKeysCommands {
    /// List the SSH public keys registered to your account
    Ls,
    /// Register an SSH public key (path to a `.pub` file or the key body
    /// itself). The fingerprint is computed server-side.
    Add {
        /// Friendly label, e.g. `laptop` or `ci-runner`
        #[arg(short, long)]
        name: String,
        /// Path to an OpenSSH public key file or the literal key body
        public_key: String,
    },
    /// Remove one or more SSH keys (by id or by unique name)
    Rm {
        /// Key ids (`ssh_key_…`) or unique names to remove
        #[arg(required = true)]
        keys: Vec<String>,
    },
}

#[derive(Subcommand)]
enum GitCommands {
    /// Fast-clone a repo: install trusted pack artifacts directly, then leave a normal Git checkout
    Clone {
        /// Repo name
        repo: String,
        /// Destination directory (default: derived from the repo name)
        dest: Option<std::path::PathBuf>,
        /// Pack/idx/blob artifact cache directory (default: platform cache dir)
        #[arg(long)]
        cache_dir: Option<std::path::PathBuf>,
        /// Prune old cached artifacts after clone; accepts K/M/G/T suffixes
        #[arg(long, value_parser = commands::git::parse_cache_max_bytes)]
        cache_max_bytes: Option<u64>,
        /// Install objects/refs without checking out the worktree
        #[arg(long)]
        no_checkout: bool,
    },
    /// Create an empty repo
    Create {
        /// Repo name
        repo: String,
        /// Default branch name
        #[arg(long, default_value = "main")]
        default_branch: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Push the current Git worktree to a repo as one commit (resumable chunk upload)
    Push {
        /// Repo name
        repo: String,
        /// Target branch
        #[arg(default_value = "main")]
        branch: String,
        /// Commit message
        #[arg(short, long, default_value = "tl push")]
        message: String,
        /// Force-with-lease: require the branch to currently equal this commit oid
        #[arg(long)]
        expect_oid: Option<String>,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Server-side three-way merge of one branch (or commit) into another
    Merge {
        /// Repo name
        repo: String,
        /// Target branch the merge lands on (ours)
        ours: String,
        /// Branch or commit to merge in (theirs)
        theirs: String,
        /// Report what the merge would do without publishing anything
        #[arg(long)]
        preflight: bool,
        /// Preflight: run the text merges for exact conflict answers instead of potential ones
        #[arg(long)]
        deep: bool,
        /// Land conflicts as diff3 markers plus a structured conflict record instead of failing
        #[arg(long, conflicts_with = "preflight")]
        materialize: bool,
        /// Merge commit message
        #[arg(short, long)]
        message: Option<String>,
        /// Merge base override (commit hex)
        #[arg(long)]
        base: Option<String>,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Show the structured conflict record of a materialized merge commit
    Conflicts {
        /// Repo name
        repo: String,
        /// Merge commit oid (hex)
        commit: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Check a detached commit job's progress (job id is printed when a push detaches)
    CommitStatus {
        /// Repo name
        repo: String,
        /// Commit job id
        job_id: String,
    },
    /// List repos in the current project
    #[command(alias = "list")]
    Ls {
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Delete a repo
    #[command(alias = "delete")]
    Rm {
        /// Repo name
        repo: String,
    },
    /// Fork a repo
    Fork {
        /// New repo name
        repo: String,
        /// Base repo name
        base_repo: String,
    },
    /// Archive a repo so it rejects pushes but still allows reads
    Archive {
        /// Repo name
        repo: String,
    },
    /// Restore an archived repo
    Restore {
        /// Repo name
        repo: String,
    },
    /// Mint a short-lived Git credential for this project
    Token {
        /// Limit the credential to one repository and grant only Git read/write scopes;
        /// omit for a project-wide credential
        repo: Option<String>,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Configure plain `git` for this worktree: add a remote and register automatic auth
    Setup {
        /// Repo name (default: the worktree directory name)
        repo: Option<String>,
        /// Name of the git remote to add or update
        #[arg(long, default_value = "tl")]
        remote: String,
        /// Create the repo if it does not exist
        #[arg(long)]
        create: bool,
    },
    /// Git credential helper (registered by `tl git setup`; speaks git's credential protocol)
    #[command(name = "credential-helper", hide = true)]
    CredentialHelper {
        /// Operation requested by git: get, store, or erase
        operation: String,
    },
    /// Print repo information (remote URL, branches, refs)
    #[command(aliases = ["status", "url"])]
    Info {
        /// Repo name
        repo: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Manage repositories
    #[command(subcommand, hide = true, alias = "repos")]
    Repo(GitRepoCommands),
    /// Manage branches
    #[command(subcommand, alias = "branches")]
    Branch(GitBranchCommands),
    /// Inspect refs
    #[command(subcommand)]
    Ref(GitRefCommands),
    /// Inspect operation history
    #[command(subcommand)]
    Op(GitOpCommands),
    /// List all refs in a repo
    #[command(hide = true)]
    Refs {
        /// Repo name
        repo: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// List operation history using an admin-capable Git credential
    #[command(hide = true)]
    Ops {
        /// Repo name
        repo: String,
        /// Git username for the admin-capable credential
        #[arg(long, default_value = "t")]
        git_username: String,
        /// Git token/password for the admin-capable credential
        #[arg(long, env = "TENSORLAKE_GIT_TOKEN")]
        git_token: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum GitRepoCommands {
    /// Create an empty repo
    Create {
        /// Repo name
        repo: String,
        /// Default branch name
        #[arg(long, default_value = "main")]
        default_branch: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// List repos in the current project
    #[command(name = "ls", alias = "list")]
    Ls {
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Delete a repo
    #[command(name = "rm", alias = "delete")]
    Rm {
        /// Repo name
        repo: String,
    },
    /// Fork a repo
    Fork {
        /// New repo name
        repo: String,
        /// Base repo name
        base_repo: String,
    },
    /// Archive a repo so it rejects pushes but still allows reads
    Archive {
        /// Repo name
        repo: String,
    },
    /// Restore an archived repo
    Restore {
        /// Repo name
        repo: String,
    },
}

#[derive(Subcommand)]
enum GitBranchCommands {
    /// List branches in a repo
    #[command(name = "ls", alias = "list")]
    Ls {
        /// Repo name
        repo: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
    /// Delete a branch in a repo
    #[command(name = "rm", alias = "delete")]
    Rm {
        /// Repo name
        repo: String,
        /// Branch name
        branch: String,
    },
}

#[derive(Subcommand)]
enum GitRefCommands {
    /// List all refs in a repo
    #[command(name = "ls")]
    Ls {
        /// Repo name
        repo: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum GitOpCommands {
    /// List operation history using an admin-capable Git credential
    #[command(name = "ls")]
    Ls {
        /// Repo name
        repo: String,
        /// Git username for the admin-capable credential
        #[arg(long, default_value = "t")]
        git_username: String,
        /// Git token/password for the admin-capable credential
        #[arg(long, env = "TENSORLAKE_GIT_TOKEN")]
        git_token: String,
        /// Output JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Parser)]
struct SshArgs {
    #[command(subcommand)]
    command: Option<SshCommands>,

    /// Sandbox ID or name
    sandbox_id: Option<String>,

    /// Shell to use
    #[arg(short, long, default_value = "/bin/bash", requires = "sandbox_id")]
    shell: String,

    /// Shell argument (repeatable)
    #[arg(
        long = "shell-arg",
        allow_hyphen_values = true,
        requires = "sandbox_id"
    )]
    shell_args: Vec<String>,

    /// Working directory
    #[arg(short, long, requires = "sandbox_id")]
    workdir: Option<String>,

    /// Environment variable (KEY=VALUE)
    #[arg(short, long, requires = "sandbox_id")]
    env: Vec<String>,
}

#[derive(Subcommand)]
enum SshCommands {
    /// Manage native SSH public keys
    #[command(subcommand)]
    Keys(SshKeysCommands),
}

#[derive(Subcommand)]
enum CronCommands {
    /// Create a cron schedule for an application
    Create {
        /// Application name
        application: String,

        /// Cron expression (5-field, minimum 60-second interval, e.g. "0 * * * *")
        #[arg(short, long)]
        schedule: String,

        /// Inline JSON to send as input on every invocation
        #[arg(short = 'j', long, conflicts_with = "input_file")]
        input_json: Option<String>,

        /// Path to a file whose bytes are sent as input on every invocation
        #[arg(short = 'f', long, conflicts_with = "input_json")]
        input_file: Option<String>,
    },

    /// List cron schedules for an application
    #[command(name = "ls")]
    List {
        /// Application name
        application: String,
    },

    /// Delete a cron schedule
    #[command(name = "rm")]
    Delete {
        /// Application name
        application: String,

        /// Schedule ID to delete
        schedule_id: String,
    },
}

#[derive(Subcommand)]
enum AppCommands {
    /// Create a new Tensorlake application
    New {
        /// Application name
        name: String,

        /// Overwrite existing files
        #[arg(short, long)]
        force: bool,
    },

    /// Deploy applications to Tensorlake Cloud
    Deploy {
        /// Arguments passed to the deploy Python module (use --build-env KEY=VALUE to inject ENV directives into generated Dockerfiles)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Manage cron schedules for applications
    #[command(subcommand)]
    Cron(CronCommands),

    /// List applications
    #[command(name = "ls")]
    Applications(ApplicationsArgs),
}

#[derive(Parser)]
struct ApplicationsArgs {
    #[command(subcommand)]
    command: Option<ApplicationsCommands>,
}

#[derive(Subcommand)]
enum ApplicationsCommands {
    /// List all applications
    Ls,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum GpuModelArg {
    #[value(name = "A10")]
    A10,
}

impl GpuModelArg {
    fn as_wire_value(self) -> &'static str {
        match self {
            Self::A10 => "A10",
        }
    }
}

#[derive(Subcommand)]
enum SbxCommands {
    /// List all sandboxes
    Ls {
        /// Include sandboxes with status `terminated`
        #[arg(short, long, conflicts_with_all = ["running", "suspended", "archived"])]
        all: bool,

        /// Show only sandboxes with status `running`
        #[arg(short, long, conflicts_with_all = ["all", "suspended", "archived"])]
        running: bool,

        /// Show only sandboxes with status `suspended`
        #[arg(short, long, conflicts_with_all = ["all", "running", "archived"])]
        suspended: bool,

        /// Only print sandbox IDs, one per line (no table formatting)
        #[arg(short, long)]
        quiet: bool,

        /// List archived (terminated) sandboxes from the server's archive
        /// store instead of the live sandbox list.
        #[arg(short = 't', long = "archived", conflicts_with_all = ["all", "running", "suspended"])]
        archived: bool,
    },

    /// Show detailed information for a sandbox
    #[command(alias = "info")]
    Describe {
        /// Sandbox ID or name
        sandbox_id: String,
    },

    /// Terminate one or more sandboxes
    #[command(name = "terminate", alias = "stop")]
    Terminate {
        /// Sandbox IDs or names
        #[arg(required = true)]
        sandbox_ids: Vec<String>,
    },

    /// Create a new sandbox
    #[command(alias = "new")]
    Create {
        /// Optional name for the sandbox. Named sandboxes support suspend/resume.
        /// Omit to create an ephemeral sandbox (no suspend/resume). When provided, must start
        /// with a lowercase letter, contain only lowercase letters, digits, and hyphens, not end
        /// with a hyphen, max 63 chars. Names that are exactly 21 lowercase alphanumeric
        /// characters are rejected (ambiguous with sandbox IDs).
        name: Option<String>,

        /// Number of CPUs (default: 1.0 for new sandboxes, inherited for snapshot restores)
        #[arg(short, long)]
        cpus: Option<f64>,

        /// Memory in MB (default: 1024 for new sandboxes, inherited for snapshot restores)
        #[arg(short, long)]
        memory: Option<i64>,

        /// Root disk size in MB (default: 10240 for new sandboxes)
        #[arg(long = "disk_mb")]
        disk_mb: Option<u64>,

        /// Number of GPUs to request
        #[arg(long = "gpus", hide = true, value_parser = clap::value_parser!(u32).range(1..))]
        gpus: Option<u32>,

        /// GPU model to request
        #[arg(long = "gpu-model", value_enum, hide = true, requires = "gpus")]
        gpu_model: Option<GpuModelArg>,

        /// Deprecated: root disk size in GB
        #[arg(long = "disk", hide = true, conflicts_with = "disk_mb")]
        disk_gb: Option<u64>,

        /// Timeout in seconds
        #[arg(short, long)]
        timeout: Option<i64>,

        /// Entrypoint command parts
        #[arg(short, long)]
        entrypoint: Vec<String>,

        /// Create from a snapshot ID
        #[arg(short, long, conflicts_with = "image")]
        snapshot: Option<String>,

        /// Create from a registered image name
        #[arg(short, long, conflicts_with = "snapshot")]
        image: Option<String>,

        /// Return immediately after creation instead of waiting for the sandbox to be running
        #[arg(short, long)]
        no_wait: bool,

        /// Expose a port via the sandbox proxy (can be repeated)
        #[arg(short = 'x', long = "expose", value_parser = parse_user_port)]
        ports: Vec<u16>,

        /// Allow unauthenticated proxy access to this sandbox
        #[arg(long, hide = true)]
        allow_unauthenticated_access: bool,

        /// Block all outbound internet access
        #[arg(short = 'N', long)]
        no_internet: bool,

        /// Allow outbound traffic to this IP or CIDR (can be repeated)
        #[arg(short = 'A', long = "network-allow")]
        network_allow: Vec<String>,

        /// Deny outbound traffic to this IP or CIDR (can be repeated)
        #[arg(short = 'D', long = "network-deny")]
        network_deny: Vec<String>,

        /// Mount a registered file system at boot as
        /// `<file_system_id>:<mount_path>` (can be repeated)
        #[arg(short = 'f', long = "filesystem", value_name = "ID:PATH")]
        file_systems: Vec<String>,
    },

    /// Suspend a running sandbox
    Suspend {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Return immediately after sending the suspend request instead of waiting for the sandbox to be suspended
        #[arg(short, long)]
        no_wait: bool,
    },

    /// Resume a suspended sandbox
    Resume {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Return immediately after sending the resume request instead of waiting for the sandbox to be running
        #[arg(short, long)]
        no_wait: bool,
    },

    /// Execute a command in a sandbox
    Exec {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Command to execute
        command: String,

        /// Command arguments
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,

        /// Timeout in seconds
        #[arg(short, long)]
        timeout: Option<f64>,

        /// Working directory
        #[arg(short, long)]
        workdir: Option<String>,

        /// Environment variable (KEY=VALUE)
        #[arg(short, long)]
        env: Vec<String>,

        /// Process user to run as: username, uid, or uid:gid (default: the
        /// image's configured user)
        #[arg(long)]
        user: Option<String>,

        /// Start the process and return immediately instead of streaming output
        #[arg(long)]
        detach: bool,

        /// Managed-process name. Requires --detach.
        #[arg(long)]
        name: Option<String>,

        /// Managed-process restart policy. Requires --detach.
        #[arg(long, value_enum)]
        restart: Option<RestartPolicyArg>,

        /// Maximum number of restarts for a managed process. Requires --detach.
        #[arg(long)]
        max_restarts: Option<u32>,

        /// Initial restart backoff in milliseconds. Requires --detach.
        #[arg(long)]
        initial_backoff_ms: Option<u64>,

        /// Maximum restart backoff in milliseconds. Requires --detach.
        #[arg(long)]
        max_backoff_ms: Option<u64>,

        /// HTTP health check as PORT or PORT:/path. Requires --detach.
        #[arg(long)]
        health_http: Option<String>,

        /// TCP health check port. Requires --detach.
        #[arg(long, value_parser = parse_tcp_port)]
        health_tcp: Option<u16>,

        /// Delay before the first health check in milliseconds. Requires --detach.
        #[arg(long)]
        health_initial_delay_ms: Option<u64>,

        /// Health check interval in milliseconds. Requires --detach.
        #[arg(long)]
        health_interval_ms: Option<u64>,

        /// Per-check health timeout in milliseconds. Requires --detach.
        #[arg(long)]
        health_timeout_ms: Option<u64>,

        /// Consecutive health failures before restart. Requires --detach.
        #[arg(long)]
        health_failure_threshold: Option<u32>,
    },

    /// List or inspect sandbox processes
    Ps {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Optional process to inspect: PID or process name given on creation
        process: Option<String>,

        /// Print JSON
        #[arg(long)]
        json: bool,
    },

    /// Read persisted sandbox logs
    Logs(LogsCliArgs),

    /// Restart a managed sandbox process
    Restart {
        /// Sandbox ID or name
        sandbox_id: String,

        /// PID or process name given on creation
        process: String,
    },

    /// Kill a sandbox process (or stop a managed process)
    Kill {
        /// Sandbox ID or name
        sandbox_id: String,

        /// PID or process name given on creation
        process: String,
    },

    /// Copy files between local and sandbox
    Cp {
        /// Source path (identifier:/path or local path)
        src: String,

        /// Destination path (identifier:/path or local path)
        dest: String,
    },

    /// Copy a running sandbox using the server live-copy API
    Copy {
        /// Source sandbox ID or name
        sandbox_id: String,

        /// Number of running sandbox copies to create
        #[arg(short = 'n', long, default_value = "1")]
        times: NonZeroUsize,

        /// Max seconds to wait for copied sandboxes to become ready
        #[arg(long)]
        timeout: Option<f64>,
    },

    /// Create a checkpoint (snapshot) or list checkpoints
    #[command(alias = "snapshot")]
    Checkpoint(SnapshotArgs),

    /// Manage user-exposed sandbox ports
    #[command(subcommand)]
    Port(PortCommands),

    /// Create a sandbox, run a command, and stream output
    Run {
        /// Command to execute
        command: String,

        /// Command arguments
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,

        /// Container image
        #[arg(short, long)]
        image: Option<String>,

        /// Number of CPUs
        #[arg(short, long, default_value = "1.0")]
        cpus: f64,

        /// Memory in MB
        #[arg(short, long, default_value = "1024")]
        memory: i64,

        /// Root disk size in MB (default: 10240 for new sandboxes)
        #[arg(long = "disk_mb")]
        disk_mb: Option<u64>,

        /// Command timeout in seconds
        #[arg(short, long)]
        timeout: Option<f64>,

        /// Working directory
        #[arg(short, long)]
        workdir: Option<String>,

        /// Environment variable (KEY=VALUE)
        #[arg(short, long)]
        env: Vec<String>,

        /// Process user to run as: username, uid, or uid:gid (default: the
        /// image's configured user)
        #[arg(long)]
        user: Option<String>,

        /// Keep sandbox after command exits
        #[arg(short, long)]
        keep: bool,

        /// Expose a port via the sandbox proxy (can be repeated)
        #[arg(short = 'x', long = "expose", value_parser = parse_user_port)]
        ports: Vec<u16>,

        /// Allow unauthenticated proxy access to this sandbox
        #[arg(long, hide = true)]
        allow_unauthenticated_access: bool,

        /// Block all outbound internet access
        #[arg(short = 'N', long)]
        no_internet: bool,

        /// Allow outbound traffic to this IP or CIDR (can be repeated)
        #[arg(short = 'A', long = "network-allow")]
        network_allow: Vec<String>,

        /// Deny outbound traffic to this IP or CIDR (can be repeated)
        #[arg(short = 'D', long = "network-deny")]
        network_deny: Vec<String>,
    },

    /// Set or update the name of a sandbox
    Name {
        /// Sandbox ID or current name
        sandbox_id: String,

        /// New name to assign. Rules: start with a lowercase letter, contain only lowercase
        /// letters, digits, and hyphens, not end with a hyphen, max 63 chars. Names that are
        /// exactly 21 lowercase alphanumeric characters are rejected (ambiguous with sandbox IDs).
        new_name: String,
    },

    /// Interactive shell in a sandbox, or manage native SSH keys
    Ssh(SshArgs),

    /// Manage PTY sessions for a sandbox
    #[command(subcommand)]
    Pty(PtyCommands),

    /// Tunnel a local TCP port into a sandbox over WebSocket
    Tunnel {
        /// Sandbox ID
        sandbox_id: String,

        /// Remote port inside the sandbox
        #[arg(value_parser = parse_tcp_port)]
        remote_port: u16,

        /// Local port to listen on (defaults to the remote port)
        #[arg(short, long, value_parser = parse_tcp_port)]
        listen_port: Option<u16>,
    },

    /// Manage sandbox images
    #[command(subcommand)]
    Image(ImageCommands),

    /// Attach, detach, or list file systems on a sandbox
    #[command(subcommand, name = "fs")]
    Fs(SbxFsCommands),
}

#[derive(Subcommand)]
enum SbxFsCommands {
    /// Attach a registered file system to a running sandbox
    Attach {
        /// Sandbox ID or name
        sandbox_id: String,

        /// File system id to attach (e.g. `file_system_...`)
        #[arg(short, long = "id")]
        file_system_id: String,

        /// Absolute guest mount path (e.g. `/mnt/skills`)
        #[arg(short, long)]
        path: String,

        /// Print the updated sandbox as JSON
        #[arg(long)]
        json: bool,
    },

    /// Detach the file system mounted at a path from a running sandbox
    Detach {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Absolute guest mount path to detach
        #[arg(short, long)]
        path: String,

        /// Print the updated sandbox as JSON
        #[arg(long)]
        json: bool,
    },

    /// List file systems currently mounted on a sandbox
    #[command(name = "ls")]
    Ls {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Print mounts as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ImageCommands {
    /// Register a custom snapshot-backed sandbox image name
    Register {
        /// Image name to register
        image_name: String,

        /// Completed snapshot ID backing this image
        snapshot_id: String,

        /// Dockerfile to store in the platform sandbox template registry
        #[arg(long = "dockerfile", value_name = "PATH")]
        dockerfile_path: String,

        /// Whether the registered image should be public
        #[arg(long)]
        public: bool,
    },

    /// Register a sandbox image from a Dockerfile
    Create {
        /// Path to the Dockerfile
        dockerfile_path: String,

        /// Registered image name (defaults to the Dockerfile stem, or the parent directory name
        /// when the file is named Dockerfile)
        #[arg(short = 'n', long)]
        registered_name: Option<String>,

        /// Root disk size in MB for the generated sandbox image (default: 10240)
        #[arg(long = "disk_mb")]
        disk_mb: Option<u64>,

        /// Root disk size in MB for the temporary builder sandbox
        #[arg(long = "builder_disk_mb")]
        builder_disk_mb: Option<u64>,

        /// Deprecated: root disk size in GB for the generated sandbox image
        #[arg(long = "disk", hide = true, conflicts_with = "disk_mb")]
        disk_gb: Option<u64>,

        /// CPUs for the temporary build sandbox
        #[arg(long)]
        cpus: Option<f64>,

        /// Memory in MB for the temporary build sandbox
        #[arg(long)]
        memory: Option<i64>,

        /// Make this sandbox image publicly accessible
        #[arg(short, long)]
        public: bool,

        /// Use Docker/BuildKit max compatibility mode (build is slower and uses more memory and disk space on builder sandbox)
        #[arg(long = "docker_compat")]
        docker_compat: bool,

        /// Build a content-addressed streaming image (non-default). Streaming
        /// images cold-boot by faulting content on demand instead of
        /// localizing a monolithic snapshot; the FROM image must be an
        /// unregistered OCI image (base builds only).
        #[arg(long, hide = true)]
        streaming: bool,

        /// Print the registered sandbox image JSON response to stdout
        #[arg(long = "json", hide = true)]
        json: bool,
    },

    /// Import a registry image directly into a sandbox image (no Dockerfile,
    /// no Docker daemon — the image's layers are written straight into the
    /// rootfs)
    Import {
        /// Registry image reference to import (e.g. ubuntu:24.04,
        /// pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime, ghcr.io/org/app:v1)
        image_reference: String,

        /// Registered image name (defaults to the image's last path segment)
        #[arg(short = 'n', long)]
        registered_name: Option<String>,

        /// Root disk size in MB for the generated sandbox image (default: 10240)
        #[arg(long = "disk_mb")]
        disk_mb: Option<u64>,

        /// Root disk size in MB for the temporary builder sandbox
        #[arg(long = "builder_disk_mb")]
        builder_disk_mb: Option<u64>,

        /// CPUs for the temporary build sandbox
        #[arg(long)]
        cpus: Option<f64>,

        /// Memory in MB for the temporary build sandbox
        #[arg(long)]
        memory: Option<i64>,

        /// Make this sandbox image publicly accessible
        #[arg(short, long)]
        public: bool,

        /// Use Docker/BuildKit max compatibility mode (import is slower and uses more memory and disk space on builder sandbox)
        #[arg(long = "docker_compat")]
        docker_compat: bool,

        /// Print the registered sandbox image JSON response to stdout
        #[arg(long = "json")]
        json: bool,
    },

    /// List all sandbox images
    Ls {
        /// Print the sandbox image list as JSON to stdout
        #[arg(long = "json")]
        json: bool,
    },

    /// Show details for a sandbox image
    Describe {
        /// Image name or ID
        name_or_id: String,
    },

    /// Delete a sandbox image
    #[command(alias = "delete")]
    Rm {
        /// Image name or ID
        name_or_id: String,
    },
}

#[derive(Parser)]
struct SnapshotArgs {
    #[command(subcommand)]
    command: Option<SnapshotCommands>,

    /// Sandbox ID or name
    sandbox_id: Option<String>,

    /// Max seconds to wait
    #[arg(short, long, default_value = "300", requires = "sandbox_id")]
    timeout: f64,

    /// Optional checkpoint type. When omitted, the client sends no `snapshot_type` and the server applies its default (currently `filesystem`). `memory` captures VM memory + filesystem state, `filesystem` captures filesystem only.
    #[arg(long, value_enum, requires = "sandbox_id")]
    checkpoint_type: Option<SnapshotTypeArg>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotTypeArg {
    Memory,
    Filesystem,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum RestartPolicyArg {
    Never,
    OnFailure,
    Always,
}

#[derive(Args, Clone, Debug)]
struct LogsCliArgs {
    #[command(subcommand)]
    command: Option<LogsCommands>,

    /// Sandbox ID or name
    sandbox_id: Option<String>,

    /// Filter by log level
    #[arg(long, value_enum)]
    level: Vec<SandboxLogLevelArg>,

    /// Filter by stable process ID from `sbx logs streams`
    #[arg(long = "process-id")]
    process_id: Vec<String>,

    /// Pagination token returned by a previous response
    #[arg(long)]
    next_token: Option<String>,

    /// Read the oldest N logs
    #[arg(long, conflicts_with = "tail")]
    head: Option<usize>,

    /// Read the newest N logs
    #[arg(long)]
    tail: Option<usize>,

    /// Case-insensitive body text search
    #[arg(long)]
    body: Option<String>,

    /// Print JSON
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand, Clone, Debug, PartialEq, Eq)]
enum LogsCommands {
    /// List streams available for persisted sandbox log filtering
    Streams {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Print JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum SandboxLogLevelArg {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl SandboxLogLevelArg {
    fn as_query_value(self) -> i8 {
        match self {
            SandboxLogLevelArg::Trace => 1,
            SandboxLogLevelArg::Debug => 2,
            SandboxLogLevelArg::Info => 3,
            SandboxLogLevelArg::Warn => 4,
            SandboxLogLevelArg::Error => 5,
            SandboxLogLevelArg::Fatal => 6,
        }
    }
}

impl RestartPolicyArg {
    fn as_wire_value(self) -> &'static str {
        match self {
            RestartPolicyArg::Never => "never",
            RestartPolicyArg::OnFailure => "on_failure",
            RestartPolicyArg::Always => "always",
        }
    }
}

impl SnapshotTypeArg {
    fn as_wire_value(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Filesystem => "filesystem",
        }
    }
}

#[derive(Subcommand)]
enum SnapshotCommands {
    /// List all snapshots
    Ls,

    /// Delete one or more snapshots
    Rm {
        /// Snapshot IDs
        #[arg(required = true)]
        snapshot_ids: Vec<String>,
    },
}

#[derive(Subcommand)]
enum PortCommands {
    /// List user-exposed ports for a sandbox
    Ls {
        /// Sandbox ID or name
        sandbox_id: String,
    },

    /// Expose one or more ports and enable unauthenticated access
    Expose {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Ports to expose
        #[arg(required = true, value_parser = parse_user_port)]
        ports: Vec<u16>,
    },

    /// Remove one or more exposed ports
    Rm {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Ports to remove
        #[arg(required = true, value_parser = parse_user_port)]
        ports: Vec<u16>,
    },
}

#[derive(Subcommand)]
enum PtyCommands {
    /// List PTY sessions for a sandbox
    #[command(name = "ls")]
    Ls {
        /// Sandbox ID or name
        sandbox_id: String,

        /// Emit JSON suitable for scripting. Includes the full token.
        #[arg(long = "json")]
        output_json: bool,
    },

    /// Attach to an existing PTY session
    Attach {
        /// Sandbox ID or name
        sandbox_id: String,

        /// PTY session ID
        session_id: String,

        /// PTY token returned when the session was created
        #[arg(long)]
        token: String,
    },

    /// Remove one or more PTY sessions
    #[command(name = "rm")]
    Rm {
        /// Sandbox ID or name
        sandbox_id: String,

        /// PTY session IDs
        #[arg(required = true)]
        session_ids: Vec<String>,
    },
}

fn parse_user_port(value: &str) -> std::result::Result<u16, String> {
    let port = parse_tcp_port(value)?;

    if port == 9501 {
        return Err("port 9501 is reserved for sandbox management".to_string());
    }

    Ok(port)
}

fn parse_tcp_port(value: &str) -> std::result::Result<u16, String> {
    let port: u16 = value
        .parse()
        .map_err(|_| format!("invalid port '{value}'"))?;

    if port == 0 {
        return Err("port must be between 1 and 65535".to_string());
    }

    Ok(port)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let resolved = resolver::resolve(
        cli.api_url.as_deref(),
        cli.cloud_url.as_deref(),
        cli.api_key.as_deref(),
        cli.personal_access_token.as_deref(),
        cli.namespace.as_deref(),
        cli.organization.as_deref(),
        cli.project.as_deref(),
        cli.debug,
    );

    let mut ctx = CliContext::from_resolved(resolved);

    let command = match cli.command {
        Some(command) => command,
        None => {
            eprintln!("{}", missing_subcommand_error());
            std::process::exit(2);
        }
    };

    let result = run_command(&mut ctx, command).await;

    if let Err(e) = result {
        match &e {
            CliError::ExitCode(code) => std::process::exit(*code),
            CliError::Cancelled => std::process::exit(1),
            _ => {
                eprintln!("Error: {}", e);
                // Walk the source chain: wrapped errors like reqwest's hide the
                // root cause (DNS failure, connection refused, TLS, timeout)
                // behind Display and only expose it via source().
                let mut prev = e.to_string();
                let mut source = std::error::Error::source(&e);
                while let Some(cause) = source {
                    let msg = cause.to_string();
                    if !prev.contains(&msg) {
                        eprintln!("  Caused by: {}", msg);
                        prev = msg;
                    }
                    source = cause.source();
                }
                if ctx.debug {
                    eprintln!("\nDebug info:");
                    eprintln!("  {:?}", e);
                }
                std::process::exit(1);
            }
        }
    }
}

fn missing_subcommand_error() -> &'static str {
    "error: 'tl' requires a subcommand but one was not provided\n\nUsage: tl [OPTIONS] <COMMAND>\n\nFor more information, try '--help'."
}

async fn run_command(ctx: &mut CliContext, command: Commands) -> error::Result<()> {
    match command {
        Commands::Version => {
            println!("tl {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Commands::Login => commands::login::run(ctx).await,
        Commands::Whoami { output } => {
            commands::whoami::run(ctx, matches!(output, OutputFormat::Json)).await
        }
        Commands::Init {
            directory,
            no_confirm,
        } => commands::init::run(ctx, directory.as_deref(), no_confirm).await,
        Commands::New { name, force } => commands::new::run(&name, force),
        Commands::Deploy { args } => run_deploy_command(ctx, &args).await,
        Commands::BuildImages {
            application_file_path,
            repository,
            tag,
            image_name,
            stage,
            template,
            push,
            build_envs,
        } => {
            commands::build_images::run(commands::build_images::BuildImageArgs {
                application_file_path: &application_file_path,
                repository: repository.as_deref(),
                tag: tag.as_deref(),
                image_name: image_name.as_deref(),
                stage: &stage,
                template: template.as_deref(),
                push,
                build_envs: &build_envs,
            })
            .await
        }
        Commands::Parse {
            path_or_url,
            pages,
            ignore_cache,
        } => commands::parse::run(ctx, &path_or_url, pages.as_deref(), ignore_cache).await,
        Commands::App(subcmd) => run_app_command(ctx, subcmd).await,
        Commands::Cron(subcmd) => run_cron_command(ctx, subcmd).await,
        Commands::Secrets(subcmd) => {
            ensure_auth_and_project(ctx).await?;
            match subcmd {
                SecretsCommands::Ls => commands::secrets::list(ctx).await,
                SecretsCommands::Set { env_file, secrets } => {
                    commands::secrets::set(ctx, env_file.as_deref(), &secrets).await
                }
                SecretsCommands::Rm { secret_names } => {
                    commands::secrets::unset(ctx, &secret_names).await
                }
            }
        }
        Commands::Fs(subcmd) => run_fs_command(ctx, subcmd).await,
        Commands::SshKeys(subcmd) => {
            // SSH keys live on the user, not on a project — only auth (PAT or
            // logged-in session) is required, no org/project context.
            ensure_auth(ctx).await?;
            run_ssh_keys_command(ctx, subcmd).await
        }
        Commands::Git(subcmd) => {
            // The credential helper is invoked by git itself, often non-interactively; it must
            // never start a login/init flow. It degrades softly (prints nothing, so git falls
            // through to prompting) when auth or project context is missing.
            if !matches!(subcmd, GitCommands::CredentialHelper { .. }) {
                ensure_auth_and_project(ctx).await?;
            }
            run_git_command(ctx, subcmd).await
        }
        Commands::Applications(app_args) => run_applications_command(ctx, app_args).await,
        Commands::Sbx(subcmd) => match subcmd {
            SbxCommands::Ssh(ssh_args)
                if matches!(ssh_args.command, Some(SshCommands::Keys(_))) =>
            {
                ensure_auth(ctx).await?;
                match ssh_args.command {
                    Some(SshCommands::Keys(keys_cmd)) => run_ssh_keys_command(ctx, keys_cmd).await,
                    None => unreachable!(),
                }
            }
            other => {
                ensure_auth_and_project(ctx).await?;
                match other {
                    SbxCommands::Ls {
                        all,
                        running,
                        suspended,
                        quiet,
                        archived,
                    } => {
                        commands::sbx::ls::run(ctx, running, suspended, all, quiet, archived).await
                    }
                    SbxCommands::Describe { sandbox_id } => {
                        commands::sbx::describe::run(ctx, &sandbox_id).await
                    }
                    SbxCommands::Terminate { sandbox_ids } => {
                        commands::sbx::terminate::run(ctx, &sandbox_ids).await
                    }
                    SbxCommands::Create {
                        name,
                        cpus,
                        memory,
                        disk_mb,
                        gpus,
                        gpu_model,
                        disk_gb,
                        timeout,
                        entrypoint,
                        snapshot,
                        image,
                        no_wait,
                        ports,
                        allow_unauthenticated_access,
                        no_internet,
                        network_allow,
                        network_deny,
                        file_systems,
                    } => {
                        let disk_mb = if let Some(value) = disk_mb {
                            Some(value)
                        } else {
                            disk_gb
                                .map(|value| {
                                    value.checked_mul(1024).ok_or_else(|| {
                                        CliError::usage("--disk is too large to convert to MiB")
                                    })
                                })
                                .transpose()?
                        };
                        commands::sbx::create::run(
                            ctx,
                            commands::sbx::create::CreateArgs {
                                name: name.as_deref(),
                                cpus,
                                memory,
                                disk_mb,
                                gpu_count: gpus,
                                gpu_model: gpu_model.map(GpuModelArg::as_wire_value),
                                timeout,
                                entrypoint: &entrypoint,
                                snapshot_id: snapshot.as_deref(),
                                image_name: image.as_deref(),
                                wait: !no_wait,
                                ports: &ports,
                                allow_unauthenticated_access,
                                no_internet,
                                network_allow: &network_allow,
                                network_deny: &network_deny,
                                file_systems: &file_systems,
                            },
                        )
                        .await
                    }
                    SbxCommands::Name {
                        sandbox_id,
                        new_name,
                    } => commands::sbx::name::run(ctx, &sandbox_id, &new_name).await,
                    SbxCommands::Suspend {
                        sandbox_id,
                        no_wait,
                    } => commands::sbx::suspend::run(ctx, &sandbox_id, !no_wait).await,
                    SbxCommands::Resume {
                        sandbox_id,
                        no_wait,
                    } => commands::sbx::resume::run(ctx, &sandbox_id, !no_wait).await,
                    SbxCommands::Exec {
                        sandbox_id,
                        command,
                        args,
                        timeout,
                        workdir,
                        env,
                        user,
                        detach,
                        name,
                        restart,
                        max_restarts,
                        initial_backoff_ms,
                        max_backoff_ms,
                        health_http,
                        health_tcp,
                        health_initial_delay_ms,
                        health_interval_ms,
                        health_timeout_ms,
                        health_failure_threshold,
                    } => {
                        commands::sbx::exec::run(
                            ctx,
                            &sandbox_id,
                            &command,
                            &args,
                            commands::sbx::exec::ExecOptions {
                                timeout,
                                workdir: workdir.as_deref(),
                                env: &env,
                                user: user.as_deref(),
                                detach,
                                name: name.as_deref(),
                                restart_policy: restart.map(RestartPolicyArg::as_wire_value),
                                max_restarts,
                                initial_backoff_ms,
                                max_backoff_ms,
                                health_http: health_http.as_deref(),
                                health_tcp,
                                health_initial_delay_ms,
                                health_interval_ms,
                                health_timeout_ms,
                                health_failure_threshold,
                            },
                        )
                        .await
                    }
                    SbxCommands::Ps {
                        sandbox_id,
                        process,
                        json,
                    } => {
                        commands::sbx::process::ps(ctx, &sandbox_id, process.as_deref(), json).await
                    }
                    SbxCommands::Logs(args) => match args.command.clone() {
                        Some(LogsCommands::Streams { sandbox_id, json }) => {
                            commands::sbx::process::log_processes(ctx, &sandbox_id, json).await
                        }
                        None => {
                            let sandbox_id = args.sandbox_id.ok_or_else(|| {
                                CliError::Other(anyhow::anyhow!(
                                    "logs requires a sandbox ID or the 'streams' subcommand"
                                ))
                            })?;
                            commands::sbx::process::logs(
                                ctx,
                                &sandbox_id,
                                commands::sbx::process::LogsArgs {
                                    levels: args
                                        .level
                                        .iter()
                                        .map(|level| level.as_query_value())
                                        .collect(),
                                    process_ids: args.process_id,
                                    next_token: args.next_token.as_deref(),
                                    head: args.head,
                                    tail: args.tail,
                                    body: args.body.as_deref(),
                                    json: args.json,
                                },
                            )
                            .await
                        }
                    },
                    SbxCommands::Restart {
                        sandbox_id,
                        process,
                    } => commands::sbx::process::restart(ctx, &sandbox_id, &process).await,
                    SbxCommands::Kill {
                        sandbox_id,
                        process,
                    } => commands::sbx::process::kill(ctx, &sandbox_id, &process).await,
                    SbxCommands::Cp { src, dest } => commands::sbx::cp::run(ctx, &src, &dest).await,
                    SbxCommands::Copy {
                        sandbox_id,
                        times,
                        timeout,
                    } => commands::sbx::copy::run(ctx, &sandbox_id, times.get(), timeout).await,
                    SbxCommands::Checkpoint(snapshot_args) => match snapshot_args.command {
                        Some(SnapshotCommands::Ls) => commands::sbx::snapshot_ls::run(ctx).await,
                        Some(SnapshotCommands::Rm { snapshot_ids }) => {
                            commands::sbx::snapshot_rm::run(ctx, &snapshot_ids).await
                        }
                        None => {
                            let sandbox_id = snapshot_args.sandbox_id.ok_or_else(|| {
                                CliError::usage(
                                    "checkpoint requires a sandbox ID or the 'ls' subcommand",
                                )
                            })?;
                            commands::sbx::snapshot::run(
                                ctx,
                                &sandbox_id,
                                snapshot_args.timeout,
                                snapshot_args
                                    .checkpoint_type
                                    .map(SnapshotTypeArg::as_wire_value),
                            )
                            .await
                        }
                    },
                    SbxCommands::Port(port_cmd) => match port_cmd {
                        PortCommands::Ls { sandbox_id } => {
                            commands::sbx::port::list(ctx, &sandbox_id).await
                        }
                        PortCommands::Expose { sandbox_id, ports } => {
                            commands::sbx::port::expose(ctx, &sandbox_id, &ports).await
                        }
                        PortCommands::Rm { sandbox_id, ports } => {
                            commands::sbx::port::remove(ctx, &sandbox_id, &ports).await
                        }
                    },
                    SbxCommands::Run {
                        command,
                        args,
                        image,
                        cpus,
                        memory,
                        disk_mb,
                        timeout,
                        workdir,
                        env,
                        user,
                        keep,
                        ports,
                        allow_unauthenticated_access,
                        no_internet,
                        network_allow,
                        network_deny,
                    } => {
                        commands::sbx::run::run(
                            ctx,
                            &command,
                            &args,
                            image.as_deref(),
                            cpus,
                            memory,
                            disk_mb,
                            timeout,
                            workdir.as_deref(),
                            &env,
                            user.as_deref(),
                            keep,
                            &ports,
                            allow_unauthenticated_access,
                            no_internet,
                            &network_allow,
                            &network_deny,
                        )
                        .await
                    }
                    SbxCommands::Ssh(ssh_args) => {
                        let sandbox_id = ssh_args.sandbox_id.ok_or_else(|| {
                            CliError::usage(
                                "ssh requires a sandbox ID or name, or the 'keys' subcommand",
                            )
                        })?;
                        commands::sbx::ssh::run(
                            ctx,
                            &sandbox_id,
                            &ssh_args.shell,
                            &ssh_args.shell_args,
                            ssh_args.workdir.as_deref(),
                            &ssh_args.env,
                        )
                        .await
                    }
                    SbxCommands::Pty(pty_cmd) => match pty_cmd {
                        PtyCommands::Ls {
                            sandbox_id,
                            output_json,
                        } => commands::sbx::pty::list(ctx, &sandbox_id, output_json).await,
                        PtyCommands::Attach {
                            sandbox_id,
                            session_id,
                            token,
                        } => {
                            commands::sbx::pty::attach(ctx, &sandbox_id, &session_id, &token).await
                        }
                        PtyCommands::Rm {
                            sandbox_id,
                            session_ids,
                        } => commands::sbx::pty::remove(ctx, &sandbox_id, &session_ids).await,
                    },
                    SbxCommands::Image(image_cmd) => match image_cmd {
                        ImageCommands::Register {
                            image_name,
                            snapshot_id,
                            dockerfile_path,
                            public,
                        } => {
                            commands::sbx::image::register::run(
                                ctx,
                                &image_name,
                                &snapshot_id,
                                &dockerfile_path,
                                public,
                            )
                            .await
                        }
                        ImageCommands::Create {
                            dockerfile_path,
                            registered_name,
                            disk_mb,
                            builder_disk_mb,
                            disk_gb,
                            cpus,
                            memory,
                            public,
                            docker_compat,
                            streaming,
                            json,
                        } => {
                            let disk_mb = if let Some(value) = disk_mb {
                                Some(value)
                            } else {
                                disk_gb
                                    .map(|value| {
                                        value.checked_mul(1024).ok_or_else(|| {
                                            CliError::usage("--disk is too large to convert to MiB")
                                        })
                                    })
                                    .transpose()?
                            };
                            commands::sbx::image::create::run(
                                ctx,
                                &dockerfile_path,
                                registered_name.as_deref(),
                                disk_mb,
                                builder_disk_mb,
                                cpus,
                                memory,
                                public,
                                docker_compat,
                                streaming,
                                json,
                            )
                            .await
                        }
                        ImageCommands::Import {
                            image_reference,
                            registered_name,
                            disk_mb,
                            builder_disk_mb,
                            cpus,
                            memory,
                            public,
                            docker_compat,
                            json,
                        } => {
                            commands::sbx::image::import::run(
                                ctx,
                                &image_reference,
                                registered_name.as_deref(),
                                disk_mb,
                                builder_disk_mb,
                                cpus,
                                memory,
                                public,
                                docker_compat,
                                json,
                            )
                            .await
                        }
                        ImageCommands::Ls { json } => {
                            commands::sbx::image::ls::run(ctx, json).await
                        }
                        ImageCommands::Describe { name_or_id } => {
                            commands::sbx::image::describe::run(ctx, &name_or_id).await
                        }
                        ImageCommands::Rm { name_or_id } => {
                            commands::sbx::image::rm::run(ctx, &name_or_id).await
                        }
                    },
                    SbxCommands::Fs(fs_cmd) => match fs_cmd {
                        SbxFsCommands::Attach {
                            sandbox_id,
                            file_system_id,
                            path,
                            json,
                        } => {
                            commands::sbx::fs::attach(
                                ctx,
                                &sandbox_id,
                                &file_system_id,
                                &path,
                                json,
                            )
                            .await
                        }
                        SbxFsCommands::Detach {
                            sandbox_id,
                            path,
                            json,
                        } => commands::sbx::fs::detach(ctx, &sandbox_id, &path, json).await,
                        SbxFsCommands::Ls { sandbox_id, json } => {
                            commands::sbx::fs::list(ctx, &sandbox_id, json).await
                        }
                    },
                    SbxCommands::Tunnel {
                        sandbox_id,
                        remote_port,
                        listen_port,
                    } => {
                        commands::sbx::tunnel::run(ctx, &sandbox_id, remote_port, listen_port).await
                    }
                }
            }
        },
    }
}

async fn run_app_command(ctx: &mut CliContext, subcmd: AppCommands) -> error::Result<()> {
    match subcmd {
        AppCommands::New { name, force } => commands::new::run(&name, force),
        AppCommands::Deploy { args } => run_deploy_command(ctx, &args).await,
        AppCommands::Cron(subcmd) => run_cron_command(ctx, subcmd).await,
        AppCommands::Applications(app_args) => run_applications_command(ctx, app_args).await,
    }
}

async fn run_deploy_command(ctx: &mut CliContext, args: &[String]) -> error::Result<()> {
    let onprem = std::env::var("TENSORLAKE_ONPREM")
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if !onprem {
        ensure_auth_and_project(ctx).await?;
    }
    commands::deploy::run(ctx, args).await
}

async fn run_cron_command(ctx: &mut CliContext, subcmd: CronCommands) -> error::Result<()> {
    ensure_auth_and_project(ctx).await?;
    match subcmd {
        CronCommands::Create {
            application,
            schedule,
            input_json,
            input_file,
        } => {
            commands::cron::create(
                ctx,
                &application,
                &schedule,
                input_json.as_deref(),
                input_file.as_deref(),
            )
            .await
        }
        CronCommands::List { application } => commands::cron::list(ctx, &application).await,
        CronCommands::Delete {
            application,
            schedule_id,
        } => commands::cron::delete(ctx, &application, &schedule_id).await,
    }
}

async fn run_applications_command(
    ctx: &mut CliContext,
    app_args: ApplicationsArgs,
) -> error::Result<()> {
    ensure_auth_and_project(ctx).await?;
    match app_args.command {
        Some(ApplicationsCommands::Ls) | None => commands::applications::ls(ctx).await,
    }
}

// `tl fs` drives the local FUSE/overlay mount stack, which is backed by the private gsvc-mount
// core and therefore only compiled into `--features mount` release builds. The command surface
// (FsCommands) is always parsed so `tl fs --help` documents it, but a build without the feature
// answers with a clear "not available" error instead of the real implementation.
#[cfg(feature = "mount")]
async fn run_fs_command(ctx: &mut CliContext, subcmd: FsCommands) -> error::Result<()> {
    // Setup installs a local app bundle; it must work before the user has logged in.
    let subcmd = match subcmd {
        FsCommands::Setup { from, check } => {
            return commands::fs::setup(from.as_deref(), check).await;
        }
        other => other,
    };
    // Path-addressed commands default their mounted-directory argument to the mount containing
    // the CWD; resolve it up front so scope hydration and the command agree on the path.
    let mut subcmd = subcmd;
    let mut mount_dir: Option<std::path::PathBuf> = None;
    match &mut subcmd {
        FsCommands::Promote { path, branch, .. } => {
            if path.is_none() {
                // With the path omitted, a sole positional that is itself a mounted
                // directory is a forgotten branch — reject it before it becomes a publish
                // onto a branch named after the directory.
                commands::fs::reject_mount_like_positional(
                    branch,
                    "branch",
                    "tl fs promote [PATH] <BRANCH>",
                )?;
            }
            mount_dir = Some(commands::fs::resolve_mount_path(path.take())?);
        }
        FsCommands::Restore { path, version } => {
            if path.is_none() {
                commands::fs::reject_mount_like_positional(
                    version,
                    "snapshot or ref",
                    "tl fs restore [PATH] <VERSION>",
                )?;
            }
            mount_dir = Some(commands::fs::resolve_mount_path(path.take())?);
        }
        FsCommands::Diff { path, a, b } => {
            let (path, ra, rb) = commands::fs::resolve_diff_args(path.take(), a.take(), b.take())?;
            *a = ra;
            *b = rb;
            mount_dir = Some(path);
        }
        FsCommands::Snapshot { path, .. }
        | FsCommands::Sync { path, .. }
        | FsCommands::Status { path, .. }
        | FsCommands::Unmount { path, .. } => {
            mount_dir = Some(commands::fs::resolve_mount_path(path.take())?);
        }
        _ => {}
    }
    // Path-addressed commands carry their scope in the mount state; resolve it from there so
    // they work from any CWD instead of triggering the interactive init flow (which, run from
    // inside a mount, would write .tensorlake/config.toml into the workspace).
    if let Some(mount_dir) = &mount_dir {
        commands::fs::hydrate_scope_from_mount(ctx, mount_dir);
    }
    ensure_auth_and_project(ctx).await?;
    // Set for exactly the path-addressed commands, whose dispatch arms are the only readers.
    let mount_dir = move || mount_dir.expect("resolved for every path-addressed command above");
    let result = match subcmd {
        FsCommands::Setup { .. } => unreachable!("handled before the auth guard"),
        FsCommands::Ls { file_system, json } => {
            commands::fs::ls(ctx, file_system.as_deref(), json).await
        }
        FsCommands::Rm { workspace_id } => commands::fs::rm(ctx, &workspace_id).await,
        FsCommands::Mount {
            target,
            path,
            mode,
            shared_rw,
            auto_commit_interval_secs,
            foreground,
            trace_ops,
            log_level,
        } => {
            let mode = match mode {
                Some(MountWriteMode::Ro) => commands::fs::WritePolicy::Ro,
                Some(MountWriteMode::Rw) => commands::fs::WritePolicy::Rw,
                None => commands::fs::WritePolicy::Auto,
            };
            commands::fs::mount(
                ctx,
                &target,
                &path,
                mode,
                shared_rw,
                auto_commit_interval_secs,
                foreground,
                trace_ops,
                &log_level,
            )
            .await
        }
        FsCommands::Daemon {
            state_dir,
            log_level,
        } => commands::fs::daemon::run(ctx, &state_dir, &log_level).await,
        FsCommands::Snapshot { message, clear, .. } => {
            commands::fs::snapshot(ctx, &mount_dir(), message.as_deref(), clear).await
        }
        FsCommands::Promote {
            branch,
            full_history,
            merge,
            message,
            ..
        } => {
            commands::fs::promote(
                ctx,
                &mount_dir(),
                &branch,
                full_history,
                merge,
                message.as_deref(),
            )
            .await
        }
        FsCommands::Sync {
            target,
            fail_on_conflict,
            message,
            ..
        } => {
            commands::fs::sync(
                ctx,
                &mount_dir(),
                target.as_deref(),
                fail_on_conflict,
                message.as_deref(),
            )
            .await
        }
        FsCommands::Status { json, .. } => commands::fs::status(ctx, &mount_dir(), json).await,
        FsCommands::Restore { version, .. } => {
            commands::fs::restore(ctx, &mount_dir(), &version).await
        }
        FsCommands::Diff { a, b, .. } => {
            commands::fs::diff(ctx, &mount_dir(), a.as_deref(), b.as_deref()).await
        }
        FsCommands::Unmount { delete, .. } => {
            commands::fs::unmount(ctx, &mount_dir(), delete).await
        }
    };
    // A cached minted git credential can be revoked before its recorded expiry; purge
    // the cache on an auth failure so the next invocation re-mints instead of retrying
    // a dead token.
    if let Err(
        CliError::Auth(_)
        | CliError::Sdk(
            tensorlake::error::SdkError::Authentication(_)
            | tensorlake::error::SdkError::Authorization(_),
        ),
    ) = &result
    {
        crate::config::files::purge_git_credentials();
    }
    result
}

#[cfg(not(feature = "mount"))]
async fn run_fs_command(_ctx: &mut CliContext, _subcmd: FsCommands) -> error::Result<()> {
    Err(CliError::usage(
        "`tl fs` local mounts are not available in this build. Install the official `tl` release, \
         which is compiled with mount support.",
    ))
}

async fn run_ssh_keys_command(ctx: &CliContext, subcmd: SshKeysCommands) -> error::Result<()> {
    match subcmd {
        SshKeysCommands::Ls => commands::ssh_keys::list(ctx).await,
        SshKeysCommands::Add { name, public_key } => {
            commands::ssh_keys::add(ctx, &name, &public_key).await
        }
        SshKeysCommands::Rm { keys } => commands::ssh_keys::remove(ctx, &keys).await,
    }
}

async fn run_git_command(ctx: &CliContext, subcmd: GitCommands) -> error::Result<()> {
    match subcmd {
        GitCommands::Clone {
            repo,
            dest,
            cache_dir,
            cache_max_bytes,
            no_checkout,
        } => {
            commands::git::clone_repo(ctx, &repo, dest, cache_dir, cache_max_bytes, no_checkout)
                .await
        }
        GitCommands::Create {
            repo,
            default_branch,
            json,
        } => commands::git::create_repo(ctx, &repo, Some(&default_branch), json).await,
        GitCommands::Ls { json } => commands::git::list_repos(ctx, json).await,
        GitCommands::Rm { repo } => commands::git::delete_repo(ctx, &repo).await,
        GitCommands::Fork { repo, base_repo } => {
            commands::git::fork_repo(ctx, &repo, &base_repo).await
        }
        GitCommands::Archive { repo } => commands::git::archive_repo(ctx, &repo).await,
        GitCommands::Restore { repo } => commands::git::restore_repo(ctx, &repo).await,
        GitCommands::Token { repo, json } => {
            commands::git::mint_token(ctx, repo.as_deref(), json).await
        }
        GitCommands::Setup {
            repo,
            remote,
            create,
        } => commands::git::setup::run(ctx, repo.as_deref(), &remote, create).await,
        GitCommands::CredentialHelper { operation } => {
            commands::git::setup::credential_helper(ctx, &operation).await
        }
        GitCommands::Push {
            repo,
            branch,
            message,
            expect_oid,
            json,
        } => commands::git::push(ctx, &repo, &branch, &message, expect_oid, json).await,
        GitCommands::Merge {
            repo,
            ours,
            theirs,
            preflight,
            deep,
            materialize,
            message,
            base,
            json,
        } => {
            commands::git::merge(
                ctx,
                &repo,
                &ours,
                &theirs,
                preflight,
                deep,
                materialize,
                message.as_deref(),
                base.as_deref(),
                json,
            )
            .await
        }
        GitCommands::Conflicts { repo, commit, json } => {
            commands::git::commit_conflicts(ctx, &repo, &commit, json).await
        }
        GitCommands::CommitStatus { repo, job_id } => {
            commands::git::commit_status(ctx, &repo, &job_id).await
        }
        GitCommands::Info { repo, json } => commands::git::status(ctx, &repo, json).await,
        GitCommands::Repo(repo_cmd) => match repo_cmd {
            GitRepoCommands::Create {
                repo,
                default_branch,
                json,
            } => commands::git::create_repo(ctx, &repo, Some(&default_branch), json).await,
            GitRepoCommands::Ls { json } => commands::git::list_repos(ctx, json).await,
            GitRepoCommands::Rm { repo } => commands::git::delete_repo(ctx, &repo).await,
            GitRepoCommands::Fork { repo, base_repo } => {
                commands::git::fork_repo(ctx, &repo, &base_repo).await
            }
            GitRepoCommands::Archive { repo } => commands::git::archive_repo(ctx, &repo).await,
            GitRepoCommands::Restore { repo } => commands::git::restore_repo(ctx, &repo).await,
        },
        GitCommands::Branch(branch_cmd) => match branch_cmd {
            GitBranchCommands::Ls { repo, json } => {
                commands::git::list_branches(ctx, &repo, json).await
            }
            GitBranchCommands::Rm { repo, branch } => {
                commands::git::delete_branch(ctx, &repo, &branch).await
            }
        },
        GitCommands::Ref(ref_cmd) => match ref_cmd {
            GitRefCommands::Ls { repo, json } => commands::git::list_refs(ctx, &repo, json).await,
        },
        GitCommands::Op(op_cmd) => match op_cmd {
            GitOpCommands::Ls {
                repo,
                git_username,
                git_token,
                json,
            } => commands::git::list_operations(ctx, &repo, &git_username, &git_token, json).await,
        },
        GitCommands::Refs { repo, json } => commands::git::list_refs(ctx, &repo, json).await,
        GitCommands::Ops {
            repo,
            git_username,
            git_token,
            json,
        } => commands::git::list_operations(ctx, &repo, &git_username, &git_token, json).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn parse_command<const N: usize>(args: [&str; N]) -> Commands {
        Cli::try_parse_from(args).unwrap().command.unwrap()
    }

    #[test]
    fn clone_command_is_not_supported() {
        let result = Cli::try_parse_from(["tl", "sbx", "clone", "sbx-123"]);

        assert!(result.is_err());
    }

    #[test]
    fn build_images_command_is_hidden_from_help() {
        let mut help = Vec::new();
        Cli::command().write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(!help.contains("build-images"));
        assert!(Cli::try_parse_from(["tl", "build-images", "app.py"]).is_ok());
    }

    #[test]
    fn missing_subcommand_error_omits_hidden_commands_and_aliases() {
        assert!(Cli::try_parse_from(["tl"]).is_ok());

        let error = missing_subcommand_error();
        assert!(error.contains("Usage: tl [OPTIONS] <COMMAND>"));
        assert!(!error.contains("subcommands:"));
        assert!(!error.contains("new"));
        assert!(!error.contains("deploy"));
        assert!(!error.contains("build-images"));
        assert!(!error.contains("parse"));
        assert!(!error.contains("cron"));
        assert!(!error.contains("ssh-key"));
        assert!(!error.contains("orchestrate"));
    }

    #[test]
    fn orchestrate_commands_are_grouped_under_app() {
        match parse_command(["tl", "app", "new", "hello_world"]) {
            Commands::App(AppCommands::New { name, force }) => {
                assert_eq!(name, "hello_world");
                assert!(!force);
            }
            _ => panic!("expected app new command"),
        }
        match parse_command(["tl", "app", "deploy", "hello_world.py"]) {
            Commands::App(AppCommands::Deploy { args }) => {
                assert_eq!(args, vec!["hello_world.py"]);
            }
            _ => panic!("expected app deploy command"),
        }
        match parse_command(["tl", "app", "cron", "ls", "hello_world"]) {
            Commands::App(AppCommands::Cron(CronCommands::List { application })) => {
                assert_eq!(application, "hello_world");
            }
            _ => panic!("expected app cron ls command"),
        }
        match parse_command(["tl", "app", "ls"]) {
            Commands::App(AppCommands::Applications(ApplicationsArgs { command: None })) => {}
            _ => panic!("expected app ls command"),
        }
    }

    #[test]
    fn app_aliases_parse() {
        assert!(Cli::try_parse_from(["tl", "apps", "ls"]).is_ok());
        assert!(Cli::try_parse_from(["tl", "orchestrate", "ls"]).is_ok());
    }

    #[test]
    fn git_commands_parse() {
        match parse_command([
            "tl",
            "git",
            "clone",
            "demo",
            "dest-dir",
            "--cache-max-bytes",
            "2GiB",
            "--no-checkout",
        ]) {
            Commands::Git(GitCommands::Clone {
                repo,
                dest,
                cache_dir,
                cache_max_bytes,
                no_checkout,
            }) => {
                assert_eq!(repo, "demo");
                assert_eq!(dest, Some(std::path::PathBuf::from("dest-dir")));
                assert_eq!(cache_dir, None);
                assert_eq!(cache_max_bytes, Some(2 * 1024 * 1024 * 1024));
                assert!(no_checkout);
            }
            _ => panic!("expected git clone command"),
        }

        match parse_command(["tl", "git", "create", "demo", "--default-branch", "trunk"]) {
            Commands::Git(GitCommands::Create {
                repo,
                default_branch,
                json,
            }) => {
                assert_eq!(repo, "demo");
                assert_eq!(default_branch, "trunk");
                assert!(!json);
            }
            _ => panic!("expected git create command"),
        }

        match parse_command(["tl", "git", "ls", "--json"]) {
            Commands::Git(GitCommands::Ls { json }) => {
                assert!(json);
            }
            _ => panic!("expected git ls command"),
        }

        match parse_command(["tl", "git", "rm", "demo"]) {
            Commands::Git(GitCommands::Rm { repo }) => {
                assert_eq!(repo, "demo");
            }
            _ => panic!("expected git rm command"),
        }

        match parse_command(["tl", "git", "branch", "ls", "demo", "--json"]) {
            Commands::Git(GitCommands::Branch(GitBranchCommands::Ls { repo, json })) => {
                assert_eq!(repo, "demo");
                assert!(json);
            }
            _ => panic!("expected git branch ls command"),
        }

        match parse_command(["tl", "git", "branch", "rm", "demo", "feature-a"]) {
            Commands::Git(GitCommands::Branch(GitBranchCommands::Rm { repo, branch })) => {
                assert_eq!(repo, "demo");
                assert_eq!(branch, "feature-a");
            }
            _ => panic!("expected git branch rm command"),
        }

        match parse_command(["tl", "git", "ref", "ls", "demo", "--json"]) {
            Commands::Git(GitCommands::Ref(GitRefCommands::Ls { repo, json })) => {
                assert_eq!(repo, "demo");
                assert!(json);
            }
            _ => panic!("expected git ref ls command"),
        }

        match parse_command(["tl", "git", "op", "ls", "demo", "--git-token", "secret"]) {
            Commands::Git(GitCommands::Op(GitOpCommands::Ls {
                repo,
                git_username,
                git_token,
                json,
            })) => {
                assert_eq!(repo, "demo");
                assert_eq!(git_username, "t");
                assert_eq!(git_token, "secret");
                assert!(!json);
            }
            _ => panic!("expected git op ls command"),
        }

        // token takes the repo positionally, like every other repo-addressed command;
        // omitting it mints a project-wide credential.
        match parse_command(["tl", "git", "token", "demo"]) {
            Commands::Git(GitCommands::Token { repo, json }) => {
                assert_eq!(repo.as_deref(), Some("demo"));
                assert!(!json);
            }
            _ => panic!("expected git token command"),
        }
        match parse_command(["tl", "git", "token", "--json"]) {
            Commands::Git(GitCommands::Token { repo, json }) => {
                assert_eq!(repo, None);
                assert!(json);
            }
            _ => panic!("expected git token command"),
        }
        assert!(Cli::try_parse_from(["tl", "git", "token", "--repo", "demo"]).is_err());

        match parse_command([
            "tl", "git", "setup", "demo", "--remote", "origin", "--create",
        ]) {
            Commands::Git(GitCommands::Setup {
                repo,
                remote,
                create,
            }) => {
                assert_eq!(repo.as_deref(), Some("demo"));
                assert_eq!(remote, "origin");
                assert!(create);
            }
            _ => panic!("expected git setup command"),
        }
        // Repo defaults to the worktree directory name; the remote defaults to `tl`.
        match parse_command(["tl", "git", "setup"]) {
            Commands::Git(GitCommands::Setup {
                repo,
                remote,
                create,
            }) => {
                assert_eq!(repo, None);
                assert_eq!(remote, "tl");
                assert!(!create);
            }
            _ => panic!("expected git setup command"),
        }

        // Git invokes the helper with root flags first and the operation appended last:
        // `tl --organization org_1 git credential-helper get`.
        match parse_command([
            "tl",
            "--organization",
            "org_1",
            "git",
            "credential-helper",
            "get",
        ]) {
            Commands::Git(GitCommands::CredentialHelper { operation }) => {
                assert_eq!(operation, "get");
            }
            _ => panic!("expected git credential-helper command"),
        }

        match parse_command(["tl", "git", "info", "demo", "--json"]) {
            Commands::Git(GitCommands::Info { repo, json }) => {
                assert_eq!(repo, "demo");
                assert!(json);
            }
            _ => panic!("expected git info command"),
        }

        match parse_command(["tl", "git", "status", "demo", "--json"]) {
            Commands::Git(GitCommands::Info { repo, json }) => {
                assert_eq!(repo, "demo");
                assert!(json);
            }
            _ => panic!("expected git status alias"),
        }

        // `tl git url` merged into `tl git info` (which prints the remote URL).
        match parse_command(["tl", "git", "url", "demo"]) {
            Commands::Git(GitCommands::Info { repo, json }) => {
                assert_eq!(repo, "demo");
                assert!(!json);
            }
            _ => panic!("expected git url alias of info"),
        }

        match parse_command(["tl", "git", "repo", "create", "demo"]) {
            Commands::Git(GitCommands::Repo(GitRepoCommands::Create { repo, .. })) => {
                assert_eq!(repo, "demo");
            }
            _ => panic!("expected legacy git repo create command"),
        }

        match parse_command(["tl", "git", "repos", "list", "--json"]) {
            Commands::Git(GitCommands::Repo(GitRepoCommands::Ls { json })) => {
                assert!(json);
            }
            _ => panic!("expected legacy git repos list alias"),
        }

        match parse_command(["tl", "git", "repo", "delete", "demo"]) {
            Commands::Git(GitCommands::Repo(GitRepoCommands::Rm { repo })) => {
                assert_eq!(repo, "demo");
            }
            _ => panic!("expected legacy git repo delete alias"),
        }

        match parse_command(["tl", "git", "branches", "list", "demo"]) {
            Commands::Git(GitCommands::Branch(GitBranchCommands::Ls { repo, .. })) => {
                assert_eq!(repo, "demo");
            }
            _ => panic!("expected legacy git branches list alias"),
        }

        match parse_command(["tl", "git", "refs", "demo", "--json"]) {
            Commands::Git(GitCommands::Refs { repo, json }) => {
                assert_eq!(repo, "demo");
                assert!(json);
            }
            _ => panic!("expected legacy git refs command"),
        }

        match parse_command(["tl", "git", "ops", "demo", "--git-token", "secret"]) {
            Commands::Git(GitCommands::Ops {
                repo,
                git_username,
                git_token,
                json,
            }) => {
                assert_eq!(repo, "demo");
                assert_eq!(git_username, "t");
                assert_eq!(git_token, "secret");
                assert!(!json);
            }
            _ => panic!("expected legacy git ops command"),
        }

        match parse_command(["tl", "git", "push", "demo"]) {
            Commands::Git(GitCommands::Push { repo, branch, .. }) => {
                assert_eq!(repo, "demo");
                assert_eq!(branch, "main");
            }
            _ => panic!("expected git push command"),
        }

        match parse_command(["tl", "git", "push", "demo", "feature-a"]) {
            Commands::Git(GitCommands::Push { repo, branch, .. }) => {
                assert_eq!(repo, "demo");
                assert_eq!(branch, "feature-a");
            }
            _ => panic!("expected git push command"),
        }

        // commit-status takes the repo positionally, like every other repo-addressed command.
        match parse_command(["tl", "git", "commit-status", "demo", "job-123"]) {
            Commands::Git(GitCommands::CommitStatus { repo, job_id }) => {
                assert_eq!(repo, "demo");
                assert_eq!(job_id, "job-123");
            }
            _ => panic!("expected git commit-status command"),
        }
        assert!(
            Cli::try_parse_from(["tl", "git", "commit-status", "--repo", "demo", "job-123"])
                .is_err()
        );
    }

    #[test]
    fn fs_commands_are_workspace_centric() {
        match parse_command(["tl", "fs", "mount", "data:main", "./w", "--mode", "ro"]) {
            Commands::Fs(FsCommands::Mount {
                target,
                path,
                mode,
                shared_rw,
                auto_commit_interval_secs,
                foreground,
                trace_ops,
                log_level,
            }) => {
                assert_eq!(target, "data:main");
                assert_eq!(path, PathBuf::from("./w"));
                assert_eq!(mode, Some(MountWriteMode::Ro));
                assert!(!shared_rw);
                assert_eq!(auto_commit_interval_secs, None);
                assert!(!foreground);
                assert!(!trace_ops);
                assert_eq!(log_level, "info");
            }
            _ => panic!("expected fs mount command"),
        }

        // A bare workspace id mounts an existing workspace; --mode rw forces writes.
        match parse_command(["tl", "fs", "mount", "0a1b2c3d", "./w", "--mode", "rw"]) {
            Commands::Fs(FsCommands::Mount { target, mode, .. }) => {
                assert_eq!(target, "0a1b2c3d");
                assert_eq!(mode, Some(MountWriteMode::Rw));
            }
            _ => panic!("expected fs mount command"),
        }

        match parse_command([
            "tl",
            "fs",
            "mount",
            "data",
            "./w",
            "--auto-commit-interval-secs",
            "30",
        ]) {
            Commands::Fs(FsCommands::Mount {
                auto_commit_interval_secs,
                ..
            }) => {
                assert_eq!(auto_commit_interval_secs, Some(30));
            }
            _ => panic!("expected fs mount command"),
        }

        // Zero is rejected at parse time: an interval of 0 is not a debounce, it's a busy loop.
        assert!(
            Cli::try_parse_from([
                "tl",
                "fs",
                "mount",
                "data",
                "./w",
                "--auto-commit-interval-secs",
                "0",
            ])
            .is_err()
        );

        match parse_command(["tl", "fs", "ls"]) {
            Commands::Fs(FsCommands::Ls {
                file_system: None,
                json: false,
            }) => {}
            _ => panic!("expected fs ls command"),
        }

        match parse_command(["tl", "fs", "ls", "data", "--json"]) {
            Commands::Fs(FsCommands::Ls { file_system, json }) => {
                assert_eq!(file_system.as_deref(), Some("data"));
                assert!(json);
            }
            _ => panic!("expected fs ls command"),
        }

        match parse_command(["tl", "fs", "rm", "0a1b2c3d"]) {
            Commands::Fs(FsCommands::Rm { workspace_id }) => {
                assert_eq!(workspace_id, "0a1b2c3d");
            }
            _ => panic!("expected fs rm command"),
        }

        match parse_command(["tl", "fs", "setup", "--check"]) {
            Commands::Fs(FsCommands::Setup {
                from: None,
                check: true,
            }) => {}
            _ => panic!("expected fs setup command"),
        }

        // Path-addressed commands default the mounted directory to the mount containing the
        // CWD, so the path positional is optional everywhere.
        match parse_command(["tl", "fs", "snapshot"]) {
            Commands::Fs(FsCommands::Snapshot {
                path: None,
                message: None,
                clear: false,
            }) => {}
            _ => panic!("expected fs snapshot command"),
        }

        match parse_command(["tl", "fs", "snapshot", "./w", "-m", "wip"]) {
            Commands::Fs(FsCommands::Snapshot {
                path,
                message,
                clear,
            }) => {
                assert_eq!(path, Some(PathBuf::from("./w")));
                assert_eq!(message.as_deref(), Some("wip"));
                assert!(!clear, "clear is opt-in");
            }
            _ => panic!("expected fs snapshot command"),
        }

        // The destructive seal-and-clear is an explicit flag.
        match parse_command(["tl", "fs", "snapshot", "--clear"]) {
            Commands::Fs(FsCommands::Snapshot { clear: true, .. }) => {}
            _ => panic!("expected fs snapshot --clear command"),
        }

        // Promote/restore have a required positional after the optional path; with a single
        // value the path is the one skipped (allow_missing_positional).
        match parse_command(["tl", "fs", "promote", "main"]) {
            Commands::Fs(FsCommands::Promote { path, branch, .. }) => {
                assert_eq!(path, None);
                assert_eq!(branch, "main");
            }
            _ => panic!("expected fs promote command"),
        }

        match parse_command(["tl", "fs", "promote", "./w", "main"]) {
            Commands::Fs(FsCommands::Promote { path, branch, .. }) => {
                assert_eq!(path, Some(PathBuf::from("./w")));
                assert_eq!(branch, "main");
            }
            _ => panic!("expected fs promote command"),
        }

        match parse_command(["tl", "fs", "restore", "0a1b2c3d"]) {
            Commands::Fs(FsCommands::Restore { path, version }) => {
                assert_eq!(path, None);
                assert_eq!(version, "0a1b2c3d");
            }
            _ => panic!("expected fs restore command"),
        }

        assert!(Cli::try_parse_from(["tl", "fs", "promote"]).is_err());
        assert!(Cli::try_parse_from(["tl", "fs", "restore"]).is_err());

        // The workspace subgroup is gone: workspaces ARE the `tl fs` surface.
        assert!(Cli::try_parse_from(["tl", "fs", "workspace", "ls", "data"]).is_err());

        // The old file-system registry commands are gone: file systems are managed by `tl git`.
        assert!(Cli::try_parse_from(["tl", "fs", "create", "--name", "data"]).is_err());
        assert!(Cli::try_parse_from(["tl", "fs", "mount", "data", "./w", "--shared-ro"]).is_err());
        assert!(
            Cli::try_parse_from(["tl", "fs", "mount", "data", "./w", "--workspace", "0a1b"])
                .is_err()
        );
    }

    #[test]
    fn legacy_orchestrate_commands_are_hidden_but_still_parse() {
        let mut help = Vec::new();
        Cli::command().write_long_help(&mut help).unwrap();
        let help = String::from_utf8(help).unwrap();

        assert!(!help.contains(" new "));
        assert!(!help.contains(" deploy "));
        assert!(!help.contains(" cron "));
        assert!(!help.contains(" parse "));
        assert!(!help.contains(" ls "));

        assert!(Cli::try_parse_from(["tl", "new", "hello_world"]).is_ok());
        assert!(Cli::try_parse_from(["tl", "deploy", "hello_world.py"]).is_ok());
        assert!(Cli::try_parse_from(["tl", "cron", "ls", "hello_world"]).is_ok());
        assert!(Cli::try_parse_from(["tl", "parse", "document.pdf"]).is_ok());
        assert!(Cli::try_parse_from(["tl", "ls"]).is_ok());
    }

    #[test]
    fn cp_file_copy_requires_destination() {
        let result = Cli::try_parse_from(["tl", "sbx", "cp", "sbx-123"]);

        assert!(result.is_err());
    }

    #[test]
    fn cp_file_copy_parses_destination() {
        match parse_command(["tl", "sbx", "cp", "local.txt", "sbx-123:/tmp/file"]) {
            Commands::Sbx(SbxCommands::Cp { src, dest }) => {
                assert_eq!(src, "local.txt");
                assert_eq!(dest, "sbx-123:/tmp/file");
            }
            _ => panic!("expected sbx cp command"),
        }
    }

    #[test]
    fn copy_times_defaults_to_one() {
        match parse_command(["tl", "sbx", "copy", "sbx-123"]) {
            Commands::Sbx(SbxCommands::Copy { times, .. }) => assert_eq!(times.get(), 1),
            _ => panic!("expected sbx copy command"),
        }
    }

    #[test]
    fn copy_times_parses_explicit_value() {
        match parse_command(["tl", "sbx", "copy", "sbx-123", "--times", "3"]) {
            Commands::Sbx(SbxCommands::Copy {
                sandbox_id, times, ..
            }) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(times.get(), 3);
            }
            _ => panic!("expected sbx copy command"),
        }
    }

    #[test]
    fn copy_times_rejects_zero() {
        let result = Cli::try_parse_from(["tl", "sbx", "copy", "sbx-123", "--times", "0"]);

        assert!(result.is_err());
    }

    #[test]
    fn sbx_ls_rejects_conflicting_status_filters() {
        for pair in [
            ["-a", "-r"],
            ["-a", "-s"],
            ["-a", "-t"],
            ["-r", "-s"],
            ["-r", "-t"],
            ["-s", "-t"],
        ] {
            let result = Cli::try_parse_from(["tl", "sbx", "ls", pair[0], pair[1]]);
            assert!(
                result.is_err(),
                "expected {} {} to conflict",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn checkpoint_type_maps_to_wire_values() {
        assert_eq!(SnapshotTypeArg::Memory.as_wire_value(), "memory");
        assert_eq!(SnapshotTypeArg::Filesystem.as_wire_value(), "filesystem");
    }

    #[test]
    fn restart_policy_maps_to_wire_values() {
        assert_eq!(RestartPolicyArg::Never.as_wire_value(), "never");
        assert_eq!(RestartPolicyArg::OnFailure.as_wire_value(), "on_failure");
        assert_eq!(RestartPolicyArg::Always.as_wire_value(), "always");
    }

    #[test]
    fn sbx_checkpoint_parses_memory_checkpoint_type() {
        match parse_command([
            "tl",
            "sbx",
            "checkpoint",
            "sbx-123",
            "--checkpoint-type",
            "memory",
        ]) {
            Commands::Sbx(SbxCommands::Checkpoint(SnapshotArgs {
                sandbox_id,
                checkpoint_type,
                ..
            })) => {
                assert_eq!(sandbox_id.as_deref(), Some("sbx-123"));
                assert_eq!(checkpoint_type, Some(SnapshotTypeArg::Memory));
            }
            _ => panic!("expected sbx checkpoint command"),
        }
    }

    #[test]
    fn sbx_checkpoint_parses_filesystem_checkpoint_type() {
        match parse_command([
            "tl",
            "sbx",
            "checkpoint",
            "sbx-123",
            "--checkpoint-type",
            "filesystem",
        ]) {
            Commands::Sbx(SbxCommands::Checkpoint(SnapshotArgs {
                sandbox_id,
                checkpoint_type,
                ..
            })) => {
                assert_eq!(sandbox_id.as_deref(), Some("sbx-123"));
                assert_eq!(checkpoint_type, Some(SnapshotTypeArg::Filesystem));
            }
            _ => panic!("expected sbx checkpoint command"),
        }
    }

    #[test]
    fn sbx_run_parses_disk_mb_override() {
        match parse_command(["tl", "sbx", "run", "--disk_mb", "30720", "echo", "hello"]) {
            Commands::Sbx(SbxCommands::Run { disk_mb, .. }) => {
                assert_eq!(disk_mb, Some(30720));
            }
            _ => panic!("expected sbx run command"),
        }
    }

    #[test]
    fn sbx_exec_parses_process_user() {
        match parse_command(["tl", "sbx", "exec", "--user", "1000:1000", "sbx-123", "id"]) {
            Commands::Sbx(SbxCommands::Exec { user, .. }) => {
                assert_eq!(user.as_deref(), Some("1000:1000"));
            }
            _ => panic!("expected sbx exec command"),
        }
    }

    #[test]
    fn sbx_exec_omits_process_user_by_default() {
        match parse_command(["tl", "sbx", "exec", "sbx-123", "id"]) {
            Commands::Sbx(SbxCommands::Exec { user, .. }) => {
                assert_eq!(user, None);
            }
            _ => panic!("expected sbx exec command"),
        }
    }

    #[test]
    fn sbx_exec_parses_managed_detach_flags() {
        match parse_command([
            "tl",
            "sbx",
            "exec",
            "sbx-123",
            "--detach",
            "--name",
            "web",
            "--restart",
            "on-failure",
            "--max-restarts",
            "3",
            "--health-http",
            "8000:/health",
            "python",
            "app.py",
        ]) {
            Commands::Sbx(SbxCommands::Exec {
                detach,
                name,
                restart,
                max_restarts,
                health_http,
                command,
                args,
                ..
            }) => {
                assert!(detach);
                assert_eq!(name.as_deref(), Some("web"));
                assert_eq!(restart, Some(RestartPolicyArg::OnFailure));
                assert_eq!(max_restarts, Some(3));
                assert_eq!(health_http.as_deref(), Some("8000:/health"));
                assert_eq!(command, "python");
                assert_eq!(args, vec!["app.py"]);
            }
            _ => panic!("expected sbx exec command"),
        }
    }

    #[test]
    fn sbx_process_lifecycle_commands_parse() {
        match parse_command(["tl", "sbx", "ps", "sbx-123", "web", "--json"]) {
            Commands::Sbx(SbxCommands::Ps {
                sandbox_id,
                process,
                json,
            }) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(process.as_deref(), Some("web"));
                assert!(json);
            }
            _ => panic!("expected sbx ps command"),
        }
        match parse_command(["tl", "sbx", "restart", "sbx-123", "42"]) {
            Commands::Sbx(SbxCommands::Restart {
                sandbox_id,
                process,
            }) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(process, "42");
            }
            _ => panic!("expected sbx restart command"),
        }
        match parse_command(["tl", "sbx", "kill", "sbx-123", "web"]) {
            Commands::Sbx(SbxCommands::Kill {
                sandbox_id,
                process,
            }) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(process, "web");
            }
            _ => panic!("expected sbx kill command"),
        }
    }

    #[test]
    fn sbx_logs_commands_parse() {
        match parse_command([
            "tl",
            "sbx",
            "logs",
            "sbx-123",
            "--level",
            "info",
            "--process-id",
            "proc-1",
            "--tail",
            "25",
            "--json",
        ]) {
            Commands::Sbx(SbxCommands::Logs(args)) => {
                assert_eq!(args.sandbox_id.as_deref(), Some("sbx-123"));
                assert_eq!(args.command, None);
                assert_eq!(args.level, vec![SandboxLogLevelArg::Info]);
                assert_eq!(args.process_id, vec!["proc-1"]);
                assert_eq!(args.tail, Some(25));
                assert!(args.json);
            }
            _ => panic!("expected sbx logs command"),
        }

        match parse_command(["tl", "sbx", "logs", "streams", "sbx-123", "--json"]) {
            Commands::Sbx(SbxCommands::Logs(args)) => {
                assert_eq!(args.sandbox_id, None);
                match args.command {
                    Some(LogsCommands::Streams { sandbox_id, json }) => {
                        assert_eq!(sandbox_id, "sbx-123");
                        assert!(json);
                    }
                    _ => panic!("expected sbx logs streams command"),
                }
            }
            _ => panic!("expected sbx logs command"),
        }
    }

    #[test]
    fn sbx_run_parses_process_user() {
        match parse_command(["tl", "sbx", "run", "--user", "ubuntu", "id"]) {
            Commands::Sbx(SbxCommands::Run { user, .. }) => {
                assert_eq!(user.as_deref(), Some("ubuntu"));
            }
            _ => panic!("expected sbx run command"),
        }
    }

    #[test]
    fn sbx_run_omits_process_user_by_default() {
        match parse_command(["tl", "sbx", "run", "id"]) {
            Commands::Sbx(SbxCommands::Run { user, .. }) => {
                assert_eq!(user, None);
            }
            _ => panic!("expected sbx run command"),
        }
    }

    #[test]
    fn sbx_create_parses_disk_mb_override() {
        match parse_command([
            "tl",
            "sbx",
            "create",
            "--disk_mb",
            "30720",
            "--image",
            "tensorlake/ubuntu-minimal",
        ]) {
            Commands::Sbx(SbxCommands::Create { disk_mb, .. }) => {
                assert_eq!(disk_mb, Some(30720));
            }
            _ => panic!("expected sbx create command"),
        }
    }

    #[test]
    fn sbx_create_parses_gpu_request_with_default_model() {
        let cli = Cli::try_parse_from([
            "tl",
            "sbx",
            "create",
            "--gpus",
            "1",
            "--image",
            "tensorlake/ubuntu-minimal",
        ])
        .unwrap();

        match cli.command {
            Some(Commands::Sbx(SbxCommands::Create {
                gpus, gpu_model, ..
            })) => {
                assert_eq!(gpus, Some(1));
                assert!(gpu_model.is_none());
            }
            _ => panic!("expected sbx create command"),
        }
    }

    #[test]
    fn sbx_create_parses_gpu_request_with_explicit_model() {
        let cli = Cli::try_parse_from([
            "tl",
            "sbx",
            "create",
            "--gpus",
            "1",
            "--gpu-model",
            "A10",
            "--image",
            "tensorlake/ubuntu-minimal",
        ])
        .unwrap();

        match cli.command {
            Some(Commands::Sbx(SbxCommands::Create {
                gpus, gpu_model, ..
            })) => {
                assert_eq!(gpus, Some(1));
                assert_eq!(gpu_model.map(GpuModelArg::as_wire_value), Some("A10"));
            }
            _ => panic!("expected sbx create command"),
        }
    }

    #[test]
    fn sbx_create_rejects_gpu_model_without_gpu_count() {
        let result = Cli::try_parse_from(["tl", "sbx", "create", "--gpu-model", "A10"]);

        assert!(result.is_err());
    }

    #[test]
    fn sbx_create_parses_hidden_disk_gb_override() {
        match parse_command([
            "tl",
            "sbx",
            "create",
            "--disk",
            "25",
            "--image",
            "tensorlake/ubuntu-minimal",
        ]) {
            Commands::Sbx(SbxCommands::Create {
                disk_mb, disk_gb, ..
            }) => {
                assert_eq!(disk_mb, None);
                assert_eq!(disk_gb, Some(25));
            }
            _ => panic!("expected sbx create command"),
        }
    }

    #[test]
    fn image_create_parses_cpu_memory_and_disk_overrides() {
        match parse_command([
            "tl",
            "sbx",
            "image",
            "create",
            "./Dockerfile",
            "--cpus",
            "3.5",
            "--memory",
            "8192",
            "--disk_mb",
            "30720",
            "--builder_disk_mb",
            "65536",
        ]) {
            Commands::Sbx(SbxCommands::Image(ImageCommands::Create {
                cpus,
                memory,
                disk_mb,
                builder_disk_mb,
                ..
            })) => {
                assert_eq!(cpus, Some(3.5));
                assert_eq!(memory, Some(8192));
                assert_eq!(disk_mb, Some(30720));
                assert_eq!(builder_disk_mb, Some(65536));
            }
            _ => panic!("expected sbx image create command"),
        }
    }

    #[test]
    fn image_create_parses_docker_compat() {
        match parse_command([
            "tl",
            "sbx",
            "image",
            "create",
            "./Dockerfile",
            "--docker_compat",
        ]) {
            Commands::Sbx(SbxCommands::Image(ImageCommands::Create { docker_compat, .. })) => {
                assert!(docker_compat)
            }
            _ => panic!("expected sbx image create command"),
        }
    }

    #[test]
    fn image_import_parses_docker_compat() {
        match parse_command([
            "tl",
            "sbx",
            "image",
            "import",
            "ubuntu:24.04",
            "--docker_compat",
        ]) {
            Commands::Sbx(SbxCommands::Image(ImageCommands::Import { docker_compat, .. })) => {
                assert!(docker_compat)
            }
            _ => panic!("expected sbx image import command"),
        }
    }

    #[test]
    fn image_create_parses_hidden_disk_gb_override() {
        match parse_command([
            "tl",
            "sbx",
            "image",
            "create",
            "./Dockerfile",
            "--disk",
            "25",
        ]) {
            Commands::Sbx(SbxCommands::Image(ImageCommands::Create {
                disk_mb, disk_gb, ..
            })) => {
                assert_eq!(disk_mb, None);
                assert_eq!(disk_gb, Some(25));
            }
            _ => panic!("expected sbx image create command"),
        }
    }

    #[test]
    fn image_register_parses_name_and_snapshot_id() {
        match parse_command([
            "tl",
            "sbx",
            "image",
            "register",
            "mighty-agent-0.0.69",
            "snap_123",
            "--dockerfile",
            "./Dockerfile",
            "--public",
        ]) {
            Commands::Sbx(SbxCommands::Image(ImageCommands::Register {
                image_name,
                snapshot_id,
                dockerfile_path,
                public,
            })) => {
                assert_eq!(image_name, "mighty-agent-0.0.69");
                assert_eq!(snapshot_id, "snap_123");
                assert_eq!(dockerfile_path, "./Dockerfile");
                assert!(public);
            }
            _ => panic!("expected sbx image register command"),
        }
    }

    #[test]
    fn sbx_port_expose_parses_ports() {
        match parse_command(["tl", "sbx", "port", "expose", "sbx-123", "8080", "3000"]) {
            Commands::Sbx(SbxCommands::Port(PortCommands::Expose { ports, .. })) => {
                assert_eq!(ports, vec![8080, 3000]);
            }
            _ => panic!("expected sbx port expose command"),
        }
    }

    #[test]
    fn sbx_port_rm_requires_ports() {
        let result = Cli::try_parse_from(["tl", "sbx", "port", "rm", "sbx-123"]);

        assert!(result.is_err());
    }

    #[test]
    fn sbx_port_expose_rejects_zero() {
        let result = Cli::try_parse_from(["tl", "sbx", "port", "expose", "sbx-123", "0"]);

        assert!(result.is_err());
    }

    #[test]
    fn sbx_port_rm_rejects_management_port() {
        let result = Cli::try_parse_from(["tl", "sbx", "port", "rm", "sbx-123", "9501"]);

        assert!(result.is_err());
    }

    #[test]
    fn sbx_tunnel_parses_remote_and_listen_ports() {
        match parse_command([
            "tl",
            "sbx",
            "tunnel",
            "sbx-123",
            "5900",
            "--listen-port",
            "15900",
        ]) {
            Commands::Sbx(SbxCommands::Tunnel {
                sandbox_id,
                remote_port,
                listen_port,
            }) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(remote_port, 5900);
                assert_eq!(listen_port, Some(15900));
            }
            _ => panic!("expected sbx tunnel command"),
        }
    }

    #[test]
    fn sbx_ssh_parses_shell_args_workdir_and_env() {
        match parse_command([
            "tl",
            "sbx",
            "ssh",
            "sbx-123",
            "--shell",
            "/bin/zsh",
            "--shell-arg",
            "-l",
            "--shell-arg",
            "-c",
            "--workdir",
            "/tmp/work",
            "--env",
            "FOO=bar",
            "--env",
            "TERM=screen-256color",
        ]) {
            Commands::Sbx(SbxCommands::Ssh(SshArgs {
                command,
                sandbox_id,
                shell,
                shell_args,
                workdir,
                env,
            })) => {
                assert!(command.is_none());
                assert_eq!(sandbox_id.as_deref(), Some("sbx-123"));
                assert_eq!(shell, "/bin/zsh");
                assert_eq!(shell_args, vec!["-l", "-c"]);
                assert_eq!(workdir, Some("/tmp/work".to_string()));
                assert_eq!(env, vec!["FOO=bar", "TERM=screen-256color"]);
            }
            _ => panic!("expected sbx ssh command"),
        }
    }

    #[test]
    fn sbx_ssh_keys_ls_parses() {
        match parse_command(["tl", "sbx", "ssh", "keys", "ls"]) {
            Commands::Sbx(SbxCommands::Ssh(SshArgs {
                command: Some(SshCommands::Keys(SshKeysCommands::Ls)),
                sandbox_id: None,
                ..
            })) => {}
            _ => panic!("expected sbx ssh keys ls command"),
        }
    }

    #[test]
    fn sbx_pty_ls_parses() {
        match parse_command(["tl", "sbx", "pty", "ls", "sbx-123"]) {
            Commands::Sbx(SbxCommands::Pty(PtyCommands::Ls {
                sandbox_id,
                output_json,
            })) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert!(!output_json);
            }
            _ => panic!("expected sbx pty ls command"),
        }
    }

    #[test]
    fn sbx_pty_ls_with_json_parses() {
        match parse_command(["tl", "sbx", "pty", "ls", "sbx-123", "--json"]) {
            Commands::Sbx(SbxCommands::Pty(PtyCommands::Ls {
                sandbox_id,
                output_json,
            })) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert!(output_json);
            }
            _ => panic!("expected sbx pty ls command"),
        }
    }

    #[test]
    fn sbx_pty_attach_parses() {
        match parse_command([
            "tl", "sbx", "pty", "attach", "sbx-123", "sess-1", "--token", "tok-1",
        ]) {
            Commands::Sbx(SbxCommands::Pty(PtyCommands::Attach {
                sandbox_id,
                session_id,
                token,
            })) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(session_id, "sess-1");
                assert_eq!(token, "tok-1");
            }
            _ => panic!("expected sbx pty attach command"),
        }
    }

    #[test]
    fn sbx_pty_rm_parses_multiple_session_ids() {
        match parse_command(["tl", "sbx", "pty", "rm", "sbx-123", "sess-1", "sess-2"]) {
            Commands::Sbx(SbxCommands::Pty(PtyCommands::Rm {
                sandbox_id,
                session_ids,
            })) => {
                assert_eq!(sandbox_id, "sbx-123");
                assert_eq!(session_ids, vec!["sess-1", "sess-2"]);
            }
            _ => panic!("expected sbx pty rm command"),
        }
    }

    #[test]
    fn sbx_tunnel_rejects_zero_port() {
        let result = Cli::try_parse_from(["tl", "sbx", "tunnel", "sbx-123", "0"]);

        assert!(result.is_err());
    }
}
