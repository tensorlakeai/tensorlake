mod auth;
mod commands;
mod config;
mod error;
mod http;
mod output;
mod project;

use clap::{Parser, Subcommand};

use auth::context::CliContext;
use auth::guard::ensure_auth_and_project;
use config::resolver;
use error::CliError;

#[derive(Parser)]
#[command(
    name = "tl",
    about = "Tensorlake CLI",
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
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Login to TensorLake
    Login,

    /// Print authentication status
    Whoami {
        /// Output format
        #[arg(short, long, default_value = "text")]
        output: String,
    },

    /// Initialize TensorLake configuration for this project
    Init {
        /// Project directory (default: auto-detect)
        #[arg(short, long)]
        directory: Option<String>,

        /// Skip confirmation of detected project directory
        #[arg(long)]
        no_confirm: bool,
    },

    /// Create a new Tensorlake application
    New {
        /// Application name
        name: String,

        /// Overwrite existing files
        #[arg(long)]
        force: bool,
    },

    /// Deploy applications to Tensorlake Cloud
    Deploy {
        /// Arguments passed to the deploy Python module (use --build-env KEY=VALUE to inject ENV directives into generated Dockerfiles)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Build Docker images for applications defined in an application file
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
        #[arg(long)]
        template: Option<String>,

        /// Push built images to the registry after building
        #[arg(long)]
        push: bool,

        /// Environment variable to inject into the generated Dockerfile as an ENV directive (KEY=VALUE, repeatable)
        #[arg(long = "build-env", value_name = "KEY=VALUE")]
        build_envs: Vec<String>,
    },

    /// Parse a document and print markdown
    Parse {
        /// Arguments passed to the parse Python module
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },

    /// Manage secrets
    #[command(subcommand)]
    Secrets(SecretsCommands),

    /// List applications
    #[command(name = "ls")]
    Applications(ApplicationsArgs),

    /// Manage sandboxes
    #[command(subcommand)]
    Sbx(SbxCommands),
}

#[derive(Subcommand)]
enum SecretsCommands {
    /// List all secrets
    Ls,
    /// Set one or more secrets (KEY=VALUE)
    Set {
        /// Secret key-value pairs (KEY=VALUE)
        #[arg(required = true)]
        secrets: Vec<String>,
    },
    /// Remove one or more secrets
    Rm {
        /// Secret names to unset
        #[arg(required = true)]
        secret_names: Vec<String>,
    },
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

#[derive(Subcommand)]
enum SbxCommands {
    /// List all sandboxes
    Ls,

    /// Stop (terminate) one or more sandboxes
    Stop {
        /// Sandbox IDs
        #[arg(required = true)]
        sandbox_ids: Vec<String>,
    },

    /// Create a new sandbox
    New {
        /// Container image
        #[arg(short, long)]
        image: Option<String>,

        /// Number of CPUs
        #[arg(long, default_value = "1.0")]
        cpus: f64,

        /// Memory in MB
        #[arg(long, default_value = "512")]
        memory: i64,

        /// Ephemeral disk in MB
        #[arg(long, default_value = "1024")]
        disk: i64,

        /// Timeout in seconds
        #[arg(long)]
        timeout: Option<i64>,

        /// Entrypoint command parts
        #[arg(long)]
        entrypoint: Vec<String>,

        /// Create from a snapshot ID
        #[arg(long)]
        snapshot: Option<String>,

        /// Wait for sandbox to be running
        #[arg(long, default_value = "true")]
        wait: bool,
    },

    /// Execute a command in a sandbox
    Exec {
        /// Sandbox ID
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
    },

    /// Copy files between local and sandbox
    Cp {
        /// Source path (sandbox_id:/path or local path)
        src: String,

        /// Destination path (sandbox_id:/path or local path)
        dest: String,
    },

    /// Create a snapshot or list snapshots
    Snapshot(SnapshotArgs),

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
        #[arg(long, default_value = "1.0")]
        cpus: f64,

        /// Memory in MB
        #[arg(long, default_value = "512")]
        memory: i64,

        /// Ephemeral disk in MB
        #[arg(long, default_value = "1024")]
        disk: i64,

        /// Command timeout in seconds
        #[arg(short, long)]
        timeout: Option<f64>,

        /// Working directory
        #[arg(short, long)]
        workdir: Option<String>,

        /// Environment variable (KEY=VALUE)
        #[arg(short, long)]
        env: Vec<String>,

        /// Keep sandbox after command exits
        #[arg(long)]
        keep: bool,
    },

    /// Interactive shell in a sandbox
    Ssh {
        /// Sandbox ID
        sandbox_id: String,

        /// Shell to use
        #[arg(short, long, default_value = "/bin/bash")]
        shell: String,
    },
}

#[derive(Parser)]
struct SnapshotArgs {
    #[command(subcommand)]
    command: Option<SnapshotCommands>,

    /// Sandbox ID
    sandbox_id: Option<String>,

    /// Max seconds to wait
    #[arg(short, long, default_value = "300", requires = "sandbox_id")]
    timeout: f64,
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

    let result = run_command(&mut ctx, cli.command).await;

    if let Err(e) = result {
        match &e {
            CliError::ExitCode(code) => std::process::exit(*code),
            CliError::Cancelled => std::process::exit(1),
            _ => {
                eprintln!("Error: {}", e);
                if ctx.debug {
                    eprintln!("\nDebug info:");
                    eprintln!("  {:?}", e);
                }
                std::process::exit(1);
            }
        }
    }
}

async fn run_command(ctx: &mut CliContext, command: Commands) -> error::Result<()> {
    match command {
        Commands::Login => commands::login::run(ctx).await,
        Commands::Whoami { output } => commands::whoami::run(ctx, output == "json").await,
        Commands::Init {
            directory,
            no_confirm,
        } => commands::init::run(ctx, directory.as_deref(), no_confirm).await,
        Commands::New { name, force } => commands::new::run(&name, force),
        Commands::Deploy { args } => {
            let onprem = std::env::var("TENSORLAKE_ONPREM")
                .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
                .unwrap_or(false);
            if !onprem {
                ensure_auth_and_project(ctx).await?;
            }
            commands::deploy::run(ctx, &args).await
        }
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
            commands::build_images::run(
                &application_file_path,
                repository.as_deref(),
                tag.as_deref(),
                image_name.as_deref(),
                &stage,
                template.as_deref(),
                push,
                &build_envs,
            )
            .await
        }
        Commands::Parse { args } => commands::parse::run(ctx, &args).await,
        Commands::Secrets(subcmd) => {
            ensure_auth_and_project(ctx).await?;
            match subcmd {
                SecretsCommands::Ls => commands::secrets::list(ctx).await,
                SecretsCommands::Set { secrets } => commands::secrets::set(ctx, &secrets).await,
                SecretsCommands::Rm { secret_names } => {
                    commands::secrets::unset(ctx, &secret_names).await
                }
            }
        }
        Commands::Applications(app_args) => {
            ensure_auth_and_project(ctx).await?;
            match app_args.command {
                Some(ApplicationsCommands::Ls) | None => commands::applications::ls(ctx).await,
            }
        }
        Commands::Sbx(subcmd) => {
            ensure_auth_and_project(ctx).await?;
            match subcmd {
                SbxCommands::Ls => commands::sbx::ls::run(ctx).await,
                SbxCommands::Stop { sandbox_ids } => {
                    commands::sbx::stop::run(ctx, &sandbox_ids).await
                }
                SbxCommands::New {
                    image,
                    cpus,
                    memory,
                    disk,
                    timeout,
                    entrypoint,
                    snapshot,
                    wait,
                } => {
                    commands::sbx::create::run(
                        ctx,
                        image.as_deref(),
                        cpus,
                        memory,
                        disk,
                        timeout,
                        &entrypoint,
                        snapshot.as_deref(),
                        wait,
                    )
                    .await
                }
                SbxCommands::Exec {
                    sandbox_id,
                    command,
                    args,
                    timeout,
                    workdir,
                    env,
                } => {
                    commands::sbx::exec::run(
                        ctx,
                        &sandbox_id,
                        &command,
                        &args,
                        timeout,
                        workdir.as_deref(),
                        &env,
                    )
                    .await
                }
                SbxCommands::Cp { src, dest } => commands::sbx::cp::run(ctx, &src, &dest).await,
                SbxCommands::Snapshot(snapshot_args) => match snapshot_args.command {
                    Some(SnapshotCommands::Ls) => commands::sbx::snapshot_ls::run(ctx).await,
                    Some(SnapshotCommands::Rm { snapshot_ids }) => {
                        commands::sbx::snapshot_rm::run(ctx, &snapshot_ids).await
                    }
                    None => {
                        let sandbox_id = snapshot_args.sandbox_id.ok_or_else(|| {
                            CliError::usage("snapshot requires a sandbox ID or the 'ls' subcommand")
                        })?;
                        commands::sbx::snapshot::run(ctx, &sandbox_id, snapshot_args.timeout).await
                    }
                },
                SbxCommands::Run {
                    command,
                    args,
                    image,
                    cpus,
                    memory,
                    disk,
                    timeout,
                    workdir,
                    env,
                    keep,
                } => {
                    commands::sbx::run::run(
                        ctx,
                        &command,
                        &args,
                        image.as_deref(),
                        cpus,
                        memory,
                        disk,
                        timeout,
                        workdir.as_deref(),
                        &env,
                        keep,
                    )
                    .await
                }
                SbxCommands::Ssh { sandbox_id, shell } => {
                    commands::sbx::ssh::run(ctx, &sandbox_id, &shell).await
                }
            }
        }
    }
}
