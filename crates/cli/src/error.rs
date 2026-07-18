use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),

    #[error("{0}")]
    Auth(String),

    #[error("{0}")]
    Config(String),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Json(#[from] serde_json::Error),

    #[error("{0}")]
    Toml(#[from] toml::de::Error),

    #[error("{0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("{0}")]
    Sdk(#[from] tensorlake::error::SdkError),

    #[error("{0}")]
    Other(#[from] anyhow::Error),

    #[error("Operation cancelled")]
    Cancelled,

    #[error("Process exited with code {0}")]
    ExitCode(i32),
}

impl CliError {
    pub fn usage(msg: impl Into<String>) -> Self {
        Self::Usage(msg.into())
    }

    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Auth(msg.into())
    }

    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, CliError>;

#[cfg(feature = "mount")]
impl From<gsvc_fs_client::CliError> for CliError {
    fn from(error: gsvc_fs_client::CliError) -> Self {
        match error {
            gsvc_fs_client::CliError::Usage(message) => Self::Usage(message),
            gsvc_fs_client::CliError::Auth(message) => Self::Auth(message),
            gsvc_fs_client::CliError::Config(message) => Self::Config(message),
            gsvc_fs_client::CliError::Http(error) => Self::Http(error),
            gsvc_fs_client::CliError::Io(error) => Self::Io(error),
            gsvc_fs_client::CliError::Json(error) => Self::Json(error),
            gsvc_fs_client::CliError::Toml(error) => Self::Toml(error),
            gsvc_fs_client::CliError::TomlSer(error) => Self::TomlSer(error),
            gsvc_fs_client::CliError::Sdk(error) => Self::Sdk(error),
            gsvc_fs_client::CliError::Other(error) => Self::Other(error),
            gsvc_fs_client::CliError::Cancelled => Self::Cancelled,
            gsvc_fs_client::CliError::ExitCode(code) => Self::ExitCode(code),
        }
    }
}
