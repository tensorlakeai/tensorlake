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
