use rand::Rng;
use std::env;
use tensorlake::Sdk;

fn env_var(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} must be set"))
}

pub fn create_sdk() -> Result<Sdk, String> {
    let url = env_var("TENSORLAKE_API_URL")?;
    let api_key = env_var("TENSORLAKE_API_KEY")?;

    Sdk::new(&url, &api_key).map_err(|e| format!("failed to create SDK: {e}"))
}

#[allow(dead_code)]
pub fn get_org_and_project_ids() -> Result<(String, String), String> {
    let org_id = env_var("TENSORLAKE_ORGANIZATION_ID")?;
    let project_id = env_var("TENSORLAKE_PROJECT_ID")?;

    Ok((org_id, project_id))
}

pub fn random_string() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    let mut rng = rand::rng();
    let length = rng.random_range(5..=10);

    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}
