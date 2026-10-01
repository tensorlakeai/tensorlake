//! `tl context`, `--context`, and the lookup order, against the real binary with an isolated
//! HOME. A scripted HTTP server stands in for platform-api where a request is expected.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{Duration, timeout};

const PROD: &str = "https://api.tensorlake.ai";

struct Home {
    _temp: tempfile::TempDir,
    dir: PathBuf,
}

impl Home {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("home");
        fs::create_dir_all(dir.join(".config/tensorlake")).unwrap();
        Home { _temp: temp, dir }
    }

    fn config(&self) -> PathBuf {
        self.dir.join(".config/tensorlake")
    }

    fn write(&self, name: &str, content: &str) {
        fs::write(self.config().join(name), content).unwrap();
    }

    fn read(&self, name: &str) -> String {
        fs::read_to_string(self.config().join(name)).unwrap_or_default()
    }

    fn toml(&self, name: &str) -> toml::Value {
        toml::from_str(&self.read(name)).unwrap()
    }
}

struct Run {
    stdout: String,
    stderr: String,
    success: bool,
    code: Option<i32>,
}

async fn tl(home: &Home, cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> Run {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tl"));
    cmd.args(args)
        .current_dir(cwd)
        .env("HOME", &home.dir)
        .env("NO_COLOR", "1")
        .env_remove("TENSORLAKE_API_KEY")
        .env_remove("TENSORLAKE_PAT")
        .env_remove("TENSORLAKE_API_URL")
        .env_remove("TENSORLAKE_ORGANIZATION_ID")
        .env_remove("TENSORLAKE_PROJECT_ID")
        .env_remove("TENSORLAKE_CONTEXT")
        .env_remove("TENSORLAKE_GIT_TOKEN")
        .kill_on_drop(true);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let output = timeout(Duration::from_secs(30), cmd.output())
        .await
        .expect("CLI must finish within 30 seconds")
        .unwrap();
    Run {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        success: output.status.success(),
        code: output.status.code(),
    }
}

/// Serve scripted responses in order and return the raw requests that arrived.
async fn scripted_server(
    responses: Vec<(u16, Value)>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            let headers = String::from_utf8(request).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            let mut body_bytes = vec![0; length];
            stream.read_exact(&mut body_bytes).await.unwrap();
            requests.push(format!("{headers}{}", String::from_utf8_lossy(&body_bytes)));
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len(),
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    (url, server)
}

fn two_contexts(home: &Home, api_url: &str) {
    home.write(
        "contexts.toml",
        &format!(
            r#"current = "default"

[contexts.default]
api_url = "{api_url}"
organization = "org_1"
project = "project_default"

[contexts.staging]
api_url = "{api_url}"
organization = "org_1"
project = "project_staging"
"#
        ),
    );
    home.write(
        "credentials.toml",
        &format!(
            r#"["{api_url}"]
token = "tl_default"
organization = "org_1"
project = "project_default"

[contexts.default]
token = "tl_default"
parent = true

[contexts.staging]
token = "tl_staging"
parent = false
"#
        ),
    );
}

#[tokio::test]
async fn migration_creates_default_and_keeps_the_old_table() {
    let home = Home::new();
    home.write(
        "credentials.toml",
        r#"["https://api.tensorlake.ai"]
token = "tl_old"
organization = "org_1"
project = "project_1"
"#,
    );
    let run = tl(&home, &home.dir, &["context", "list", "-o", "json"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let list: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(list[0]["name"], "default");
    assert_eq!(list[0]["current"], true);
    assert_eq!(list[0]["project"], "project_1");
    assert_eq!(list[0]["token"], "login");

    // contexts.toml has no secrets.
    let contexts = home.read("contexts.toml");
    assert!(contexts.contains(r#"current = "default""#), "{contexts}");
    assert!(!contexts.contains("tl_old"), "{contexts}");

    // credentials.toml keeps the per-URL table an older CLI reads, and gains the context token.
    let credentials = home.toml("credentials.toml");
    assert_eq!(credentials[PROD]["token"], toml::Value::from("tl_old"));
    assert_eq!(credentials[PROD]["project"], toml::Value::from("project_1"));
    assert_eq!(
        credentials["contexts"]["default"]["token"],
        toml::Value::from("tl_old")
    );
    assert_eq!(
        credentials["contexts"]["default"]["parent"],
        toml::Value::from(true)
    );
}

#[tokio::test]
async fn use_rename_and_delete_update_current() {
    let home = Home::new();
    two_contexts(&home, PROD);

    let run = tl(&home, &home.dir, &["context", "current"], &[]).await;
    assert_eq!(run.stdout.trim(), "default");

    let run = tl(&home, &home.dir, &["context", "use", "staging"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert_eq!(
        home.toml("contexts.toml")["current"],
        toml::Value::from("staging")
    );
    // The per-URL table now holds the staging token, so an older CLI sees it.
    let credentials = home.toml("credentials.toml");
    assert_eq!(credentials[PROD]["token"], toml::Value::from("tl_staging"));
    assert_eq!(
        credentials[PROD]["project"],
        toml::Value::from("project_staging")
    );

    let run = tl(
        &home,
        &home.dir,
        &["context", "rename", "staging", "stage"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    assert_eq!(
        home.toml("contexts.toml")["current"],
        toml::Value::from("stage")
    );
    let credentials = home.toml("credentials.toml");
    assert_eq!(
        credentials["contexts"]["stage"]["token"],
        toml::Value::from("tl_staging")
    );
    assert!(credentials["contexts"].get("staging").is_none());

    // Delete: the server has no revoke route, so the token is removed locally only.
    // The context's API URL is a closed port so the revoke attempt fails fast.
    let run = tl(
        &home,
        &home.dir,
        &["context", "set", "stage", "api_url=http://127.0.0.1:9"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    let run = tl(&home, &home.dir, &["context", "delete", "stage"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert!(run.stderr.contains("could not revoke"), "{}", run.stderr);
    let contexts = home.toml("contexts.toml");
    assert!(contexts.get("current").is_none(), "{contexts}");
    assert!(contexts["contexts"].get("stage").is_none());
    assert!(
        home.toml("credentials.toml")["contexts"]
            .get("stage")
            .is_none()
    );
    assert!(run.stderr.contains("no current context"), "{}", run.stderr);

    let run = tl(&home, &home.dir, &["context", "current"], &[]).await;
    assert!(!run.success);
    assert!(run.stderr.contains("no current context"), "{}", run.stderr);
}

#[tokio::test]
async fn unknown_context_is_a_clear_error() {
    let home = Home::new();
    two_contexts(&home, PROD);

    let run = tl(
        &home,
        &home.dir,
        &["--context", "nope", "secrets", "ls"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr
            .contains("unknown context 'nope' (from --context flag)"),
        "{}",
        run.stderr
    );
    assert!(run.stderr.contains("default, staging"), "{}", run.stderr);

    let run = tl(
        &home,
        &home.dir,
        &["secrets", "ls"],
        &[("TENSORLAKE_CONTEXT", "nope")],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr.contains("(from TENSORLAKE_CONTEXT)"),
        "{}",
        run.stderr
    );

    // Repair commands still run, with a warning.
    let run = tl(
        &home,
        &home.dir,
        &["--context", "nope", "context", "list"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stderr.contains("warning: unknown context 'nope'"),
        "{}",
        run.stderr
    );
}

#[tokio::test]
async fn a_project_with_no_token_says_how_to_get_one() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let run = tl(
        &home,
        &home.dir,
        &["--project", "project_other", "secrets", "ls"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert_eq!(
        run.stderr.trim(),
        "Error: no token for project project_other. run: tl context create <name> --project project_other"
    );
}

#[tokio::test]
async fn bad_id_formats_are_rejected_before_any_request() {
    let home = Home::new();
    let run = tl(
        &home,
        &home.dir,
        &["--organization", "organizations/org_abc", "secrets", "ls"],
        &[],
    )
    .await;
    assert_eq!(run.code, Some(2));
    assert!(
        run.stderr
            .contains("invalid organization ID 'organizations/org_abc'"),
        "{}",
        run.stderr
    );
    assert!(run.stderr.contains("starts with 'org_'"), "{}", run.stderr);

    let run = tl(
        &home,
        &home.dir,
        &["--project", "proj-1", "secrets", "ls"],
        &[],
    )
    .await;
    assert_eq!(run.code, Some(2));
    assert!(
        run.stderr.contains("starts with 'project_'"),
        "{}",
        run.stderr
    );
}

#[tokio::test]
async fn whoami_shows_the_context_and_where_it_came_from() {
    let home = Home::new();
    // A closed port: whoami's name lookup fails fast and is skipped.
    two_contexts(&home, "http://127.0.0.1:9");

    let run = tl(&home, &home.dir, &["whoami", "-o", "json"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "default");
    assert_eq!(body["context"]["source"], "current context");
    assert_eq!(body["personalAccessToken"]["projectId"], "project_default");

    let run = tl(
        &home,
        &home.dir,
        &["whoami", "-o", "json"],
        &[("TENSORLAKE_CONTEXT", "staging")],
    )
    .await;
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "staging");
    assert_eq!(body["context"]["source"], "TENSORLAKE_CONTEXT");
    assert_eq!(body["personalAccessToken"]["projectId"], "project_staging");
    assert!(
        body["personalAccessToken"]["token"]
            .as_str()
            .unwrap()
            .starts_with("tl_staging")
    );

    // The flag beats the env var.
    let run = tl(
        &home,
        &home.dir,
        &["--context", "default", "whoami", "-o", "json"],
        &[("TENSORLAKE_CONTEXT", "staging")],
    )
    .await;
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "default");
    assert_eq!(body["context"]["source"], "--context flag");

    // The local config beats the current context, by name or by scope.
    let project = home.dir.join("project");
    fs::create_dir_all(project.join(".tensorlake")).unwrap();
    fs::write(
        project.join(".tensorlake/config.toml"),
        "context = \"staging\"\n",
    )
    .unwrap();
    let run = tl(&home, &project, &["whoami", "-o", "json"], &[]).await;
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "staging");
    assert_eq!(body["context"]["source"], ".tensorlake/config.toml");

    fs::write(
        project.join(".tensorlake/config.toml"),
        "organization = \"org_1\"\nproject = \"project_staging\"\n",
    )
    .unwrap();
    let run = tl(&home, &project, &["whoami", "-o", "json"], &[]).await;
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["personalAccessToken"]["projectId"], "project_staging");
    // The token comes from the context that matches the project.
    assert_eq!(body["context"]["name"], "staging");
    assert!(
        body["personalAccessToken"]["token"]
            .as_str()
            .unwrap()
            .starts_with("tl_staging")
    );
    // A project flag picks the matching context too.
    let run = tl(
        &home,
        &project,
        &["--project", "project_default", "whoami", "-o", "json"],
        &[],
    )
    .await;
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "default");
    assert!(
        body["personalAccessToken"]["token"]
            .as_str()
            .unwrap()
            .starts_with("tl_default")
    );
}

#[tokio::test]
async fn api_key_with_conflicting_flags_warns() {
    let (url, server) = scripted_server(vec![
        (
            200,
            json!({"id": "key_1", "organizationId": "org_1", "projectId": "project_1"}),
        ),
        (200, json!({"name": "Prod", "organizationName": "Acme"})),
    ])
    .await;
    let home = Home::new();
    let run = tl(
        &home,
        &home.dir,
        &[
            "--api-url",
            &url,
            "--api-key",
            "tl_apiKey_x",
            "--organization",
            "org_1",
            "--project",
            "project_9",
            "whoami",
            "-o",
            "json",
        ],
        &[],
    )
    .await;
    server.await.unwrap();
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stderr.contains("warning: --organization/--project (org_1/project_9) do not match the API key scope (org_1/project_1)"),
        "{}",
        run.stderr
    );
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["apiKey"]["projectId"], "project_1");
}

#[tokio::test]
async fn create_mints_a_token_from_the_login() {
    let (url, server) = scripted_server(vec![(
        200,
        json!({"token": "tl_minted", "organizationId": "org_1", "projectId": "project_new",
               "expiresAt": "2027-01-01T00:00:00Z"}),
    )])
    .await;
    let home = Home::new();
    two_contexts(&home, &url);
    let run = tl(
        &home,
        &home.dir,
        &["context", "create", "new", "--project", "project_new"],
        &[],
    )
    .await;
    let requests = server.await.unwrap();
    assert!(run.success, "{}", run.stderr);
    assert!(
        requests[0].starts_with("POST /platform/cli/tokens/mint "),
        "{}",
        requests[0]
    );
    assert!(
        requests[0].contains("authorization: Bearer tl_default"),
        "{}",
        requests[0]
    );
    assert!(
        requests[0].ends_with(r#"{"projectId":"project_new"}"#),
        "{}",
        requests[0]
    );
    assert_eq!(
        run.stdout.trim(),
        "saved context 'new'. run: tl context use new"
    );

    let contexts = home.toml("contexts.toml");
    assert_eq!(
        contexts["contexts"]["new"]["project"],
        toml::Value::from("project_new")
    );
    assert_eq!(
        contexts["current"],
        toml::Value::from("default"),
        "create does not switch"
    );
    let credentials = home.toml("credentials.toml");
    assert_eq!(
        credentials["contexts"]["new"]["token"],
        toml::Value::from("tl_minted")
    );
    assert_eq!(
        credentials["contexts"]["new"]["parent"],
        toml::Value::from(false)
    );
}

#[tokio::test]
async fn create_falls_back_to_the_browser_when_mint_is_missing() {
    // Mint is 404; the browser login starts, and its start route fails so no browser opens.
    let (url, server) = scripted_server(vec![
        (404, json!({"message": "Not Found"})),
        (500, json!({"message": "test stop"})),
    ])
    .await;
    let home = Home::new();
    two_contexts(&home, &url);
    let run = tl(
        &home,
        &home.dir,
        &["context", "create", "new", "--project", "project_new"],
        &[],
    )
    .await;
    let requests = server.await.unwrap();
    assert!(!run.success);
    assert!(
        requests[0].starts_with("POST /platform/cli/tokens/mint "),
        "{}",
        requests[0]
    );
    assert!(
        requests[1].starts_with("POST /platform/cli/login/start "),
        "{}",
        requests[1]
    );
    assert!(
        run.stderr
            .contains("the server cannot mint tokens yet. using the browser login instead."),
        "{}",
        run.stderr
    );
    assert!(
        home.toml("contexts.toml")["contexts"].get("new").is_none(),
        "nothing saved"
    );
}

#[tokio::test]
async fn create_without_a_login_token_uses_the_browser() {
    let (url, server) = scripted_server(vec![(500, json!({"message": "test stop"}))]).await;
    let home = Home::new();
    let run = tl(
        &home,
        &home.dir,
        &[
            "--api-url",
            &url,
            "context",
            "create",
            "new",
            "--project",
            "project_new",
        ],
        &[],
    )
    .await;
    let requests = server.await.unwrap();
    assert!(!run.success);
    assert!(
        requests[0].starts_with("POST /platform/cli/login/start "),
        "{}",
        requests[0]
    );
    assert!(
        run.stderr.contains("no login token for this organization"),
        "{}",
        run.stderr
    );
}

#[tokio::test]
async fn logout_forgets_the_tokens_of_the_organization() {
    let (url, server) = scripted_server(vec![(404, json!({"message": "Not Found"}))]).await;
    let home = Home::new();
    two_contexts(&home, &url);
    let run = tl(&home, &home.dir, &["logout"], &[]).await;
    let requests = server.await.unwrap();
    assert!(run.success, "{}", run.stderr);
    assert!(
        requests[0].starts_with("POST /platform/cli/tokens/revoke "),
        "{}",
        requests[0]
    );
    assert!(
        requests[0].contains("authorization: Bearer tl_default"),
        "parent token: {}",
        requests[0]
    );
    assert!(
        run.stdout
            .contains("removed the tokens of: default, staging"),
        "{}",
        run.stdout
    );

    let credentials = home.toml("credentials.toml");
    assert!(
        credentials.get(&url).is_none(),
        "per-URL table removed: {credentials}"
    );
    assert!(
        credentials["contexts"]
            .as_table()
            .map(|t| t.is_empty())
            .unwrap_or(true),
        "{credentials}"
    );
    // The contexts stay, without tokens.
    let run = tl(&home, &home.dir, &["context", "list", "-o", "json"], &[]).await;
    let list: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert!(
        list.as_array()
            .unwrap()
            .iter()
            .all(|c| c["token"] == "none"),
        "{list}"
    );
}

#[tokio::test]
async fn set_warns_that_the_token_may_not_match() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let run = tl(
        &home,
        &home.dir,
        &["context", "set", "staging", "project=project_moved"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    assert!(run.stderr.contains("may not match"), "{}", run.stderr);
    assert_eq!(
        home.toml("contexts.toml")["contexts"]["staging"]["project"],
        toml::Value::from("project_moved")
    );
    let run = tl(
        &home,
        &home.dir,
        &["context", "set", "staging", "project=nope"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr.contains("starts with 'project_'"),
        "{}",
        run.stderr
    );

    let run = tl(&home, &home.dir, &["context", "show", "staging"], &[]).await;
    assert!(
        run.stdout.contains("Project      : project_moved"),
        "{}",
        run.stdout
    );
    assert!(
        run.stdout.contains("Token        : minted"),
        "{}",
        run.stdout
    );
}
