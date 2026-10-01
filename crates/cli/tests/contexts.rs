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

    /// Write a config file as the CLI would: private to the user.
    fn write(&self, name: &str, content: &str) {
        let path = self.config().join(name);
        fs::write(&path, content).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
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

[contexts.staging]
token = "tl_staging"
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
    assert_eq!(list[0]["token"], "saved");

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

    // credentials.toml is private to the user.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(home.config().join("credentials.toml"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "mode {mode:04o}");
    }
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

    let run = tl(&home, &home.dir, &["context", "delete", "stage"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let contexts = home.toml("contexts.toml");
    assert!(contexts.get("current").is_none(), "{contexts}");
    assert!(contexts["contexts"].get("stage").is_none());
    let credentials = home.toml("credentials.toml");
    assert!(credentials["contexts"].get("stage").is_none());
    assert!(
        credentials.get(PROD).is_none(),
        "the per-URL copy of the deleted token goes too: {credentials}"
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
}

/// A stale `TENSORLAKE_CONTEXT` must not block the commands that repair the contexts.
#[tokio::test]
async fn recovery_commands_run_with_an_unknown_context_named() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let stale = [("TENSORLAKE_CONTEXT", "gone")];

    let run = tl(&home, &home.dir, &["context", "list", "-o", "json"], &stale).await;
    assert!(run.success, "{}", run.stderr);
    let list: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(list.as_array().unwrap().len(), 2);

    let run = tl(
        &home,
        &home.dir,
        &["--context", "gone", "context", "current"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    assert_eq!(run.stdout.trim(), "default");

    let run = tl(&home, &home.dir, &["version"], &stale).await;
    assert!(run.success, "{}", run.stderr);

    let run = tl(&home, &home.dir, &["logout", "--all"], &stale).await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stdout
            .contains("removed the tokens of: default, staging"),
        "{}",
        run.stdout
    );

    // A single logout runs in the named context, so it still needs that context.
    let run = tl(&home, &home.dir, &["logout"], &stale).await;
    assert!(!run.success);
    assert!(
        run.stderr
            .contains("unknown context 'gone' (from TENSORLAKE_CONTEXT)"),
        "{}",
        run.stderr
    );
}

/// `tl login` with an unknown context named reaches the browser login, instead of failing
/// before it starts. The login start route fails, so no browser opens.
#[tokio::test]
async fn login_runs_with_an_unknown_context_named() {
    let (url, server) = scripted_server(vec![(500, json!({"message": "test stop"}))]).await;
    let home = Home::new();
    two_contexts(&home, &url);
    let run = tl(
        &home,
        &home.dir,
        &["login"],
        &[("TENSORLAKE_CONTEXT", "gone")],
    )
    .await;
    let requests = server.await.unwrap();
    assert!(!run.success);
    assert!(
        !run.stderr.contains("unknown context"),
        "the missing context must not block the login: {}",
        run.stderr
    );
    assert_eq!(requests.len(), 1, "the browser login talked to the server");
    assert!(
        requests[0].starts_with("POST /platform/cli/login/start "),
        "{}",
        requests[0]
    );
}

/// A login to another API URL replaces the context but leaves the old URL's table behind.
/// `tl logout --all` removes that too, so no saved token survives it.
#[tokio::test]
async fn logout_all_removes_a_per_url_login_with_no_context() {
    let home = Home::new();
    home.write(
        "contexts.toml",
        r#"current = "default"

[contexts.default]
api_url = "https://api.b.example"
organization = "org_1"
project = "project_b"
"#,
    );
    home.write(
        "credentials.toml",
        r#"["https://api.a.example"]
token = "tl_a"
organization = "org_1"
project = "project_a"

["https://api.b.example"]
token = "tl_b"
organization = "org_1"
project = "project_b"

[contexts.default]
token = "tl_b"
"#,
    );

    let run = tl(&home, &home.dir, &["logout", "--all"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stdout.contains("removed the tokens of: default"),
        "{}",
        run.stdout
    );
    assert!(
        run.stderr
            .contains("also removed the saved login with no context for: https://api.a.example"),
        "{}",
        run.stderr
    );
    let credentials = home.toml("credentials.toml");
    assert!(
        credentials.get("https://api.a.example").is_none(),
        "the orphaned per-URL table is gone: {credentials}"
    );
    assert!(
        credentials.get("https://api.b.example").is_none(),
        "{credentials}"
    );

    // Nothing authenticates against the old URL any more.
    let run = tl(
        &home,
        &home.dir,
        &["--api-url", "https://api.a.example", "whoami", "-o", "json"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["authenticated"], false, "{body}");
}

#[tokio::test]
async fn an_api_key_wins_over_the_context_token() {
    let (url, server) = scripted_server(vec![(500, json!({"message": "test stop"}))]).await;
    let home = Home::new();
    two_contexts(&home, &url);

    let run = tl(
        &home,
        &home.dir,
        &["secrets", "ls"],
        &[("TENSORLAKE_API_KEY", "tl_apiKey_ci")],
    )
    .await;
    let requests = server.await.unwrap();
    assert!(
        requests[0].contains("Bearer tl_apiKey_ci"),
        "{}",
        requests[0]
    );
    assert!(run.stderr.contains("HTTP 500"), "{}", run.stderr);
}

#[tokio::test]
async fn a_broken_contexts_file_is_never_overwritten() {
    let home = Home::new();
    // A typo: the table header is missing its closing bracket.
    let broken = "current = \"default\"\n\n[contexts.default\napi_url = \"https://api.tensorlake.ai\"\n\n[contexts.staging]\napi_url = \"https://api.tensorlake.ai\"\n";
    home.write("contexts.toml", broken);

    // Commands that write the file stop with a clear error and leave it alone.
    for args in [
        vec!["context", "use", "staging"],
        vec!["context", "rename", "staging", "stage"],
        vec!["context", "delete", "staging"],
    ] {
        let run = tl(&home, &home.dir, &args, &[]).await;
        assert!(!run.success, "{args:?}: {}", run.stdout);
        assert!(
            run.stderr.contains("contexts.toml does not parse"),
            "{args:?}: {}",
            run.stderr
        );
        assert!(
            run.stderr.contains("fix the file or move it away"),
            "{args:?}: {}",
            run.stderr
        );
        assert_eq!(home.read("contexts.toml"), broken, "{args:?}");
    }

    // Commands that only read warn once and go on as if there were no contexts.
    let run = tl(&home, &home.dir, &["context", "list"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert_eq!(
        run.stderr.matches("contexts.toml does not parse").count(),
        1,
        "{}",
        run.stderr
    );
    assert!(run.stderr.contains("no contexts"), "{}", run.stderr);
    assert_eq!(home.read("contexts.toml"), broken);
}

#[tokio::test]
async fn a_project_flag_for_another_project_is_an_error() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let run = tl(
        &home,
        &home.dir,
        &["--project", "project_staging", "secrets", "ls"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr.starts_with(
            "Error: --project project_staging does not match context 'default' (project_default)."
        ),
        "{}",
        run.stderr
    );
    assert!(
        run.stderr.contains("tl context use <name>"),
        "{}",
        run.stderr
    );

    // The same project with its own context is fine. The server is a closed port, so the
    // command fails after configuration, not during it.
    let run = tl(
        &home,
        &home.dir,
        &[
            "--context",
            "staging",
            "--project",
            "project_staging",
            "--api-url",
            "http://127.0.0.1:9",
            "whoami",
            "-o",
            "json",
        ],
        &[],
    )
    .await;
    assert!(
        !run.stderr.contains("does not match context"),
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

    // The context beats the organization and project in the local config.
    let project = home.dir.join("project");
    fs::create_dir_all(project.join(".tensorlake")).unwrap();
    fs::write(
        project.join(".tensorlake/config.toml"),
        "organization = \"org_1\"\nproject = \"project_staging\"\n",
    )
    .unwrap();
    let run = tl(&home, &project, &["whoami", "-o", "json"], &[]).await;
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "default");
    assert_eq!(body["personalAccessToken"]["projectId"], "project_default");
}

#[tokio::test]
async fn login_with_a_context_name_uses_the_browser_login() {
    // The login start route fails, so no browser opens and nothing is saved.
    let (url, server) = scripted_server(vec![(500, json!({"message": "test stop"}))]).await;
    let home = Home::new();
    two_contexts(&home, &url);
    let run = tl(&home, &home.dir, &["login", "--context", "new"], &[]).await;
    let requests = server.await.unwrap();
    assert!(!run.success);
    assert_eq!(
        requests.len(),
        1,
        "only the browser login talks to the server"
    );
    assert!(
        requests[0].starts_with("POST /platform/cli/login/start "),
        "{}",
        requests[0]
    );
    assert!(
        !requests[0].contains("authorization:"),
        "no saved token is sent to the login: {}",
        requests[0]
    );
    assert!(
        home.toml("contexts.toml")["contexts"].get("new").is_none(),
        "nothing saved"
    );
    assert_eq!(
        home.toml("contexts.toml")["current"],
        toml::Value::from("default"),
        "the current context is unchanged"
    );
}

#[tokio::test]
async fn logout_forgets_the_current_context_only() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let run = tl(&home, &home.dir, &["logout"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stdout.contains("removed the tokens of: default"),
        "{}",
        run.stdout
    );

    let credentials = home.toml("credentials.toml");
    assert!(
        credentials.get(PROD).is_none(),
        "per-URL table removed: {credentials}"
    );
    assert!(
        credentials["contexts"].get("default").is_none(),
        "{credentials}"
    );
    assert_eq!(
        credentials["contexts"]["staging"]["token"],
        toml::Value::from("tl_staging"),
        "the other context keeps its token"
    );
    // The context stays, without a token.
    let run = tl(&home, &home.dir, &["context", "list", "-o", "json"], &[]).await;
    let list: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(list[0]["name"], "default");
    assert_eq!(list[0]["token"], "none");
    assert_eq!(list[1]["name"], "staging");
    assert_eq!(list[1]["token"], "saved");
}

#[tokio::test]
async fn logout_of_another_context_uses_the_context_flag() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let run = tl(&home, &home.dir, &["--context", "staging", "logout"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let credentials = home.toml("credentials.toml");
    assert!(
        credentials["contexts"].get("staging").is_none(),
        "{credentials}"
    );
    assert_eq!(
        credentials[PROD]["token"],
        toml::Value::from("tl_default"),
        "the per-URL table holds the current context's token, which stays"
    );
}

#[tokio::test]
async fn logout_all_forgets_every_saved_token() {
    let home = Home::new();
    two_contexts(&home, PROD);

    let run = tl(&home, &home.dir, &["logout", "--all"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stdout
            .contains("removed the tokens of: default, staging"),
        "{}",
        run.stdout
    );
    let credentials = home.toml("credentials.toml");
    assert!(
        credentials.get(PROD).is_none(),
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

/// A `credentials.toml` from before per-URL tables: one unscoped token at the top.
fn legacy_credentials(home: &Home) {
    home.write("credentials.toml", "token = \"tl_legacy\"\n");
}

#[tokio::test]
async fn logout_removes_the_legacy_unscoped_token() {
    let home = Home::new();
    legacy_credentials(&home);
    // The first command migrates the legacy token into context `default`.
    let run = tl(&home, &home.dir, &["context", "list"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert_eq!(
        home.toml("credentials.toml")["contexts"]["default"]["token"],
        toml::Value::from("tl_legacy")
    );

    let run = tl(&home, &home.dir, &["logout"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let credentials = home.read("credentials.toml");
    assert!(
        !credentials.contains("tl_legacy"),
        "the legacy token must not survive logout: {credentials}"
    );
    let run = tl(&home, &home.dir, &["context", "list", "-o", "json"], &[]).await;
    let list: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(list[0]["token"], "none");
}

#[tokio::test]
async fn delete_removes_the_legacy_unscoped_token() {
    let home = Home::new();
    legacy_credentials(&home);
    let run = tl(&home, &home.dir, &["context", "list"], &[]).await;
    assert!(run.success, "{}", run.stderr);

    let run = tl(&home, &home.dir, &["context", "delete", "default"], &[]).await;
    assert!(run.success, "{}", run.stderr);

    let credentials = home.read("credentials.toml");
    assert!(
        !credentials.contains("tl_legacy"),
        "the legacy token must not survive deletion: {credentials}"
    );
    assert!(
        home.toml("contexts.toml")["contexts"]
            .get("default")
            .is_none()
    );
}
