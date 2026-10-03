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
        // Never the keychain of the machine that runs the tests.
        .env("TENSORLAKE_TOKEN_STORAGE", "file")
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
async fn migration_creates_default_and_removes_the_old_table() {
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
    assert_eq!(list[0]["token_status"], "saved");

    // contexts.toml has no secrets, and says where the token is.
    let contexts = home.read("contexts.toml");
    assert!(contexts.contains(r#"current = "default""#), "{contexts}");
    assert!(!contexts.contains("tl_old"), "{contexts}");
    assert_eq!(
        home.toml("contexts.toml")["contexts"]["default"]["storage"],
        toml::Value::from("file")
    );
    assert_eq!(list[0]["storage"], "file");

    // credentials.toml holds the context token only. The per-URL table was a copy of it.
    let credentials = home.toml("credentials.toml");
    assert!(credentials.get(PROD).is_none(), "{credentials}");
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
    // A switch writes no token anywhere. The per-URL copy an older CLI wrote is gone.
    let credentials = home.toml("credentials.toml");
    assert!(credentials.get(PROD).is_none(), "{credentials}");
    assert_eq!(
        credentials["contexts"]["staging"]["token"],
        toml::Value::from("tl_staging")
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
    // A closed port: the second command fails after configuration, not during it.
    two_contexts(&home, "http://127.0.0.1:9");
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

    // The same project with its own context is fine.
    let run = tl(
        &home,
        &home.dir,
        &[
            "--context",
            "staging",
            "--project",
            "project_staging",
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
    assert_eq!(list[0]["token_status"], "none");
    assert_eq!(list[1]["name"], "staging");
    assert_eq!(list[1]["token_status"], "saved");
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
        credentials["contexts"]["default"]["token"],
        toml::Value::from("tl_default"),
        "the current context keeps its token"
    );
}

#[tokio::test]
async fn an_invalid_storage_setting_is_an_error() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let run = tl(
        &home,
        &home.dir,
        &["context", "use", "staging"],
        &[("TENSORLAKE_TOKEN_STORAGE", "cloud")],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr
            .contains("TENSORLAKE_TOKEN_STORAGE=cloud is not valid"),
        "{}",
        run.stderr
    );

    // A read-only command must not read the setting as "no contexts" and say "not logged
    // in": the token is there, the setting is wrong.
    let run = tl(
        &home,
        &home.dir,
        &["whoami"],
        &[("TENSORLAKE_TOKEN_STORAGE", "cloud")],
    )
    .await;
    assert!(!run.success, "{}", run.stdout);
    assert!(
        run.stderr
            .contains("TENSORLAKE_TOKEN_STORAGE=cloud is not valid"),
        "{}",
        run.stderr
    );
    assert!(!run.stderr.contains("not logged in"), "{}", run.stderr);
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
        credentials
            .get("contexts")
            .and_then(|t| t.as_table())
            .is_none_or(|t| t.is_empty()),
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
            .all(|c| c["token_status"] == "none"),
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
    assert_eq!(list[0]["token_status"], "none");
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

#[tokio::test]
async fn a_named_context_for_another_api_url_is_an_error() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let other = "http://127.0.0.1:9";

    let run = tl(
        &home,
        &home.dir,
        &["--context", "staging", "--api-url", other, "secrets", "ls"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr.starts_with(&format!(
            "Error: context 'staging' is for {PROD} but the API URL of this run is {other}."
        )),
        "{}",
        run.stderr
    );

    let run = tl(
        &home,
        &home.dir,
        &["secrets", "ls"],
        &[
            ("TENSORLAKE_CONTEXT", "staging"),
            ("TENSORLAKE_API_URL", other),
        ],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr.contains("drop TENSORLAKE_CONTEXT"),
        "{}",
        run.stderr
    );

    // The `current` context for another URL is skipped, as before, not an error.
    let run = tl(
        &home,
        &home.dir,
        &["--api-url", other, "whoami", "-o", "json"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert!(body["context"].is_null(), "{}", run.stdout);

    // The context commands still work with the mismatch, so the user can fix it.
    let run = tl(
        &home,
        &home.dir,
        &[
            "--context",
            "staging",
            "--api-url",
            other,
            "context",
            "current",
        ],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
}

#[tokio::test]
async fn a_broken_credentials_toml_is_an_error_and_is_left_alone() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let broken = "[contexts.default\ntoken = \"tl_default\"\n";
    home.write("credentials.toml", broken);

    // Reads warn and go on without a token.
    let run = tl(&home, &home.dir, &["context", "list", "-o", "json"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stderr.contains("credentials.toml does not parse"),
        "{}",
        run.stderr
    );

    // A write stops instead of saving over the file.
    let run = tl(
        &home,
        &home.dir,
        &["context", "rename", "staging", "stage"],
        &[],
    )
    .await;
    assert!(!run.success, "{}", run.stderr);
    assert!(
        run.stderr.contains("credentials.toml does not parse"),
        "{}",
        run.stderr
    );
    assert_eq!(home.read("credentials.toml"), broken);
}

#[tokio::test]
async fn parallel_commands_keep_every_token() {
    let home = Home::new();
    two_contexts(&home, PROD);

    // The first run of each process moves the tokens and rewrites `credentials.toml` and
    // `contexts.toml`. Run many at once.
    let home = &home;
    let runs = (0..8).map(|i| async move {
        let name = if i % 2 == 0 { "default" } else { "staging" };
        tl(home, &home.dir, &["context", "use", name], &[]).await
    });
    for run in futures::future::join_all(runs).await {
        assert!(run.success, "{}", run.stderr);
    }

    let credentials = home.toml("credentials.toml");
    assert_eq!(
        credentials["contexts"]["default"]["token"],
        toml::Value::from("tl_default")
    );
    assert_eq!(
        credentials["contexts"]["staging"]["token"],
        toml::Value::from("tl_staging")
    );
}

/// Like [`tl`], with `stdin` written to the command, as git writes a credential request.
async fn tl_with_stdin(home: &Home, args: &[&str], stdin: &str) -> Run {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tl"));
    cmd.args(args)
        .current_dir(&home.dir)
        .env("HOME", &home.dir)
        .env("NO_COLOR", "1")
        .env("TENSORLAKE_TOKEN_STORAGE", "file")
        .env_remove("TENSORLAKE_CONTEXT")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn().unwrap();
    let mut pipe = child.stdin.take().unwrap();
    let input = stdin.to_string();
    tokio::spawn(async move {
        let _ = pipe.write_all(input.as_bytes()).await;
    });
    let output = timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("CLI must finish within 30 seconds")
        .unwrap();
    Run {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        success: output.status.success(),
    }
}

/// `--all` means every saved token, so a context token whose context is no longer in
/// `contexts.toml` goes too.
#[tokio::test]
async fn logout_all_removes_a_token_whose_context_is_not_listed() {
    let home = Home::new();
    two_contexts(&home, PROD);
    home.write(
        "credentials.toml",
        &format!(
            r#"["{PROD}"]
token = "tl_default"

[contexts.default]
token = "tl_default"

[contexts.staging]
token = "tl_staging"

[contexts.removed-by-hand]
token = "tl_orphan"
"#
        ),
    );

    let run = tl(&home, &home.dir, &["logout", "--all"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    assert!(
        run.stdout
            .contains("removed the tokens of: default, staging"),
        "{}",
        run.stdout
    );
    assert!(
        run.stderr.contains(
            "also removed the token of a context that contexts.toml does not list: removed-by-hand"
        ),
        "{}",
        run.stderr
    );
    let credentials = home.read("credentials.toml");
    assert!(!credentials.contains("tl_orphan"), "{credentials}");
    assert!(!credentials.contains("tl_staging"), "{credentials}");
}

/// A local command uses no token and no project, so a stale `TENSORLAKE_CONTEXT` or an
/// exported `TENSORLAKE_PROJECT_ID` for another project must not stop it.
#[tokio::test]
async fn local_commands_run_with_a_stale_context_or_another_project() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let stale = [
        ("TENSORLAKE_CONTEXT", "gone"),
        ("TENSORLAKE_PROJECT_ID", "project_other"),
    ];

    let run = tl(&home, &home.dir, &["new", "demo"], &stale).await;
    assert!(run.success, "{}", run.stderr);
    assert!(home.dir.join("demo").is_dir(), "the app was scaffolded");

    // A command that runs in the context still needs it.
    let run = tl(&home, &home.dir, &["secrets", "ls"], &stale).await;
    assert!(!run.success);
    assert!(
        run.stderr
            .contains("unknown context 'gone' (from TENSORLAKE_CONTEXT)"),
        "{}",
        run.stderr
    );
}

/// Git runs the helper on every fetch and push. When the context baked into the helper line
/// is gone, the helper says why and exits 0, so git falls through to a prompt instead of
/// failing the fetch with a `tl` error.
#[tokio::test]
async fn credential_helper_fails_softly_when_its_context_is_gone() {
    let home = Home::new();
    two_contexts(&home, PROD);
    let request = "protocol=https\nhost=git.tensorlake.ai\npath=project_1/demo\n\n";

    let run = tl_with_stdin(
        &home,
        &[
            "--context",
            "gone",
            "--organization",
            "org_1",
            "git",
            "credential-helper",
            "get",
        ],
        request,
    )
    .await;
    assert!(run.success, "exit 0 so git falls through: {}", run.stderr);
    assert_eq!(run.stdout, "", "no protocol lines on stdout");
    assert!(
        run.stderr.contains("unknown context 'gone'") && run.stderr.contains("tl git setup"),
        "{}",
        run.stderr
    );

    // Any other command still fails loudly.
    let run = tl(
        &home,
        &home.dir,
        &["--context", "gone", "secrets", "ls"],
        &[],
    )
    .await;
    assert!(!run.success);
}

/// The Windows keychain does not tell `staging` from `STAGING`, so such names are refused
/// everywhere, before a browser opens or a token moves.
#[tokio::test]
async fn a_name_that_differs_only_by_case_is_refused() {
    let home = Home::new();
    two_contexts(&home, PROD);
    // The first run records where each token is. Take the baseline after that.
    let run = tl(&home, &home.dir, &["context", "list"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let before = home.read("contexts.toml");

    let run = tl(&home, &home.dir, &["login", "--context", "Default"], &[]).await;
    assert!(!run.success);
    assert!(
        run.stderr.contains(
            "context name 'Default' differs from the saved context 'default' only by case"
        ) && run.stderr.contains("use 'default'"),
        "{}",
        run.stderr
    );

    let run = tl(
        &home,
        &home.dir,
        &["context", "rename", "staging", "STAGING"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr
            .contains("cannot rename context 'staging' to 'STAGING'")
            && run.stderr.contains("differ only by case"),
        "{}",
        run.stderr
    );

    let run = tl(
        &home,
        &home.dir,
        &["context", "rename", "staging", "DEFAULT"],
        &[],
    )
    .await;
    assert!(!run.success);
    assert!(
        run.stderr
            .contains("differs from the saved context 'default' only by case"),
        "{}",
        run.stderr
    );

    assert_eq!(home.read("contexts.toml"), before, "nothing changed");
    let credentials = home.toml("credentials.toml");
    assert_eq!(
        credentials["contexts"]["staging"]["token"],
        toml::Value::from("tl_staging")
    );
}

/// An empty `TENSORLAKE_CONTEXT` names nothing. `tl login` must not try to save under ''.
#[tokio::test]
async fn an_empty_context_env_var_does_not_block_login() {
    let (url, server) = scripted_server(vec![(500, json!({"message": "test stop"}))]).await;
    let home = Home::new();
    two_contexts(&home, &url);
    let run = tl(&home, &home.dir, &["login"], &[("TENSORLAKE_CONTEXT", "")]).await;
    let requests = timeout(Duration::from_secs(10), server)
        .await
        .unwrap_or_else(|_| panic!("the login never reached the server: {}", run.stderr))
        .unwrap();
    assert!(!run.success);
    assert!(
        !run.stderr.contains("invalid context name"),
        "{}",
        run.stderr
    );
    assert_eq!(requests.len(), 1, "the browser login talked to the server");
    assert!(
        requests[0].starts_with("POST /platform/cli/login/start "),
        "{}",
        requests[0]
    );
}

/// A context with no organization and project runs the init flow first. The run stays in
/// that context afterwards: its token is used, not the current context's.
#[tokio::test]
async fn after_init_the_run_stays_in_its_context() {
    let (url, server) = scripted_server(vec![
        (200, json!({"items": [{"id": "org_1", "name": "Org"}]})),
        (
            200,
            json!({"items": [{"id": "project_new", "name": "New"}]}),
        ),
        (500, json!({"message": "test stop"})),
    ])
    .await;
    let home = Home::new();
    home.write(
        "contexts.toml",
        &format!(
            r#"current = "default"

[contexts.default]
api_url = "{url}"
organization = "org_1"
project = "project_default"

[contexts.staging]
api_url = "{url}"
"#
        ),
    );
    home.write(
        "credentials.toml",
        &format!(
            r#"["{url}"]
token = "tl_default"

[contexts.default]
token = "tl_default"

[contexts.staging]
token = "tl_staging"
"#
        ),
    );
    // A directory with no `.tensorlake/config.toml`, so init has to ask the server.
    let project = home.dir.join("project");
    fs::create_dir_all(&project).unwrap();

    let run = tl(
        &home,
        &project,
        &["--context", "staging", "secrets", "ls"],
        &[],
    )
    .await;
    let requests = timeout(Duration::from_secs(10), server)
        .await
        .unwrap_or_else(|_| panic!("the command never reached the server: {}", run.stderr))
        .unwrap();
    assert_eq!(requests.len(), 3, "{}", run.stderr);
    assert!(
        requests[0].contains("/platform/v1/organizations ") && requests[0].contains("tl_staging"),
        "init uses the token of the named context: {}",
        requests[0]
    );
    assert!(
        requests[2].contains("Bearer tl_staging"),
        "the command runs with the token of the named context: {}",
        requests[2]
    );
    assert!(
        requests[2].contains("project_new"),
        "the command runs in the project init chose: {}",
        requests[2]
    );
}

#[tokio::test]
async fn init_does_not_use_the_token_of_another_context() {
    // One scripted answer: if init calls the server, the test can tell.
    let (url, server) = scripted_server(vec![(
        200,
        json!({"items": [{"id": "org_1", "name": "Org"}]}),
    )])
    .await;
    let home = Home::new();
    home.write(
        "contexts.toml",
        &format!(
            r#"current = "default"

[contexts.default]
api_url = "{url}"
organization = "org_1"
project = "project_default"

[contexts.staging]
api_url = "{url}"
"#
        ),
    );
    // `staging` has no token (as after `tl --context staging logout`). The per-URL table
    // holds the token of `default`.
    home.write(
        "credentials.toml",
        &format!(
            r#"["{url}"]
token = "tl_default"

[contexts.default]
token = "tl_default"
"#
        ),
    );
    let project = home.dir.join("project");
    fs::create_dir_all(&project).unwrap();

    let run = tl(
        &home,
        &project,
        &["--context", "staging", "init", "--no-confirm"],
        &[],
    )
    .await;
    assert!(!run.success, "{}", run.stdout);
    assert!(
        run.stderr.contains("context 'staging' has no token")
            && run.stderr.contains("tl login --context staging"),
        "{}",
        run.stderr
    );
    assert!(
        !project.join(".tensorlake/config.toml").exists(),
        "init must not write a project chosen with another context's token"
    );
    assert!(
        timeout(Duration::from_millis(500), server).await.is_err(),
        "init must not call the server with the token of another context"
    );
}

#[tokio::test]
async fn after_migration_api_url_finds_the_login_of_another_context() {
    let home = Home::new();
    // A closed port: whoami's name lookup fails fast and is skipped.
    let other = "http://127.0.0.1:9";
    // Two logins from before contexts: production and another server.
    home.write(
        "credentials.toml",
        &format!(
            r#"["{PROD}"]
token = "tl_prod"
organization = "org_1"
project = "project_prod"

["{other}"]
token = "tl_other"
organization = "org_2"
project = "project_other"
"#
        ),
    );

    // Before the upgrade, `--api-url <other>` used the login for that server. The upgrade
    // moves that login into a context that is not current. `--api-url` must still find it.
    let run = tl(
        &home,
        &home.dir,
        &["--api-url", other, "whoami", "-o", "json"],
        &[],
    )
    .await;
    assert!(run.success, "{}", run.stderr);
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "127-0-0-1", "{}", run.stdout);
    assert!(
        body["personalAccessToken"]["token"]
            .as_str()
            .unwrap()
            .starts_with("tl_other"),
        "{}",
        run.stdout
    );
    assert_eq!(body["personalAccessToken"]["projectId"], "project_other");

    // The old per-URL tables are gone; the tokens live with their contexts.
    let credentials = home.toml("credentials.toml");
    assert!(credentials.get(PROD).is_none(), "{credentials}");
    assert!(credentials.get(other).is_none(), "{credentials}");

    // Without `--api-url`, the current context (production) is used, as before.
    let run = tl(&home, &home.dir, &["whoami", "-o", "json"], &[]).await;
    assert!(run.success, "{}", run.stderr);
    let body: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(body["context"]["name"], "default");
    assert!(
        body["personalAccessToken"]["token"]
            .as_str()
            .unwrap()
            .starts_with("tl_prod"),
        "{}",
        run.stdout
    );
}
