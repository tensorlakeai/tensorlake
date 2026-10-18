use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
};

#[tokio::test]
async fn mount_snapshot_cli_replay_preserves_the_original_mount() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let replaced = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let server = tokio::spawn({
            let replaced = replaced.clone();
            let requests = requests.clone();
            async move {
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut header = Vec::new();
                    let mut byte = [0];
                    while !header.ends_with(b"\r\n\r\n") {
                        assert!(header.len() < 16384);
                        stream.read_exact(&mut byte).await.unwrap();
                        header.push(byte[0]);
                    }
                    let header = String::from_utf8(header).unwrap();
                    assert!(header.starts_with("POST /api/v1/filesystems/control "));
                    let length = header.lines().find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    }).unwrap();
                    assert!(length < 16384);
                    let mut body = vec![0; length];
                    stream.read_exact(&mut body).await.unwrap();
                    let request: Value = serde_json::from_slice(&body).unwrap();
                    requests.lock().unwrap().push(request.clone());
                    let mount = if replaced.load(Ordering::SeqCst) { "replacement" } else { "original" };
                    let response = match request["operation"].as_str().unwrap() {
                        "status" => json!({"ok":true,"mount_id":mount}),
                        "snapshot_start" => {
                            assert_eq!(request["mount_id"], mount);
                            json!({"ok":true,"mount_id":mount,"request_id":request["request_id"],
                                "phase":"completed","snapshot_id":format!("snapshot-{mount}")})
                        }
                        other => panic!("unexpected request {other}"),
                    }.to_string();
                    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
                }
            }
        });
        let directory = tempfile::tempdir().unwrap();
        let run = |args: Vec<String>| {
            let mut command = Command::new(env!("CARGO_BIN_EXE_tl"));
            command.args(["--api-url", &format!("http://{address}"), "--pat", "fixture-only",
                "--organization", "test-org", "--project", "test-project",
                "sbx", "fs", "snapshot", "sandbox", "--path", "/work", "--json"])
                .args(args).current_dir(directory.path())
                .env("TENSORLAKE_SANDBOX_PROXY_URL", format!("http://{address}"))
                .env_remove("TENSORLAKE_API_KEY").env_remove("TENSORLAKE_PAT")
                .env_remove("TENSORLAKE_ORGANIZATION_ID").env_remove("TENSORLAKE_PROJECT_ID")
                .kill_on_drop(true);
            command
        };
        let first = run(vec![]).output().await.unwrap();
        assert!(first.status.success(), "{}", String::from_utf8_lossy(&first.stderr));
        let receipt: Value = serde_json::from_slice(&first.stdout).unwrap();
        let id = receipt["request_id"].as_str().unwrap();
        assert!(String::from_utf8_lossy(&first.stderr).contains("--mount-id original"));
        let replay = vec!["--request-id".into(), id.into(), "--mount-id".into(), "original".into()];
        let same = run(replay.clone()).output().await.unwrap();
        assert!(same.status.success(), "{}", String::from_utf8_lossy(&same.stderr));
        assert_eq!(serde_json::from_slice::<Value>(&same.stdout).unwrap(), receipt);
        replaced.store(true, Ordering::SeqCst);
        let different = run(replay).output().await.unwrap();
        assert!(!different.status.success());
        assert!(String::from_utf8_lossy(&different.stderr).contains("mount_id changed"));
        let missing_identity = run(vec!["--request-id".into(), id.into()]).output().await.unwrap();
        assert!(!missing_identity.status.success());
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 5, "replacement must not accept a new capture");
        assert_eq!(requests.iter().filter(|request| request["operation"] == "snapshot_start").count(), 2);
        server.abort();
    }).await.expect("CLI replay fixture must finish within 20 seconds");
}
