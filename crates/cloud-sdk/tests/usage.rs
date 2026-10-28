use tensorlake::{
    ClientBuilder,
    usage::{SandboxUsageRequest, UsageClient, UsageWindow, models::UsageChargeBasis},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PAGE_ONE: &str = r#"{
  "windowStart": "2026-10-01T00:00:00.000Z",
  "windowEnd": "2026-10-28T12:00:00.000Z",
  "attributedUsageCharge": {"currency": "usd", "amountMinor": "1234", "basis": "current_price_estimate"},
  "includedUsage": null,
  "attributedUsageValue": null,
  "projects": [
    {"project": {"projectId": "proj-1", "projectName": "Default"}, "sandboxCount": 2, "attributedAmountMinor": "1234", "usageValueAmountMinor": null}
  ],
  "sandboxes": [
    {
      "sandboxId": "sb-1",
      "project": {"projectId": "proj-1", "projectName": "Default"},
      "attributedAmountMinor": "1000",
      "usageValueAmountMinor": null,
      "resources": {
        "cpu": {"quantity": "1.5", "unit": "vCPU-hour", "attributedAmountMinor": "900", "usageValueAmountMinor": null},
        "memory": {"quantity": "3", "unit": "GiB-hour", "attributedAmountMinor": "100", "usageValueAmountMinor": null},
        "sandboxStorage": {"quantity": "0", "unit": "GiB-hour", "attributedAmountMinor": "0", "usageValueAmountMinor": null},
        "snapshotStorage": {"quantity": "0", "unit": "GiB-hour", "attributedAmountMinor": "0", "usageValueAmountMinor": null},
        "networkEgress": {"quantity": "0", "unit": "GiB", "attributedAmountMinor": "0", "usageValueAmountMinor": null}
      }
    }
  ],
  "total": 2,
  "nextCursor": 1
}"#;

/// Serve one JSON response and return the request line the client sent.
async fn serve_once(body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let request_bytes = read_http_request(&mut socket).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        let request = String::from_utf8_lossy(&request_bytes).to_string();
        request.lines().next().unwrap_or_default().to_string()
    });
    (format!("http://{address}"), server)
}

fn usage_client(base_url: &str) -> UsageClient {
    let client = ClientBuilder::new(base_url).build().expect("build client");
    UsageClient::new(client, "org-1")
}

#[tokio::test]
async fn current_period_sandboxes_send_no_dates_and_next_page_pins_the_window() {
    let (base_url, server) = serve_once(PAGE_ONE).await;
    let usage = usage_client(&base_url);

    let page = usage
        .sandboxes(&SandboxUsageRequest {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .expect("first page")
        .into_inner();

    assert_eq!(
        server.await.expect("server join"),
        "GET /platform/v1/organizations/org-1/sandbox-usage/sandboxes?limit=1 HTTP/1.1"
    );
    assert_eq!(page.sandboxes[0].sandbox_id, "sb-1");
    assert_eq!(page.sandboxes[0].resources.cpu.quantity, "1.5");
    assert_eq!(
        page.attributed_usage_charge.as_ref().map(|c| &c.basis),
        Some(&UsageChargeBasis::CurrentPriceEstimate)
    );

    // The next page asks for the first page's exact window, even though the
    // first request had no dates.
    let (base_url, server) = serve_once(PAGE_ONE).await;
    let usage = usage_client(&base_url);
    usage
        .sandboxes(&SandboxUsageRequest {
            window: UsageWindow::LastWeek,
            limit: Some(1),
            cursor: page.next_page_cursor(),
        })
        .await
        .expect("second page");

    assert_eq!(
        server.await.expect("server join"),
        "GET /platform/v1/organizations/org-1/sandbox-usage/sandboxes?fromDate=2026-10-01T00%3A00%3A00.000Z&toDate=2026-10-28T12%3A00%3A00.000Z&cursor=1&limit=1 HTTP/1.1"
    );
}

#[tokio::test]
async fn lookback_summary_sends_an_exact_range() {
    const SUMMARY: &str = r#"{
      "granularity": "day",
      "windowStart": "2026-10-21T12:00:00.000Z",
      "windowEnd": "2026-10-28T12:00:00.000Z",
      "computeUnits": "42.5",
      "resources": null,
      "usagePricing": null,
      "usageCost": null,
      "usageCharge": null,
      "billedSpendCents": null,
      "runningSandboxes": 3,
      "series": [],
      "projects": [{"projectId": "proj-1", "projectName": "Default", "computeUnits": "42.5"}]
    }"#;
    let (base_url, server) = serve_once(SUMMARY).await;
    let usage = usage_client(&base_url);

    let summary = usage
        .summary(UsageWindow::LastWeek)
        .await
        .expect("summary")
        .into_inner();

    let request_line = server.await.expect("server join");
    assert!(
        request_line.starts_with(
            "GET /platform/v1/organizations/org-1/sandbox-usage/breakdown?granularity=day&fromDate="
        ),
        "{request_line}"
    );
    assert!(request_line.contains("&toDate="), "{request_line}");
    assert_eq!(summary.compute_units, "42.5");
    assert_eq!(summary.running_sandboxes, 3);
    assert_eq!(summary.usage_charge, None);
    assert_eq!(summary.projects[0].project_id, "proj-1");
}

#[tokio::test]
async fn missing_organization_is_rejected_before_any_request() {
    let client = ClientBuilder::new("http://127.0.0.1:9")
        .build()
        .expect("build client");
    let err = UsageClient::new(client, "")
        .summary(UsageWindow::CurrentPeriod)
        .await
        .expect_err("empty organization must fail");
    assert!(
        err.to_string().contains("organization ID is required"),
        "{err}"
    );
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buf = [0_u8; 4096];

    loop {
        let read = socket.read(&mut buf).await.expect("read request");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buf[..read]);

        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    request
}
