"""``UsageClient`` reads usage through the real Rust binding.

A scripted HTTP server answers the Platform API usage routes. Paging must
pin the first page's window, and amounts must parse without precision loss.
"""

import json
import threading
import unittest
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from tensorlake import UsageClient
from tensorlake.usage import UsageError

_WINDOW_START = "2026-10-01T00:00:00.000Z"
_WINDOW_END = "2026-10-28T12:00:00.000Z"


def _resource(quantity: str, unit: str, amount: str) -> dict:
    return {
        "quantity": quantity,
        "unit": unit,
        "attributedAmountMinor": amount,
        "usageValueAmountMinor": None,
    }


def _sandbox(sandbox_id: str, amount: str) -> dict:
    return {
        "sandboxId": sandbox_id,
        "project": {"projectId": "proj-1", "projectName": "Default"},
        "attributedAmountMinor": amount,
        "usageValueAmountMinor": None,
        "resources": {
            "cpu": _resource("1.5", "vCPU-hour", amount),
            "memory": _resource("0", "GiB-hour", "0"),
            "sandboxStorage": _resource("0", "GiB-hour", "0"),
            "snapshotStorage": _resource("0", "GiB-hour", "0"),
            "networkEgress": _resource("0", "GiB", "0"),
        },
    }


def _page(sandboxes: list[dict], next_cursor: int | None) -> dict:
    return {
        "windowStart": _WINDOW_START,
        "windowEnd": _WINDOW_END,
        "attributedUsageCharge": {
            "currency": "usd",
            "amountMinor": "300",
            "basis": "current_price_estimate",
        },
        "includedUsage": None,
        "attributedUsageValue": None,
        "projects": [],
        "sandboxes": sandboxes,
        "total": 2,
        "nextCursor": next_cursor,
    }


class _UsageHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler API
        return None

    def do_GET(self):
        url = urlparse(self.path)
        query = parse_qs(url.query)
        self.server.requests.append((url.path, query))
        if url.path.endswith("/sandbox-usage/breakdown"):
            payload = {
                "granularity": "day",
                "windowStart": _WINDOW_START,
                "windowEnd": _WINDOW_END,
                "computeUnits": "12345678901234567890.5",
                "resources": None,
                "usagePricing": None,
                "usageCost": None,
                "usageCharge": None,
                "billedSpendCents": None,
                "runningSandboxes": 2,
                "series": [],
                "projects": [
                    {
                        "projectId": "proj-1",
                        "projectName": "Default",
                        "computeUnits": "1.25",
                    }
                ],
            }
        elif "cursor" in query:
            payload = _page([_sandbox("sb-2", "100")], None)
        else:
            payload = _page([_sandbox("sb-1", "200")], 1)
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class TestUsageClient(unittest.TestCase):
    def setUp(self):
        self.server = HTTPServer(("127.0.0.1", 0), _UsageHandler)
        self.server.requests = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.client = UsageClient(
            api_key="test-key",
            organization_id="org-1",
            api_url=f"http://127.0.0.1:{self.server.server_port}",
        )

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()

    def test_summary_for_current_period_sends_no_dates(self):
        summary = self.client.summary()

        path, query = self.server.requests[0]
        self.assertEqual(
            path, "/platform/v1/organizations/org-1/sandbox-usage/breakdown"
        )
        self.assertEqual(query, {"granularity": ["day"]})
        self.assertEqual(summary.compute_units, Decimal("12345678901234567890.5"))
        self.assertIsNone(summary.usage_charge)
        self.assertEqual(summary.projects[0].compute_units, Decimal("1.25"))

    def test_summary_lookback_sends_an_exact_range(self):
        self.client.summary(last="7d")

        _, query = self.server.requests[0]
        self.assertIn("fromDate", query)
        self.assertIn("toDate", query)

    def test_sandboxes_page_through_one_window(self):
        sandboxes = list(self.client.sandboxes())

        self.assertEqual([s.sandbox_id for s in sandboxes], ["sb-1", "sb-2"])
        self.assertEqual(sandboxes[0].attributed_amount_minor, 200)
        self.assertEqual(sandboxes[0].resources.cpu.quantity, Decimal("1.5"))
        first_path, first_query = self.server.requests[0]
        self.assertEqual(
            first_path, "/platform/v1/organizations/org-1/sandbox-usage/sandboxes"
        )
        self.assertNotIn("fromDate", first_query)
        _, second_query = self.server.requests[1]
        self.assertEqual(second_query["fromDate"], [_WINDOW_START])
        self.assertEqual(second_query["toDate"], [_WINDOW_END])
        self.assertEqual(second_query["cursor"], ["1"])

    def test_rejects_unsupported_lookback(self):
        with self.assertRaises(UsageError):
            self.client.sandboxes(last="2d")
        self.assertEqual(self.server.requests, [])

    def test_requires_organization(self):
        with self.assertRaises(UsageError):
            UsageClient(api_key="test-key", organization_id="")


if __name__ == "__main__":
    unittest.main()
