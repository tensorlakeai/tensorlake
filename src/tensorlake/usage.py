"""Sandbox usage and cost APIs backed by the Rust cloud SDK.

Money amounts are integers in minor currency units (for example cents).
Quantities and compute units are ``Decimal`` so no precision is lost.
"""

from __future__ import annotations

import json
import os
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from tensorlake.cloud_client import CloudClient

UsageLookback = Literal["1d", "7d", "30d"]
"""How far back to read instead of the current billing period."""

_LOOKBACKS = ("1d", "7d", "30d")


class UsageError(RuntimeError):
    """Raised when the usage client is missing configuration or input is invalid."""


class _Model(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class UsageChargeResources(_Model):
    cpu_amount_minor: int
    memory_amount_minor: int
    sandbox_storage_amount_minor: int
    snapshot_storage_amount_minor: int


class UsageCharge(_Model):
    """Metered usage, excluding subscription fees and tax."""

    currency: str
    amount_minor: int
    resources: UsageChargeResources


class IncludedUsage(_Model):
    """Plan allowance consumption, valued at the overage rate."""

    currency: str
    included_amount_minor: int
    period_used_amount_minor: int
    range_usage_amount_minor: int
    period_start: str
    period_end: str


class ProjectComputeUnits(_Model):
    project_id: str
    project_name: str
    compute_units: Decimal


class UsageSummary(_Model):
    """Organization usage totals for a window."""

    window_start: str
    window_end: str
    compute_units: Decimal
    usage_charge: UsageCharge | None = None
    """``None`` when the organization has no metered price."""
    included_usage: IncludedUsage | None = None
    usage_value: UsageCharge | None = None
    """Usage valued at the overage rate before the plan allowance."""
    running_sandboxes: int
    projects: list[ProjectComputeUnits]


class UsageProject(_Model):
    project_id: str
    project_name: str


class ResourceUsage(_Model):
    quantity: Decimal
    unit: str
    """``vCPU-hour``, ``GiB-hour`` or ``GiB``."""
    attributed_amount_minor: int | None = None
    usage_value_amount_minor: int | None = None


class SandboxResourceUsage(_Model):
    cpu: ResourceUsage
    memory: ResourceUsage
    sandbox_storage: ResourceUsage
    snapshot_storage: ResourceUsage
    network_egress: ResourceUsage


class SandboxUsage(_Model):
    """Usage of one sandbox and its share of the organization's usage charge."""

    sandbox_id: str
    project: UsageProject | None = None
    """``None`` for usage whose project no longer exists."""
    attributed_amount_minor: int | None = None
    usage_value_amount_minor: int | None = None
    resources: SandboxResourceUsage


class UsageAmount(_Model):
    currency: str
    amount_minor: int


class AttributedUsageCharge(_Model):
    currency: str
    amount_minor: int
    basis: str
    """``invoice`` for a finalized invoice, ``current_price_estimate`` otherwise."""


class SandboxUsageExport(_Model):
    """Every sandbox of a window with its usage and attributed charge."""

    window_start: str
    window_end: str
    attributed_usage_charge: AttributedUsageCharge | None = None
    """The organization's usage charge; the sandboxes' attributed amounts add
    up to it exactly. ``None`` when the organization has no metered price."""
    included_usage: IncludedUsage | None = None
    attributed_usage_value: UsageAmount | None = None
    sandboxes: list[SandboxUsage]


def _non_empty_env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


class UsageClient:
    """Read an organization's sandbox usage and cost.

    Every read covers the current billing period, or the last ``"1d"``,
    ``"7d"`` or ``"30d"`` when ``last`` is set.
    """

    def __init__(
        self,
        api_key: str | None = None,
        organization_id: str | None = None,
        api_url: str | None = None,
    ):
        """Create a usage client.

        Args:
            api_key: Tensorlake API key. Defaults to ``TENSORLAKE_API_KEY``.
            organization_id: Organization to read. Defaults to
                ``TENSORLAKE_ORGANIZATION_ID``.
            api_url: Cloud API URL. Defaults to ``TENSORLAKE_API_URL``.
        """
        token = api_key or _non_empty_env("TENSORLAKE_API_KEY")
        if not token:
            raise UsageError("Missing TENSORLAKE_API_KEY credentials.")
        self._organization_id = organization_id or _non_empty_env(
            "TENSORLAKE_ORGANIZATION_ID"
        )
        if not self._organization_id:
            raise UsageError(
                "An organization ID is required. Pass organization_id or set "
                "TENSORLAKE_ORGANIZATION_ID."
            )
        self._client = CloudClient(
            api_url=api_url
            or _non_empty_env("TENSORLAKE_API_URL")
            or "https://api.tensorlake.ai",
            api_key=token,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "UsageClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def summary(self, last: UsageLookback | None = None) -> UsageSummary:
        """Organization usage totals for the window."""
        raw = self._client.sandbox_usage_summary_json(
            self._organization_id, _check_lookback(last)
        )
        return UsageSummary.model_validate(json.loads(raw))

    def sandboxes(self, last: UsageLookback | None = None) -> SandboxUsageExport:
        """Usage and attributed cost of every sandbox that ran in the window.

        The server computes the attribution once and returns every sandbox in
        one response.
        """
        raw = self._client.sandbox_usage_export_json(
            self._organization_id, _check_lookback(last)
        )
        return SandboxUsageExport.model_validate(json.loads(raw))


def _check_lookback(last: str | None) -> str | None:
    if last is not None and last not in _LOOKBACKS:
        raise UsageError(f"last must be one of {', '.join(_LOOKBACKS)}; got {last!r}")
    return last
