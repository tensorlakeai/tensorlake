"""Tests for sandbox models."""

import pytest
from pydantic import ValidationError

from tensorlake.sandbox.models import (
    ContainerResourcesInfo,
    CreateSandboxPoolResponse,
    CreateSandboxResponse,
    ListSandboxesResponse,
    ListSandboxPoolsResponse,
    NetworkConfig,
    SandboxInfo,
    SandboxPoolInfo,
    SandboxStatus,
)


class TestSandboxStatus:
    """Tests for SandboxStatus enum."""

    def test_status_values(self):
        """Test that status enum has correct values."""
        assert SandboxStatus.PENDING.value == "Pending"
        assert SandboxStatus.RUNNING.value == "Running"
        assert SandboxStatus.TERMINATED.value == "Terminated"


class TestContainerResourcesInfo:
    """Tests for ContainerResourcesInfo model."""

    def test_valid_resources(self):
        """Test valid resource configuration."""
        resources = ContainerResourcesInfo(
            cpus=2.0, memory_mb=1024, ephemeral_disk_mb=2048
        )
        assert resources.cpus == 2.0
        assert resources.memory_mb == 1024
        assert resources.ephemeral_disk_mb == 2048

    def test_missing_required_field(self):
        """Test that missing required fields raise validation error."""
        with pytest.raises(ValidationError):
            ContainerResourcesInfo(cpus=1.0, memory_mb=512)


class TestNetworkConfig:
    """Tests for NetworkConfig model."""

    def test_default_values(self):
        """Test default network configuration."""
        network = NetworkConfig()
        assert network.allow_internet_access is True
        assert network.allow_out == []
        assert network.deny_out == []

    def test_custom_values(self):
        """Test custom network configuration."""
        network = NetworkConfig(
            allow_internet_access=False,
            allow_out=["api.example.com"],
            deny_out=["badsite.com"],
        )
        assert network.allow_internet_access is False
        assert network.allow_out == ["api.example.com"]
        assert network.deny_out == ["badsite.com"]


class TestCreateSandboxResponse:
    """Tests for CreateSandboxResponse model."""

    def test_valid_response(self):
        """Test valid create sandbox response."""
        response = CreateSandboxResponse(
            sandbox_id="sb_123", status=SandboxStatus.PENDING
        )
        assert response.sandbox_id == "sb_123"
        assert response.status == SandboxStatus.PENDING

    def test_from_dict(self):
        """Test creating response from dictionary."""
        data = {"sandbox_id": "sb_456", "status": "Running"}
        response = CreateSandboxResponse(**data)
        assert response.sandbox_id == "sb_456"
        assert response.status == SandboxStatus.RUNNING


class TestSandboxInfo:
    """Tests for SandboxInfo model."""

    def test_minimal_sandbox_info(self):
        """Test sandbox info with minimal fields."""
        info = SandboxInfo(
            sandbox_id="sb_123",
            namespace="default",
            status=SandboxStatus.RUNNING,
            resources=ContainerResourcesInfo(
                cpus=1.0, memory_mb=512, ephemeral_disk_mb=1024
            ),
        )
        assert info.sandbox_id == "sb_123"
        assert info.namespace == "default"
        assert info.status == SandboxStatus.RUNNING
        assert info.image is None
        assert info.secret_names == []

    def test_full_sandbox_info(self):
        """Test sandbox info with all fields."""
        info = SandboxInfo(
            sandbox_id="sb_123",
            namespace="default",
            status=SandboxStatus.RUNNING,
            image="python:3.11",
            resources=ContainerResourcesInfo(
                cpus=2.0, memory_mb=1024, ephemeral_disk_mb=2048
            ),
            secret_names=["api_key"],
            timeout_secs=3600,
            entrypoint=["/bin/bash", "-c", "python main.py"],
            network=NetworkConfig(allow_internet_access=True),
            pool_id="pool_456",
            created_at=1704067200000,
        )
        assert info.image == "python:3.11"
        assert info.resources.cpus == 2.0
        assert info.secret_names == ["api_key"]
        assert info.timeout_secs == 3600
        assert info.entrypoint == ["/bin/bash", "-c", "python main.py"]
        assert info.pool_id == "pool_456"


class TestListSandboxesResponse:
    """Tests for ListSandboxesResponse model."""

    def test_empty_list(self):
        """Test empty sandbox list."""
        response = ListSandboxesResponse(sandboxes=[])
        assert response.sandboxes == []

    def test_with_sandboxes(self):
        """Test list with sandboxes."""
        sandboxes = [
            SandboxInfo(
                sandbox_id="sb_1",
                namespace="default",
                status=SandboxStatus.RUNNING,
                resources=ContainerResourcesInfo(
                    cpus=1.0, memory_mb=512, ephemeral_disk_mb=1024
                ),
            ),
            SandboxInfo(
                sandbox_id="sb_2",
                namespace="default",
                status=SandboxStatus.PENDING,
                resources=ContainerResourcesInfo(
                    cpus=2.0, memory_mb=1024, ephemeral_disk_mb=2048
                ),
            ),
        ]
        response = ListSandboxesResponse(sandboxes=sandboxes)
        assert len(response.sandboxes) == 2
        assert response.sandboxes[0].sandbox_id == "sb_1"
        assert response.sandboxes[1].sandbox_id == "sb_2"


class TestCreateSandboxPoolResponse:
    """Tests for CreateSandboxPoolResponse model."""

    def test_valid_response(self):
        """Test valid create pool response."""
        response = CreateSandboxPoolResponse(pool_id="pool_123", namespace="default")
        assert response.pool_id == "pool_123"
        assert response.namespace == "default"


class TestSandboxPoolInfo:
    """Tests for SandboxPoolInfo model."""

    def test_minimal_pool_info(self):
        """Test pool info with minimal fields."""
        info = SandboxPoolInfo(
            pool_id="pool_123",
            namespace="default",
            image="python:3.11",
            resources=ContainerResourcesInfo(
                cpus=1.0, memory_mb=512, ephemeral_disk_mb=1024
            ),
        )
        assert info.pool_id == "pool_123"
        assert info.namespace == "default"
        assert info.image == "python:3.11"
        assert info.secret_names == []
        assert info.timeout_secs == 0

    def test_full_pool_info(self):
        """Test pool info with all fields."""
        info = SandboxPoolInfo(
            pool_id="pool_123",
            namespace="default",
            image="python:3.11",
            resources=ContainerResourcesInfo(
                cpus=2.0, memory_mb=1024, ephemeral_disk_mb=2048
            ),
            secret_names=["api_key"],
            timeout_secs=3600,
            entrypoint=["/bin/bash"],
            max_containers=10,
            warm_containers=2,
            created_at=1704067200000,
            updated_at=1704153600000,
        )
        assert info.image == "python:3.11"
        assert info.resources.cpus == 2.0
        assert info.secret_names == ["api_key"]
        assert info.timeout_secs == 3600
        assert info.max_containers == 10
        assert info.warm_containers == 2


class TestListSandboxPoolsResponse:
    """Tests for ListSandboxPoolsResponse model."""

    def test_empty_list(self):
        """Test empty pool list."""
        response = ListSandboxPoolsResponse(pools=[])
        assert response.pools == []

    def test_with_pools(self):
        """Test list with pools."""
        pools = [
            SandboxPoolInfo(
                pool_id="pool_1",
                namespace="default",
                image="python:3.11",
                resources=ContainerResourcesInfo(
                    cpus=1.0, memory_mb=512, ephemeral_disk_mb=1024
                ),
            ),
            SandboxPoolInfo(
                pool_id="pool_2",
                namespace="default",
                image="node:18",
                resources=ContainerResourcesInfo(
                    cpus=2.0, memory_mb=1024, ephemeral_disk_mb=2048
                ),
            ),
        ]
        response = ListSandboxPoolsResponse(pools=pools)
        assert len(response.pools) == 2
        assert response.pools[0].pool_id == "pool_1"
        assert response.pools[1].pool_id == "pool_2"
