"""Tests for sandbox client."""

import httpx
import pytest
import respx

from tensorlake.sandbox import (
    CreateSandboxPoolResponse,
    CreateSandboxResponse,
    PoolInUseError,
    PoolNotFoundError,
    RemoteAPIError,
    SandboxClient,
    SandboxInfo,
    SandboxNotFoundError,
    SandboxPoolInfo,
    SandboxStatus,
)


@pytest.fixture
def client():
    """Create a sandbox client pointing to a remote (non-localhost) API."""
    return SandboxClient(api_url="http://test.local", namespace="test-ns")


@pytest.fixture
def localhost_client():
    """Create a sandbox client pointing to localhost."""
    return SandboxClient(api_url="http://localhost:8900", namespace="test-ns")


@pytest.fixture
def mock_api():
    """Mock the HTTP API for remote client."""
    with respx.mock(base_url="http://test.local") as mock:
        yield mock


@pytest.fixture
def mock_localhost_api():
    """Mock the HTTP API for localhost client."""
    with respx.mock(base_url="http://localhost:8900") as mock:
        yield mock


class TestSandboxClientInit:
    """Tests for SandboxClient initialization."""

    def test_default_initialization(self):
        """Test client with default values."""
        client = SandboxClient(api_url="https://api.tensorlake.ai")
        assert client._api_url == "https://api.tensorlake.ai"
        assert client._namespace == "default"

    def test_custom_initialization(self):
        """Test client with custom values."""
        client = SandboxClient(
            api_url="http://custom:9000",
            namespace="custom-ns",
            api_key="test-key",
            organization_id="org-123",
            project_id="proj-456",
        )
        assert client._api_url == "http://custom:9000"
        assert client._namespace == "custom-ns"
        assert client._api_key == "test-key"
        assert client._organization_id == "org-123"
        assert client._project_id == "proj-456"

    def test_context_manager(self):
        """Test client as context manager."""
        with SandboxClient(api_url="https://api.tensorlake.ai") as client:
            assert isinstance(client, SandboxClient)


class TestURLRouting:
    """Tests for URL routing between localhost and remote."""

    def test_remote_url(self):
        """Remote API uses flat paths."""
        client = SandboxClient(api_url="https://api.tensorlake.ai", namespace="test-ns")
        assert client._endpoint_url("sandboxes") == "https://api.tensorlake.ai/sandboxes"
        assert (
            client._endpoint_url("sandbox-pools")
            == "https://api.tensorlake.ai/sandbox-pools"
        )

    def test_localhost_url(self):
        """Localhost uses namespace-scoped paths."""
        client = SandboxClient(api_url="http://localhost:8900", namespace="test-ns")
        assert (
            client._endpoint_url("sandboxes")
            == "http://localhost:8900/v1/namespaces/test-ns/sandboxes"
        )
        assert (
            client._endpoint_url("sandbox-pools")
            == "http://localhost:8900/v1/namespaces/test-ns/sandbox-pools"
        )

    def test_127_0_0_1_url(self):
        """127.0.0.1 is treated as localhost."""
        client = SandboxClient(api_url="http://127.0.0.1:8900", namespace="test-ns")
        assert (
            client._endpoint_url("sandboxes")
            == "http://127.0.0.1:8900/v1/namespaces/test-ns/sandboxes"
        )


class TestAuthHeaders:
    """Tests for authentication header injection."""

    def test_auth_headers_with_api_key(self):
        """Test that API key is added as Bearer token."""
        client = SandboxClient(
            api_url="https://api.tensorlake.ai", api_key="test-key"
        )
        headers = {}
        client._add_auth_headers(headers)
        assert headers["Authorization"] == "Bearer test-key"

    def test_auth_headers_with_org_and_project(self):
        """Test that org and project IDs are added as headers."""
        client = SandboxClient(
            api_url="https://api.tensorlake.ai",
            organization_id="org-123",
            project_id="proj-456",
        )
        headers = {}
        client._add_auth_headers(headers)
        assert headers["X-Forwarded-Organization-Id"] == "org-123"
        assert headers["X-Forwarded-Project-Id"] == "proj-456"

    def test_auth_headers_no_credentials(self):
        """Test that no headers are added without credentials."""
        client = SandboxClient(api_url="https://api.tensorlake.ai", api_key=None)
        headers = {}
        client._add_auth_headers(headers)
        assert "Authorization" not in headers


class TestSandboxCreate:
    """Tests for sandbox creation."""

    def test_create_minimal(self, client, mock_api):
        """Test creating sandbox with minimal parameters."""
        mock_api.post("/sandboxes").mock(
            return_value=httpx.Response(
                200, json={"sandbox_id": "sb_123", "status": "Pending"}
            )
        )

        response = client.create(image="python:3.11")
        assert response.sandbox_id == "sb_123"
        assert response.status == SandboxStatus.PENDING

    def test_create_with_resources(self, client, mock_api):
        """Test creating sandbox with custom resources."""
        route = mock_api.post("/sandboxes").mock(
            return_value=httpx.Response(
                200, json={"sandbox_id": "sb_456", "status": "Running"}
            )
        )

        response = client.create(
            image="python:3.11", cpus=2.0, memory_mb=1024, ephemeral_disk_mb=2048
        )
        assert response.sandbox_id == "sb_456"

        request = route.calls.last.request
        payload = request.read()
        assert b'"cpus":2.0' in payload or b'"cpus": 2.0' in payload
        assert b'"memory_mb":1024' in payload or b'"memory_mb": 1024' in payload

    def test_create_with_secrets(self, client, mock_api):
        """Test creating sandbox with secrets."""
        route = mock_api.post("/sandboxes").mock(
            return_value=httpx.Response(
                200, json={"sandbox_id": "sb_789", "status": "Pending"}
            )
        )

        response = client.create(image="python:3.11", secret_names=["api_key", "token"])
        assert response.sandbox_id == "sb_789"

        request = route.calls.last.request
        payload = request.read()
        assert b"secret_names" in payload

    def test_create_with_network_config(self, client, mock_api):
        """Test creating sandbox with network configuration."""
        route = mock_api.post("/sandboxes").mock(
            return_value=httpx.Response(
                200, json={"sandbox_id": "sb_net", "status": "Pending"}
            )
        )

        response = client.create(
            image="python:3.11",
            allow_internet_access=False,
            allow_out=["api.example.com"],
        )
        assert response.sandbox_id == "sb_net"

        request = route.calls.last.request
        payload = request.read()
        assert b"network" in payload

    def test_create_with_pool(self, client, mock_api):
        """Test creating sandbox from pool."""
        route = mock_api.post("/sandboxes").mock(
            return_value=httpx.Response(
                200, json={"sandbox_id": "sb_pool", "status": "Running"}
            )
        )

        response = client.create(pool_id="pool_123")
        assert response.sandbox_id == "sb_pool"

        request = route.calls.last.request
        payload = request.read()
        assert b"pool_id" in payload

    def test_create_error(self, client, mock_api):
        """Test error handling in create."""
        mock_api.post("/sandboxes").mock(
            return_value=httpx.Response(500, text="Internal server error")
        )

        with pytest.raises(RemoteAPIError) as exc_info:
            client.create(image="python:3.11")
        assert exc_info.value.status_code == 500

    def test_create_localhost(self, localhost_client, mock_localhost_api):
        """Test creating sandbox via localhost uses namespace-scoped URL."""
        mock_localhost_api.post("/v1/namespaces/test-ns/sandboxes").mock(
            return_value=httpx.Response(
                200, json={"sandbox_id": "sb_local", "status": "Pending"}
            )
        )

        response = localhost_client.create(image="python:3.11")
        assert response.sandbox_id == "sb_local"


class TestSandboxGet:
    """Tests for getting sandbox info."""

    def test_get_success(self, client, mock_api):
        """Test getting sandbox info."""
        mock_api.get("/sandboxes/sb_123").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox_id": "sb_123",
                    "namespace": "test-ns",
                    "status": "Running",
                    "image": "python:3.11",
                    "resources": {
                        "cpus": 1.0,
                        "memory_mb": 512,
                        "ephemeral_disk_mb": 1024,
                    },
                },
            )
        )

        info = client.get("sb_123")
        assert isinstance(info, SandboxInfo)
        assert info.sandbox_id == "sb_123"
        assert info.status == SandboxStatus.RUNNING
        assert info.image == "python:3.11"

    def test_get_not_found(self, client, mock_api):
        """Test getting non-existent sandbox."""
        mock_api.get("/sandboxes/sb_404").mock(
            return_value=httpx.Response(404, text="Not found")
        )

        with pytest.raises(SandboxNotFoundError) as exc_info:
            client.get("sb_404")
        assert exc_info.value.sandbox_id == "sb_404"


class TestSandboxList:
    """Tests for listing sandboxes."""

    def test_list_empty(self, client, mock_api):
        """Test listing when no sandboxes exist."""
        mock_api.get("/sandboxes").mock(
            return_value=httpx.Response(200, json={"sandboxes": []})
        )

        sandboxes = client.list()
        assert sandboxes == []

    def test_list_with_sandboxes(self, client, mock_api):
        """Test listing multiple sandboxes."""
        mock_api.get("/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandboxes": [
                        {
                            "sandbox_id": "sb_1",
                            "namespace": "test-ns",
                            "status": "Running",
                            "resources": {
                                "cpus": 1.0,
                                "memory_mb": 512,
                                "ephemeral_disk_mb": 1024,
                            },
                        },
                        {
                            "sandbox_id": "sb_2",
                            "namespace": "test-ns",
                            "status": "Pending",
                            "resources": {
                                "cpus": 2.0,
                                "memory_mb": 1024,
                                "ephemeral_disk_mb": 2048,
                            },
                        },
                    ]
                },
            )
        )

        sandboxes = client.list()
        assert len(sandboxes) == 2
        assert sandboxes[0].sandbox_id == "sb_1"
        assert sandboxes[1].sandbox_id == "sb_2"


class TestSandboxDelete:
    """Tests for deleting sandboxes."""

    def test_delete_success(self, client, mock_api):
        """Test deleting a sandbox."""
        mock_api.delete("/sandboxes/sb_123").mock(
            return_value=httpx.Response(200)
        )

        client.delete("sb_123")

    def test_delete_not_found(self, client, mock_api):
        """Test deleting non-existent sandbox."""
        mock_api.delete("/sandboxes/sb_404").mock(
            return_value=httpx.Response(404, text="Not found")
        )

        with pytest.raises(SandboxNotFoundError) as exc_info:
            client.delete("sb_404")
        assert exc_info.value.sandbox_id == "sb_404"


class TestPoolCreate:
    """Tests for pool creation."""

    def test_create_pool_minimal(self, client, mock_api):
        """Test creating pool with minimal parameters."""
        mock_api.post("/sandbox-pools").mock(
            return_value=httpx.Response(
                200, json={"pool_id": "pool_123", "namespace": "test-ns"}
            )
        )

        response = client.create_pool(image="python:3.11")
        assert response.pool_id == "pool_123"
        assert response.namespace == "test-ns"

    def test_create_pool_with_config(self, client, mock_api):
        """Test creating pool with full configuration."""
        route = mock_api.post("/sandbox-pools").mock(
            return_value=httpx.Response(
                200, json={"pool_id": "pool_456", "namespace": "test-ns"}
            )
        )

        response = client.create_pool(
            image="python:3.11",
            cpus=2.0,
            memory_mb=1024,
            max_containers=10,
            warm_containers=2,
        )
        assert response.pool_id == "pool_456"

        request = route.calls.last.request
        payload = request.read()
        assert b"max_containers" in payload
        assert b"warm_containers" in payload


class TestPoolGet:
    """Tests for getting pool info."""

    def test_get_pool_success(self, client, mock_api):
        """Test getting pool info."""
        mock_api.get("/sandbox-pools/pool_123").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pool_id": "pool_123",
                    "namespace": "test-ns",
                    "image": "python:3.11",
                    "resources": {
                        "cpus": 1.0,
                        "memory_mb": 512,
                        "ephemeral_disk_mb": 1024,
                    },
                },
            )
        )

        info = client.get_pool("pool_123")
        assert isinstance(info, SandboxPoolInfo)
        assert info.pool_id == "pool_123"
        assert info.image == "python:3.11"

    def test_get_pool_not_found(self, client, mock_api):
        """Test getting non-existent pool."""
        mock_api.get("/sandbox-pools/pool_404").mock(
            return_value=httpx.Response(404, text="Not found")
        )

        with pytest.raises(PoolNotFoundError) as exc_info:
            client.get_pool("pool_404")
        assert exc_info.value.pool_id == "pool_404"


class TestPoolList:
    """Tests for listing pools."""

    def test_list_pools_empty(self, client, mock_api):
        """Test listing when no pools exist."""
        mock_api.get("/sandbox-pools").mock(
            return_value=httpx.Response(200, json={"pools": []})
        )

        pools = client.list_pools()
        assert pools == []

    def test_list_pools_with_data(self, client, mock_api):
        """Test listing multiple pools."""
        mock_api.get("/sandbox-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pools": [
                        {
                            "pool_id": "pool_1",
                            "namespace": "test-ns",
                            "image": "python:3.11",
                            "resources": {
                                "cpus": 1.0,
                                "memory_mb": 512,
                                "ephemeral_disk_mb": 1024,
                            },
                        },
                        {
                            "pool_id": "pool_2",
                            "namespace": "test-ns",
                            "image": "node:18",
                            "resources": {
                                "cpus": 2.0,
                                "memory_mb": 1024,
                                "ephemeral_disk_mb": 2048,
                            },
                        },
                    ]
                },
            )
        )

        pools = client.list_pools()
        assert len(pools) == 2
        assert pools[0].pool_id == "pool_1"
        assert pools[1].pool_id == "pool_2"


class TestPoolUpdate:
    """Tests for updating pools."""

    def test_update_pool_success(self, client, mock_api):
        """Test updating pool configuration."""
        mock_api.put("/sandbox-pools/pool_123").mock(
            return_value=httpx.Response(
                200,
                json={
                    "pool_id": "pool_123",
                    "namespace": "test-ns",
                    "image": "python:3.12",
                    "resources": {
                        "cpus": 2.0,
                        "memory_mb": 1024,
                        "ephemeral_disk_mb": 2048,
                    },
                },
            )
        )

        info = client.update_pool(
            "pool_123", image="python:3.12", cpus=2.0, memory_mb=1024
        )
        assert info.pool_id == "pool_123"
        assert info.image == "python:3.12"

    def test_update_pool_not_found(self, client, mock_api):
        """Test updating non-existent pool."""
        mock_api.put("/sandbox-pools/pool_404").mock(
            return_value=httpx.Response(404, text="Not found")
        )

        with pytest.raises(PoolNotFoundError):
            client.update_pool("pool_404", image="python:3.11")


class TestPoolDelete:
    """Tests for deleting pools."""

    def test_delete_pool_success(self, client, mock_api):
        """Test deleting a pool."""
        mock_api.delete("/sandbox-pools/pool_123").mock(
            return_value=httpx.Response(200)
        )

        client.delete_pool("pool_123")

    def test_delete_pool_not_found(self, client, mock_api):
        """Test deleting non-existent pool."""
        mock_api.delete("/sandbox-pools/pool_404").mock(
            return_value=httpx.Response(404, text="Not found")
        )

        with pytest.raises(PoolNotFoundError):
            client.delete_pool("pool_404")

    def test_delete_pool_in_use(self, client, mock_api):
        """Test deleting pool that is in use."""
        mock_api.delete("/sandbox-pools/pool_busy").mock(
            return_value=httpx.Response(409, text="Pool has active containers")
        )

        with pytest.raises(PoolInUseError) as exc_info:
            client.delete_pool("pool_busy")
        assert exc_info.value.pool_id == "pool_busy"
