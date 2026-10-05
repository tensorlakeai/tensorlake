# Artifact Storage regions

Select `us-east-1` or `eu-central-1` when constructing a filesystem or repository
client. Omit the region to keep the existing endpoint and behavior.

```python
from tensorlake.filesystem import FilesystemClient
from tensorlake.repositories import RepositoryClient

filesystems = FilesystemClient(api_key="your-api-key", region="eu-central-1")
repositories = RepositoryClient(api_key="your-api-key", region="eu-central-1")
```

```typescript
import { FilesystemClient, RepositoryClient } from "tensorlake";

const filesystems = new FilesystemClient({ apiKey, region: "eu-central-1" });
const repositories = new RepositoryClient({ apiKey, region: "eu-central-1" });
```

```rust
use tensorlake::{Sdk, artifact_storage::ArtifactStorageRegion};

let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
let storage = sdk.artifact_storage_in_region(ArtifactStorageRegion::EuCentral1)?;
# Ok::<(), tensorlake::error::SdkError>(())
```

| Region | Credential API | Git / filesystem endpoint |
| --- | --- | --- |
| `us-east-1` | `https://api.tensorlake.ai` | `https://git.tensorlake.ai` |
| `eu-central-1` | `https://api.eu-central-1.tensorlake.ai` | `https://git.eu-central-1.tensorlake.ai` |

Credential minting, data requests, and SDK filesystem/repository mounts use the same
region. Each client maintains its own credential cache. Authentication uses the
same project API key; the service still derives project scope from that key.

Repositories and filesystems are independent in each region. Selecting a region
does not move data or discover a repository's location. Forks stay in the selected
region, and clients never fall back to another region on a failure. Unsupported
region names fail before a network request. A region cannot be combined with a
development or custom API endpoint; omit it when testing against those endpoints.

The Frankfurt API endpoint serves Artifact Storage credential minting only. It is
not a replacement base URL for sandbox, application or document APIs. Sandbox-side
mount placement is configured by the compute service separately; selecting an SDK
storage region does not change the sandbox's region or its storage integration.
