# ImageBuilder v2/v3 Coexistence Implementation

## Overview

This implementation enables side-by-side operation of ImageBuilder v2 (existing) and v3 (new) with environment variable and CLI flag control, maintaining backward compatibility while allowing opt-in to new functionality.

**Default: v2** (existing functionality, conservative approach)
**Opt-in: v3** (new functionality with parallel builds, improved logging)

## Configuration

### Environment Variable
```bash
export TENSORLAKE_IMAGE_BUILDER_VERSION=v2  # Options: "v2" (default), "v3"
```

### CLI Flag
```bash
tensorlake deploy --image-builder-version v3 app.py
```

**Priority:** CLI flag > Environment variable > Default (v2)

## Architecture

### Factory Pattern with Protocol
Uses Protocol (structural typing) for flexibility without forcing inheritance. Both v2 and v3 clients work independently, unified by a common factory.

### Key Components

#### 1. Unified Exception Hierarchy (`exceptions.py`)
All exceptions include version information for debugging:

```python
from tensorlake.applications.image_builder.exceptions import (
    ImageBuilderError,              # Base exception
    ImageBuilderConfigError,        # Configuration errors
    ImageBuilderBuildError,         # Build failures
    ImageBuilderNetworkError,       # Network errors
    ImageBuilderV2Error,            # V2-specific base
    ImageBuilderClientV3Error,      # V3-specific base
)
```

**Features:**
- Version tracking in all errors
- Request ID correlation for traceability
- Clear, actionable error messages
- Proper error context propagation

#### 2. Factory (`factory.py`)
Version detection and builder instantiation:

```python
from tensorlake.applications.image_builder import (
    get_image_builder_version,
    create_image_builder_from_context,
)

# Detect version
version = get_image_builder_version(override="v3")  # CLI override

# Create builder
builder = create_image_builder_from_context(
    api_key=api_key,
    pat=pat,
    organization_id=org_id,
    project_id=proj_id,
    version=version,
)
```

**Version Detection Logic:**
1. Check CLI flag override
2. Fall back to `TENSORLAKE_IMAGE_BUILDER_VERSION` env var
3. Default to "v2" if not specified
4. Normalize shorthand: "2" → "v2", "3" → "v3"
5. Validate and raise `ImageBuilderConfigError` for invalid values

#### 3. V2 Adapter (`adapter_v2.py`)
Wraps existing ImageBuilderV2Client to accept BuildRequest interface:

```python
from tensorlake.applications.image_builder.adapter_v2 import ImageBuilderV2Adapter

adapter = ImageBuilderV2Adapter.from_context(
    api_key=api_key,
    organization_id=org_id,
    project_id=proj_id,
)

await adapter.build(build_request)
```

**Features:**
- Sequential building (v2 limitation)
- Converts BuildRequest to individual BuildContext calls
- Wraps v2 exceptions in unified hierarchy
- Displays version indicator: "🔧 Using ImageBuilder v2"

#### 4. Protocol (`protocol.py`)
Common interface for both builders:

```python
class ImageBuilder(Protocol):
    async def build(self, req: BuildRequest) -> None:
        ...
```

#### 5. Updated CLI (`cli/deploy.py`)
Added `--image-builder-version` flag:

```bash
tensorlake deploy --image-builder-version v3 app.py
```

The deploy command:
- Detects version early and displays to user
- Uses factory to create appropriate builder
- Handles `ImageBuilderConfigError` gracefully

## File Structure

```
src/tensorlake/applications/image_builder/
├── __init__.py                 # Exports factory, builders, exceptions
├── exceptions.py               # NEW: Unified exception hierarchy
├── factory.py                  # NEW: Version detection & builder factory
├── adapter_v2.py              # NEW: V2 client adapter
├── protocol.py                # NEW: Common interface protocol
├── client_v2.py               # COPIED: V2 client (from builder/)
├── client_v3.py               # MODIFIED: Updated exception imports
└── ...

src/tensorlake/builder/
└── client_v2.py               # MODIFIED: Added deprecation warning

src/tensorlake/cli/
└── deploy.py                  # MODIFIED: Added --image-builder-version flag

tests/applications/image_builder/
├── test_factory.py            # NEW: Factory & version detection tests
├── test_exceptions.py         # NEW: Exception hierarchy tests
├── test_adapter_v2.py         # NEW: V2 adapter tests
└── test_backward_compatibility.py  # NEW: Backward compatibility tests
```

## Usage Examples

### Default Behavior (v2)
```bash
tensorlake deploy app.py
# Output: 🔧 Using ImageBuilder v2
# Uses sequential building
```

### Environment Variable Override
```bash
export TENSORLAKE_IMAGE_BUILDER_VERSION=v3
tensorlake deploy app.py
# Output: 🔧 Using ImageBuilder v3
# Uses parallel building with reporters
```

### CLI Flag Override
```bash
export TENSORLAKE_IMAGE_BUILDER_VERSION=v2
tensorlake deploy --image-builder-version v3 app.py
# Output: 🔧 Using ImageBuilder v3
# CLI flag takes priority
```

### Invalid Version
```bash
export TENSORLAKE_IMAGE_BUILDER_VERSION=v99
tensorlake deploy app.py
# Error: Invalid image builder version: 'v99'. Valid options: 'v2', 'v3' (default: 'v2')
```

## Error Handling

All exceptions include version context:

```python
try:
    await builder.build(req)
except ImageBuilderError as e:
    # Error message includes version automatically
    print(f"Build failed: {e}")
    # Example: "Build failed: Network timeout (version: v2) (request_id: req-123)"
```

## Backward Compatibility

### Default to v2
Existing deployments continue to use v2 by default with no changes required.

### Old Import Path
The old import path is still supported with a deprecation warning:

```python
from tensorlake.builder.client_v2 import ImageBuilderV2Client
# Warning: Importing from tensorlake.builder.client_v2 is deprecated.
#          Please use tensorlake.applications.image_builder.client_v2 instead.
```

### No Breaking Changes
All existing code continues to work without modifications.

## Testing

Core functionality has been validated with manual integration tests covering:

1. ✅ Default version is v2
2. ✅ Environment variable override
3. ✅ CLI flag override (takes priority)
4. ✅ Shorthand normalization ("2" → "v2", "3" → "v3")
5. ✅ Invalid version error handling
6. ✅ Factory creates v2 adapter correctly
7. ✅ Factory creates v3 builder correctly
8. ✅ V2 exception hierarchy
9. ✅ V3 exception hierarchy
10. ✅ Backward compatibility with deprecation warnings

## Migration Path

### For Users
1. **No action required** - v2 is the default
2. **To try v3**: Set environment variable or use CLI flag
3. **To switch permanently**: Update deployment scripts to use v3

### For Future Development
1. Monitor v3 adoption and stability
2. Gradually encourage migration to v3
3. Eventually make v3 the default (breaking change, requires major version bump)
4. Remove v2 support in a future major version

## Production Considerations

### Error Handling
- All errors include version information for debugging
- Request ID tracking for correlation
- Clear, actionable error messages
- Graceful degradation on configuration errors

### Logging
- Version logged on deployment start
- Build events tagged with version
- Compatible with existing CloudEvents format
- Structured logging for observability

### Performance
- V2 adapter is a thin wrapper with no performance impact
- Factory instantiation is fast (no additional overhead)
- Version detection happens once at deployment start

## Known Limitations

1. **V2 Sequential Builds**: V2 builds images sequentially (not parallel)
2. **No Dynamic Switching**: Version is determined at deployment start
3. **Environment Variables**: Requires shell access to set env vars

## Future Enhancements

1. **Config File Support**: Allow version specification in config file
2. **Per-Application Version**: Support different versions for different apps
3. **Gradual Migration**: Tools to help migrate from v2 to v3
4. **Metrics & Monitoring**: Track v2 vs v3 usage and performance
