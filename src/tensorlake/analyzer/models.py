"""Data models for analyzer output using standard library only."""

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ImageBuildOperationModel:
    """Represents a single build operation in an image."""

    type: str
    args: List[str]
    options: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ImageModel:
    """Represents a Docker image configuration."""

    name: str
    tag: str
    base_image: str
    build_operations: List[ImageBuildOperationModel] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "tag": self.tag,
            "base_image": self.base_image,
            "build_operations": [op.to_dict() for op in self.build_operations],
        }


@dataclass
class RetriesModel:
    """Represents retry configuration."""

    max_retries: int = 0
    initial_delay: float = 1.0
    max_delay: float = 60.0
    delay_multiplier: float = 2.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FunctionConfigModel:
    """Represents function configuration."""

    function_name: str
    description: str
    image_name: str
    timeout: int
    cpu: float
    memory: float
    ephemeral_disk: float
    cacheable: bool
    max_concurrency: int
    class_name: Optional[str] = None
    class_method_name: Optional[str] = None
    class_init_timeout: Optional[int] = None
    secrets: List[str] = field(default_factory=list)
    retries: Optional[RetriesModel] = None
    gpu: Optional[Any] = None
    region: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if self.retries:
            result["retries"] = self.retries.to_dict()
        return result


@dataclass
class ApplicationConfigModel:
    """Represents application configuration."""

    retries: RetriesModel
    input_serializer: str
    output_serializer: str
    version: str
    tags: Dict[str, str] = field(default_factory=dict)
    region: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["retries"] = self.retries.to_dict()
        return result


@dataclass
class FunctionModel:
    """Represents a complete function with its configuration."""

    function_name: str
    function_config: FunctionConfigModel
    application_config: Optional[ApplicationConfigModel] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "function_name": self.function_name,
            "function_config": self.function_config.to_dict(),
            "application_config": (
                self.application_config.to_dict() if self.application_config else None
            ),
        }


@dataclass
class ApplicationModel:
    """Represents an application with its associated functions."""

    application_name: str
    version: str
    config: ApplicationConfigModel
    functions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "application_name": self.application_name,
            "version": self.version,
            "functions": self.functions,
            "config": self.config.to_dict(),
        }


@dataclass
class FunctionZIPManifest:
    """Function metadata for ZIP manifest."""

    name: str
    module_import_name: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CodeZIPManifest:
    """Code ZIP manifest containing all function metadata."""

    functions: Dict[str, FunctionZIPManifest] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "functions": {name: fn.to_dict() for name, fn in self.functions.items()}
        }


@dataclass
class AnalysisOutput:
    """Complete analysis output schema."""

    images: Dict[str, ImageModel] = field(default_factory=dict)
    functions: Dict[str, FunctionModel] = field(default_factory=dict)
    applications: Dict[str, ApplicationModel] = field(default_factory=dict)
    code_manifest: CodeZIPManifest = field(default_factory=CodeZIPManifest)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "images": {name: img.to_dict() for name, img in self.images.items()},
            "functions": {name: fn.to_dict() for name, fn in self.functions.items()},
            "applications": {
                name: app.to_dict() for name, app in self.applications.items()
            },
            "code_manifest": self.code_manifest.to_dict(),
        }
