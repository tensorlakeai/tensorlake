import json

from tensorlake.applications import (
    HttpBody,
    Image,
    Retries,
    application,
    function,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.manifests.application import (
    create_application_manifest,
)


@function(
    description="Processes a customer payload",
    cpu=2.25,
    memory=2,
    ephemeral_disk=3,
    gpu="H100:2",
    timeout=45,
    image=Image(name="compatibility-manifest-image"),
    secrets=["MANIFEST_SECRET"],
    retries=Retries(max_retries=2),
    region="eu-west-1",
    warm_containers=1,
    min_containers=2,
    max_containers=3,
)
def manifest_child(value: int) -> int:
    return value


@application(
    tags={"suite": "compatibility"},
    retries=Retries(max_retries=1),
    region="us-east-1",
    allow=["unauthenticated_requests"],
)
@function(description="Receives a public HTTP body", timeout=30)
def manifest_application(body: HttpBody) -> dict:
    return {"size": len(body.content)}


def function_view(function_manifest) -> dict:
    return {
        "name": function_manifest.name,
        "description": function_manifest.description,
        "secret_names": function_manifest.secret_names,
        "initialization_timeout_sec": function_manifest.initialization_timeout_sec,
        "timeout_sec": function_manifest.timeout_sec,
        "resources": function_manifest.resources.model_dump(),
        "retry_policy": function_manifest.retry_policy.model_dump(),
        "parameter_types": [
            parameter.data_type.type for parameter in function_manifest.parameters
        ],
        "return_type": (
            None
            if function_manifest.return_type is None
            else function_manifest.return_type.type
        ),
        "placement_constraints": (
            function_manifest.placement_constraints.filter_expressions
        ),
        "max_concurrency": function_manifest.max_concurrency,
        "warm_containers": function_manifest.warm_containers,
        "min_containers": function_manifest.min_containers,
        "max_containers": function_manifest.max_containers,
        "image": function_manifest.image,
    }


manifest = create_application_manifest(
    application_function=manifest_application,
    all_functions=get_functions(),
)
print(
    json.dumps(
        {
            "name": manifest.name,
            "description": manifest.description,
            "tags": manifest.tags,
            "allow": manifest.allow,
            "entrypoint": {
                "function_name": manifest.entrypoint.function_name,
                "input_serializer": manifest.entrypoint.input_serializer,
                "output_serializer": manifest.entrypoint.output_serializer,
            },
            "functions": {
                name: function_view(manifest.functions[name])
                for name in ("manifest_application", "manifest_child")
            },
        },
        sort_keys=True,
    )
)
