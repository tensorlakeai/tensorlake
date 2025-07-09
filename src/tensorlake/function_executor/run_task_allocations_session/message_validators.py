from ..proto.function_executor_pb2 import (
    RunTaskAllocationsSessionClientMessage,
    SerializedObjectChunk,
    SerializedObjectID,
    SerializedObjectManifest,
    TaskAllocationInput,
)
from ..proto.google.rpc.status_pb2 import Status
from ..proto.message_validator import MessageValidator


def validate_client_session_message(
    message: RunTaskAllocationsSessionClientMessage,
) -> None:
    """Validates the supplied RunTaskAllocationsSessionClientMessage.

    Raises ValueError if the description is not valid.
    """
    if message.HasField("open_session_request"):
        MessageValidator(message.open_session_request).required_field("session_id")
    elif message.HasField("upload_serialized_object_request"):
        if message.upload_serialized_object_request.HasField("manifest"):
            validate_serialized_object_manifest(
                message.upload_serialized_object_request.manifest
            )
        elif message.upload_serialized_object_request.HasField("chunk"):
            validate_serialized_object_chunk(
                message.upload_serialized_object_request.chunk
            )
        else:
            raise ValueError(
                "UploadSerializedObjectRequest must contain either manifest or chunk"
            )
    elif message.HasField("upload_serialized_object_response"):
        (
            MessageValidator(message.upload_serialized_object_response)
            .required_field("status")
            .required_field("id")
        )
        validate_serialized_object_id(message.upload_serialized_object_response.id)
        validate_status(message.upload_serialized_object_response.status)
    elif message.HasField("run_task_allocations_request"):
        if len(message.run_task_allocations_request.allocations) != 1:
            # This is required until we implement batching of allocations.
            raise ValueError(
                "RunTaskAllocationsRequest must contain exactly one allocation"
            )
        for allocation_input in message.run_task_allocations_request.allocations:
            validate_task_allocation_input(allocation_input)
    elif message.HasField("set_invocation_state_response"):
        MessageValidator(message.set_invocation_state_response).required_field("status")
        validate_status(message.set_invocation_state_response.status)
    elif message.HasField("get_invocation_state_response"):
        (
            MessageValidator(message.get_invocation_state_response)
            .required_field("key")
            .required_serialized_object("value")
            .required_field("status")
        )
        validate_status(message.get_invocation_state_response.status)
    elif message.HasField("leave_session_request"):
        MessageValidator(message.leave_session_request).required_field("close")
    else:
        raise ValueError(
            f"Unknown ClientSessionMessage.message {message.WhichOneof('message')}"
        )


def validate_serialized_object_id(id: SerializedObjectID) -> None:
    """Validates the supplied SerializedObjectID.

    Raises ValueError if the description is not valid.
    """
    MessageValidator(id).required_field("value")


def validate_serialized_object_manifest(manifest: SerializedObjectManifest) -> None:
    """Validates the supplied SerializedObjectManifest.

    Raises ValueError if the manifest is not valid.
    """
    (
        MessageValidator(manifest)
        .required_field("id")
        .required_field("encoding")
        .required_field("encoding_version")
        .required_field("size")
        .required_field("sha256_hash")
    )
    MessageValidator(manifest.id).required_field("id")


def validate_serialized_object_chunk(chunk: SerializedObjectChunk) -> None:
    """Validates the supplied SerializedObjectChunk.

    Raises ValueError if the chunk is not valid.
    """
    (MessageValidator(chunk).required_field("id").required_field("data"))
    validate_serialized_object_id(chunk.id)


def validate_status(status: Status) -> None:
    """Validates the supplied status.

    Raises ValueError if the status is not valid.
    """
    MessageValidator(status).required_field("code")
    # message is optional


def validate_task_allocation_input(allocation_input: TaskAllocationInput) -> None:
    """Validates the supplied TaskAllocationInput.

    Raises ValueError if the allocation is not valid.
    """
    allocation_input: TaskAllocationInput
    (
        MessageValidator(allocation_input)
        .required_field("graph_invocation_id")
        .required_field("task_id")
        .required_field("allocation_id")
        .required_field("function_input")
    )
    validate_serialized_object_id(allocation_input.function_input)
    if allocation_input.HasField("function_init_value"):
        validate_serialized_object_id(allocation_input.function_init_value)
