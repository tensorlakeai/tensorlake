from tensorlake.vendor.nanoid import generate as nanoid


def generate_public_endpoint_id() -> str:
    return f"endpoint_{nanoid()}"
