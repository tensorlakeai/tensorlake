import os

from tensorlake.applications import run_remote_application


def test_remote_invocation():
    """
    Tests invoking the customer_support application remotely on Tensorlake Cloud.
    """
    feedbacks = {
        "customer_a": "The product was great!",
        "customer_b": "I am very disappointed with the service.",
        "customer_c": "It worked perfectly, thank you!",
    }
    print(f"Invoking customer_support remotely for feedbacks:\n{feedbacks}\n")

    try:
        # remote=True is implied when using run_remote_application, but we pass the function object
        # which allows the SDK to resolve the name automatically.
        request = run_remote_application("customer_support", feedbacks)

        print(f"Request ID: {request.id}")
        print("Waiting for completion (this may take a moment as it calls OpenAI)...")

        response = request.output()

        print("\n" + "=" * 50)
        print("REMOTE RESPONSE")
        print("=" * 50 + "\n")
        print(response)

    except Exception as e:
        print(f"\nError invoking remote application: {e}")
        print("Ensure TENSORLAKE_API_KEY is set and the application is deployed.")


if __name__ == "__main__":
    if "TENSORLAKE_API_KEY" not in os.environ:
        print(
            "Warning: TENSORLAKE_API_KEY is not set. Remote invocation will likely fail."
        )

    test_remote_invocation()
