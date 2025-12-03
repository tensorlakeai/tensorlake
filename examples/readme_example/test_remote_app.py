import os

from tensorlake.applications import run_remote_application


def test_remote_invocation():
    """
    Tests invoking the city_guide_app application remotely on Tensorlake Cloud.
    """
    city = "San Francisco"
    print(f"Invoking city_guide_app remotely for city: {city}\n")

    try:
        # remote=True is implied when using run_remote_application, but we pass the function object
        # which allows the SDK to resolve the name automatically.
        request = run_remote_application("city_guide_app", city)

        print(f"Request ID: {request.id}")
        print("Waiting for completion (this may take a moment as it calls OpenAI)...")

        response = request.output()

        print("\n" + "=" * 50)
        print("CITY GUIDE")
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
