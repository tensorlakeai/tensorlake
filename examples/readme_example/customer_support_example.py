import os
from typing import Dict
from openai import OpenAI
from tensorlake.applications import application, function, run_local_application, Image

image = Image(base_image="python:3.11-slim", name="openai_story_writer").run(
    "pip install openai"
)


@function(
    description="Analyzes the sentiment of input text",
    secrets=["OPENAI_API_KEY"],
    image=image,
)
def analyze_sentiment(feedback: str) -> str:
    """Step 1: Analyze the sentiment of the input text."""
    print(f"Analyzing: {feedback}")
    try:
        client = OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are a sentiment analyzer. Respond with only one word: POSITIVE or NEGATIVE.",
                },
                {"role": "user", "content": feedback},
            ],
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Warning: OpenAI API call failed ({e}). Using mock sentiment.")
        return "POSITIVE" if "great" in feedback.lower() else "NEGATIVE"


@function(
    description="Drafts a customer support email based on the sentiment.",
    secrets=["OPENAI_API_KEY"],
    image=image,
)
def draft_response(sentiment: str) -> str:
    """Step 2: Draft a customer support email based on the sentiment."""
    print(f"Drafting email for {sentiment} feedback...")
    if sentiment == "NEGATIVE":
        prompt = "Write a short, empathetic apology email to a customer."
    else:
        prompt = "Write a short, enthusiastic thank you email to a customer."

    try:
        client = OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Warning: OpenAI API call failed ({e}). Using mock response.")
        return f"Dear Customer, {prompt} (Mock Response)"


@application(
    tags={"type": "quickstart", "use_case": "customer_support"},
)
@function(description="Customer Support application.", image=image)
def customer_support(feedbacks: Dict[str, str]) -> Dict[str, str]:
    """
    Main application workflow:
    1. Analyze sentiment for all feedbacks in parallel.
    2. Draft responses for all sentiments in parallel.
    3. Aggregate results into a dictionary.
    """
    names = list(feedbacks.keys())
    feedback_texts = list(feedbacks.values())

    # 1. Analyze sentiment in parallel
    sentiments = analyze_sentiment.map(feedback_texts)

    # 2. Draft responses in parallel
    responses = draft_response.map(sentiments)

    # 3. Compile: Return dictionary
    return dict(zip(names, responses))


if __name__ == "__main__":
    # Example usage
    FEEDBACKS = {
        "customer_a": "The product was great!",
        "customer_b": "I am very disappointed with the service.",
        "customer_c": "It worked perfectly, thank you!",
    }

    print(f"Generating responses for: {FEEDBACKS}\n")

    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set.")
        exit(1)

    try:
        # Run locally using Tensorlake's local runner
        request = run_local_application(customer_support, FEEDBACKS)
        response = request.output()

        print("\n" + "=" * 50)
        print("FINAL RESPONSE")
        print("=" * 50 + "\n")
        print(response)

    except Exception as e:
        print(f"Error: {e}")
