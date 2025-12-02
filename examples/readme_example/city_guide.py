import os

from agents import Agent, Runner
from agents.tool import WebSearchTool, function_tool
from openai import OpenAI

from tensorlake.applications import Image, application, function, run_local_application

# Define the image with necessary dependencies
image = Image(base_image="python:3.11-slim", name="city_guide_image").run(
    "pip install openai openai-agents"
)


@function(
    description="Gets the weather for a city in Fahrenheit",
    secrets=["OPENAI_API_KEY"],
    image=image,
)
def get_weather(city: str) -> str:
    """Step 1: Get weather in Fahrenheit."""
    print(f"Getting weather for: {city}")

    agent = Agent(
        name="Weather Reporter",
        instructions="You are a weather reporter. Use the web search tool to find the current date and provide the current weather and temperature in Fahrenheit for the given city for today's date. Be concise.",
        tools=[WebSearchTool()],
    )
    result = Runner.run_sync(agent, f"City: {city}")
    return result.final_output.strip()


@function(
    description="Suggests an activity based on the weather using Web Search",
    secrets=["OPENAI_API_KEY"],
    image=image,
)
def get_activity(city: str, weather: str) -> str:
    """Step 2: Suggest an activity based on weather using Web Search."""
    print(f"Finding activity for {city} with weather: {weather}")

    agent = Agent(
        name="Activity Finder",
        instructions="You are a travel guide. Use the web search tool to find one interesting activity to do in the city based on the weather. Be concise.",
        tools=[WebSearchTool()],
    )

    result = Runner.run_sync(agent, f"City: {city}, Weather: {weather}")

    return result.final_output.strip()


@function(
    description="Creates a final guide with appropriate temperature units",
    secrets=["OPENAI_API_KEY"],
    image=image,
)
def create_guide(city: str, weather: str, activity: str) -> str:
    """Step 3: Create final guide with converted temperature if needed."""
    print(f"Creating guide for {city}")

    @function_tool
    def convert_to_celsius(fahrenheit: float) -> float:
        """Converts a temperature from Fahrenheit to Celsius.

        Args:
            fahrenheit: The temperature in Fahrenheit to convert.

        Returns:
            The temperature converted to Celsius.
        """
        print(f"Converting {fahrenheit}F to Celsius")
        return (fahrenheit - 32) * 5.0 / 9.0

    agent = Agent(
        name="Guide Creator",
        instructions="You are a helpful travel assistant. Determine if the city typically uses Celsius, if so use the `convert_to_celsius` tool to convert the temperature in the weather description to Celsius. Only include either Fahrenheit or Celsius in the final output, depending on what the city typically uses. Then combine the weather and activity into a short, friendly guide for the user.",
        tools=[convert_to_celsius],
    )

    result = Runner.run_sync(
        agent, f"City: {city}\nWeather (F): {weather}\nActivity: {activity}"
    )
    return result.final_output.strip()


@application(
    tags={"type": "example", "use_case": "city_guide"},
)
@function(description="City Guide Application", image=image)
def city_guide_app(city: str) -> str:
    """
    Main application workflow:
    1. Get weather in Fahrenheit.
    2. Get activity based on weather using Web Search.
    3. Create final guide with appropriate units.
    """
    # 1. Get weather
    weather = get_weather(city)

    # 2. Get activity
    activity = get_activity(city, weather)

    # 3. Create guide
    guide = create_guide(city, weather, activity)

    return guide


if __name__ == "__main__":
    # Example usage
    CITY = "San Francisco"

    print(f"Generating city guide for: {CITY}\n")

    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set.")
        exit(1)

    # Run locally using Tensorlake's local runner
    request = run_local_application(city_guide_app, CITY)
    response = request.output()

    print("\n" + "=" * 50)
    print("CITY GUIDE")
    print("=" * 50 + "\n")
    print(response)
