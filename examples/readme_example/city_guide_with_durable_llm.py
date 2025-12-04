import asyncio
import os
import subprocess
import sys
import tempfile
from typing import AsyncIterator

from agents import (
    Agent,
    Model,
    ModelResponse,
    Runner,
)
from agents.items import TResponseStreamEvent
from agents.models.openai_provider import OpenAIProvider
from agents.tool import WebSearchTool, function_tool

from tensorlake.applications import (
    Image,
    application,
    cls,
    function,
    run_local_application,
)

# Define the image with necessary OpenAI dependencies.
FUNCTION_CONTAINER_IMAGE = Image(
    base_image="python:3.11-slim", name="city_guide_image"
).run("pip install openai openai-agents")


@function_tool
@function(
    image=FUNCTION_CONTAINER_IMAGE,
    description="Gets the weather for a city in Fahrenheit",
    secrets=["OPENAI_API_KEY"],
)
def get_weather_tool(city: str) -> str:
    """Get the current weather for a city.

    Args:
        city: The name of the city to get weather for.

    Returns:
        A string describing the current weather conditions.
    """
    print(f"Getting weather for: {city}")

    agent = Agent(
        name="Weather Reporter",
        instructions="You are a weather reporter. Use the web search tool to find the current date and provide "
        "the current weather and temperature in Fahrenheit for the given city for today's date. Be concise.",
        tools=[WebSearchTool()],
    )
    result = Runner.run_sync(agent, f"City: {city}")
    return result.final_output.strip()


@function_tool
@function(
    image=FUNCTION_CONTAINER_IMAGE,
    description="Suggests an activity based on the weather using Web Search",
    secrets=["OPENAI_API_KEY"],
)
def get_activity_tool(city: str, weather: str) -> str:
    """Suggest an activity based on the weather.

    Args:
        city: The name of the city.
        weather: The current weather conditions.

    Returns:
        A suggested activity for the given weather.
    """
    print(f"Finding activity for {city} with weather: {weather}")

    agent = Agent(
        name="Activity Finder",
        instructions="You are a travel guide. Use the web search tool to find one interesting activity to do in "
        "the city based on the weather. Be concise.",
        tools=[WebSearchTool()],
    )

    result = Runner.run_sync(agent, f"City: {city}, Weather: {weather}")
    return result.final_output.strip()


@function(
    image=FUNCTION_CONTAINER_IMAGE,
    description="Runs unsafe (i.e. AI-generated) Python code in an isolated Function container and returns output printed by the code",
    timeout=5,  # Allow to run the code for up to 5 seconds as the code is not trusted.
)
def run_unsafe_python_code(python_code: str) -> str:
    print(f"Running unsafe Python code:\n{python_code}")

    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".py", delete=False
    ) as temp_file:
        temp_file.write(python_code)
        temp_file_path = temp_file.name

    try:
        result = subprocess.run(
            [sys.executable, temp_file_path],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    finally:
        os.remove(temp_file_path)


@function_tool
@function(
    image=FUNCTION_CONTAINER_IMAGE,
    description="Creates a final guide with appropriate temperature units",
    secrets=["OPENAI_API_KEY"],
)
def create_guide_tool(city: str, weather: str, activity: str) -> str:
    """Create a city guide combining weather and activity.

    Args:
        city: The name of the city.
        weather: The weather description in Fahrenheit.
        activity: The suggested activity.

    Returns:
        A friendly city guide for the user.
    """

    @function_tool
    def convert_to_celsius_tool(python_code: str) -> float:
        """Converts a temperature from Fahrenheit to Celsius using the provided Python code.

        Args:
            python_code: Python code converts a temperature from Fahrenheit to Celsius. The code prints a single float value.

        Returns:
            The temperature in Celsius.
        """
        return float(run_unsafe_python_code(python_code))

    print(f"Creating guide for {city}")
    agent = Agent(
        name="Guide Creator",
        instructions="You are a helpful travel assistant. Determine if the city typically uses Celsius, if so use the `convert_to_celsius_tool` tool to "
        "convert the temperature in the weather description to Celsius. Only include either Fahrenheit or Celsius in the final output, "
        "depending on what the city typically uses. Then combine the weather and activity into a short, friendly guide for the user.",
        tools=[convert_to_celsius_tool],
    )

    result = Runner.run_sync(
        agent, f"City: {city}\nWeather (F): {weather}\nActivity: {activity}"
    )
    return result.final_output.strip()


TOOLS = [
    get_weather_tool,
    get_activity_tool,
    create_guide_tool,
]


@application(
    tags={"type": "example", "use_case": "city_guide"},
)
@function(
    description="City Guide Application",
    image=FUNCTION_CONTAINER_IMAGE,
)
def city_guide_with_durable_llm_app(city: str) -> str:
    """
    Main application workflow:
    1. Get weather in Fahrenheit.
    2. Get activity based on weather using Web Search.
    3. Create final guide with appropriate units.
    """
    agent = Agent(
        name="Guide Creator",
        instructions="You are a helpful travel assistant. Use the `get_weather_tool`, `get_activity_tool`, and `create_guide_tool` "
        "tools to generate a city guide for the given city. Do not modify what the create_guide_tool returns.",
        tools=TOOLS,
        model=DurableOpenAIGPT51Model(),
    )

    result = Runner.run_sync(agent, f"City: {city}")
    return result.final_output.strip()


# See Model class interface documented at:
# https://openai.github.io/openai-agents-python/ref/models/interface/#agents.models.interface.Model.
@cls()
class DurableOpenAIGPT51Model(Model):
    """Wrapper over OpenAI GPT-5.1 model that uses Tensorlake Functions to implement memoization of LLM calls."""

    def __init__(self):
        self._model: Model = OpenAIProvider().get_model("gpt-5.1")

    async def get_response(self, *args, tools, **kwargs) -> ModelResponse:
        # Drop tools because its _on_invoke_tool is not picklable. Error is:
        # Can't pickle local object 'function_tool.<locals>._create_function_tool.<locals>._on_invoke_tool'
        # We'll use local copy of tools instead inside the Tensorlake function.
        return self._get_response(*args, **kwargs)

    @function(
        secrets=["OPENAI_API_KEY"],
        description="Get LLM response for DurableOpenAIGPT51Model",
        image=FUNCTION_CONTAINER_IMAGE,
    )
    def _get_response(self, *args, **kwargs) -> ModelResponse:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._model.get_response(*args, tools=TOOLS, **kwargs)
            )
        finally:
            loop.close()

    def stream_response(self, *args, **kwargs) -> AsyncIterator[TResponseStreamEvent]:
        # This is used in Chat Completions API, OpenAI Agents SDK uses Responses API by default.
        raise NotImplementedError(
            "Streaming is not supported for DurableOpenAIGPT51Model "
            "because Tensorlake Applications don't support it."
        )


if __name__ == "__main__":
    CITY = "Milan, Italy"
    print(f"Generating city guide for: {CITY}\n")

    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set.")
        exit(1)

    request = run_local_application(city_guide_with_durable_llm_app, CITY)
    response = request.output()

    print("\n" + "=" * 50)
    print("CITY GUIDE")
    print("=" * 50 + "\n")
    print(response)
