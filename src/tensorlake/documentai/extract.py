"""
This module defines the data structures used for structured data extraction.
"""

import json
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, Json, field_validator

from tensorlake.documentai.common import TableParsingStrategy


class ModelProvider(str, Enum):
    """
    The model provider to use for structured data extraction.

    TENSORLAKE: private models, running on Tensorlake infrastructure.
    SONNET: Claude 3.5 Sonnet model.
    GPT4OMINI: GPT-4o-mini model.
    """

    TENSORLAKE = "tensorlake"
    SONNET = "claude-3-5-sonnet-latest"
    GPT4OMINI = "gpt-4o-mini"


class ExtractionOptions(BaseModel):
    """
    Options for structured data extraction.

    Args:
        json_schema: The JSON schema to guide structured data extraction from the file.
        model: The model provider to use for structured data extraction.. Defaults to ModelProvider.TENSORLAKE.
        deliver_webhook: Whether to deliver the result to a webhook. Defaults to False.
        prompt: Override the prompt to customize structured extractions. Use this if you want to extract data froma file using a different prompt than the one we use to extract.
        table_parsing_strategy: The algorithm to use for parsing tables in the document. Defaults to TableParsingStrategy.TSR.
    """

    json_schema: Json = Field(alias="jsonSchema")
    model: ModelProvider = Field(
        alias="modelProvider", default=ModelProvider.TENSORLAKE
    )
    deliver_webhook: bool = Field(alias="deliverWebhook", default=False)
    prompt: Optional[str] = None
    table_parsing_strategy: TableParsingStrategy = Field(
        alias="tableParsingStrategy", default=TableParsingStrategy.TSR
    )

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("json_schema", mode="before")
    @classmethod
    def transform(cls, raw: dict) -> Json:
        return json.dumps(raw)
