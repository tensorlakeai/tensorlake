import { openai } from "@ai-sdk/openai";
import { ToolLoopAgent, stepCountIs, tool } from "ai";
import { registerApplication } from "tensorlake/applications";
import { z } from "zod";

export const aiAgent = registerApplication(
  "typescript_ai_agent",
  async (prompt: string) => {
    const agent = new ToolLoopAgent({
      model: openai("gpt-4o-mini"),
      instructions: [
        "You are a concise, helpful assistant.",
        "Use the analyzeText tool when the user asks about the size or structure of text.",
      ].join(" "),
      tools: {
        analyzeText: tool({
          description: "Counts the characters, words, and lines in a piece of text.",
          inputSchema: z.object({
            text: z.string().describe("The text to analyze"),
          }),
          execute: async ({ text }) => ({
            characters: [...text].length,
            words: text.trim() === "" ? 0 : text.trim().split(/\s+/u).length,
            lines: text === "" ? 0 : text.split(/\r?\n/u).length,
          }),
        }),
      },
      stopWhen: stepCountIs(5),
    });

    const response = await agent.generate({ prompt });
    return response.text;
  },
  {
    secrets: ["OPENAI_API_KEY"],
  },
);
