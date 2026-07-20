import {
  registerApplication,
  registerFunction,
  schema,
} from "tensorlake/applications";

const square = registerFunction(
  async (value: number): Promise<number> => value * value,
  {
    name: "map_reduce_square",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    description: "Squares one value in an isolated function sandbox",
  },
);

const add = registerFunction(
  async (total: number, value: number): Promise<number> => total + value,
  {
    name: "map_reduce_add",
    parameters: [
      schema.parameter("total", schema.number()),
      schema.parameter("value", schema.number()),
    ] as const,
    returns: schema.number(),
    description: "Adds one mapped value to the running total",
  },
);

const mapReduceOutput = schema.object({
  squares: schema.array(schema.number()),
  sumOfSquares: schema.number(),
});

export const mapReduce = registerApplication(
  async (values: number[]): Promise<{ squares: number[]; sumOfSquares: number }> => {
    // map fans these calls out through the function-executor protocol.
    const squares = await square.map(values);

    // reduce creates a sequential durable-call chain. An empty input returns 0.
    const sumOfSquares = await add.reduce(squares, 0);

    return { squares, sumOfSquares };
  },
  {
    name: "typescript_map_reduce",
    parameters: [schema.parameter("values", schema.array(schema.number()))] as const,
    returns: mapReduceOutput,
    description: "Squares values in parallel, then sums the squares sequentially",
    tags: { example: "typescript", feature: "map-reduce" },
  },
);
