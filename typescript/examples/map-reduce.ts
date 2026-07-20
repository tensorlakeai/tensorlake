import { registerApplication, registerFunction } from "tensorlake/applications";

const square = registerFunction(
  "map_reduce_square",
  async (value: number): Promise<number> => value * value,
  {
    description: "Squares one value in an isolated function sandbox",
  },
);

const add = registerFunction(
  "map_reduce_add",
  async (total: number, value: number): Promise<number> => total + value,
  {
    description: "Adds one mapped value to the running total",
  },
);

export const mapReduce = registerApplication(
  "typescript_map_reduce",
  async (values: number[]): Promise<{ squares: number[]; sumOfSquares: number }> => {
    // map fans these calls out through the function-executor protocol.
    const squares = await square.map(values);

    // reduce creates a sequential durable-call chain. An empty input returns 0.
    const sumOfSquares = await add.reduce(squares, 0);

    return { squares, sumOfSquares };
  },
  {
    description: "Squares values in parallel, then sums the squares sequentially",
    tags: { example: "typescript", feature: "map-reduce" },
  },
);
