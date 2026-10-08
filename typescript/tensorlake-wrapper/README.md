# tensorlake

This package has moved to [`@tensorlakeai/tensorlake`](https://www.npmjs.com/package/@tensorlakeai/tensorlake).

`tensorlake` is kept as a compatibility package: it depends on the same version
of `@tensorlakeai/tensorlake` and re-exports it, so existing imports such as
`import { SandboxClient } from "tensorlake"` and
`import { registerApplication } from "tensorlake/applications"` keep working.

New projects should install the scoped package and import from it directly:

```sh
npm install @tensorlakeai/tensorlake
```

```ts
import { SandboxClient } from "@tensorlakeai/tensorlake";
import { registerApplication } from "@tensorlakeai/tensorlake/applications";
```
