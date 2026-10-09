# Node install security

Keep this repository's existing package manager and lockfile format.
For npm, `.npmrc` sets a one-day resolution delay and disables install hooks.
That delay alone does not validate an existing lockfile. Before installing, run:

```sh
node /path/to/repo/.github/scripts/guarded-npm.mjs ci
```

Run the command from the project containing `package-lock.json`. For a new
registry dependency, use `guarded-npm.mjs install -- package@version`; it creates
or updates the lock without running hooks, validates every registry entry and
then performs `npm ci --ignore-scripts`.

The validator requires registry publication timestamps and matching artifact
integrity for direct, transitive and optional packages. Young versions and
missing metadata fail the install. `tensorlake@0.5.144` is always rejected.
Local tarballs require `--allow-local-artifacts` in a workflow that verifies or
builds that artifact. Automatic lifecycle scripts stay disabled; required native
setup must be invoked explicitly after review.

Fresh-release testing uses `--allow-version name@exact.version` on the guarded
command, documented in that workflow. The exception applies only to that exact
package/version. Its transitive dependencies still undergo age validation.
Do not use scope-wide exclusions or disable the delay for the whole test graph.

Existing pnpm projects retain pnpm and their longer release delay where present.
Their policy rechecks frozen lockfiles and requires publication timestamps.


For an intentionally temporary test dependency, `install --temporary` restores the original package manifest and lockfile after validating and installing; it does not relax publication-age checks.
