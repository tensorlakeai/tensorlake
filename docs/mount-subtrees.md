# Mount a directory from a filesystem

Select an existing directory within a registered filesystem when creating a sandbox:

```sh
tl sbx create --filesystem 'data:/mnt/models:source_path=/models'
```

Or attach it to a running sandbox:

```sh
tl sbx fs attach SANDBOX --id data --path /mnt/models --source-path /models
tl sbx fs ls SANDBOX
tl sbx fs ls SANDBOX --json
```

`source_path` selects a directory inside the filesystem; `--path` selects where
that directory appears in the sandbox. Paths must be absolute and canonical:
no `.` or `..` components, repeated slashes, trailing slash (except `/`), or
control characters. The selected directory must exist. Omission mounts `/` and
omits the new field from requests, preserving existing whole-filesystem commands.

The create option can be combined with `ro` and `prefetch`, including snapshot
pins: `data@SNAPSHOT:/mnt/models:ro,source_path=/models`. Quote arguments with
spaces. For source paths containing commas, use `sbx fs attach --source-path`;
commas delimit create-time options.

Listings show both source and mount paths; JSON preserves `source_path` returned
by the server. Older responses without it mean `/`. Subtree selection requires
the corresponding server and guest runtime support. Updating the CLI alone does
not update the guest code inside an existing memory snapshot.

The Rust SDK exposes `FileSystemMount.source_path` for create and pool claim,
and `attach_file_system_with_request` for runtime attach. The existing positional
`attach_file_system` method continues to select the whole filesystem.
