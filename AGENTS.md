# Tensorlake SDK agent guide

Read [README.md](README.md) for the SDK overview and public API examples.

Use product terms in user documentation, comments, and user-facing messages because sandbox runtimes are internal platform details. Map Firecracker (FC) to **non-CAS sandbox**, Cloud Hypervisor (CH) to **CPU-only CAS sandbox**, and gVisor to **GPU CAS sandbox**. The SDK may expose runtime identifiers for inspection and diagnostics, but they are not the primary user-visible terms. Once migration to CAS is complete, the product terms will be **CPU-only sandboxes** and **GPU sandboxes**.
