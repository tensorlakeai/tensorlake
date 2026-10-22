"""Resolve integration-test scope without logging the environment API key."""

import json
import os
import urllib.request

api_url = os.environ["TENSORLAKE_API_URL"]
api_key = os.environ["TENSORLAKE_API_KEY"]
if not api_key:
    raise RuntimeError(
        "Configure TENSORLAKE_API_KEY in the selected GitHub environment"
    )
request = urllib.request.Request(
    f"{api_url}/platform/v1/keys/introspect",
    data=b"{}",
    method="POST",
    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
)
with urllib.request.urlopen(request, timeout=30) as response:
    scope = json.load(response)
organization_id = scope["organizationId"]
project_id = scope["projectId"]
expected_project_id = os.environ.get("TENSORLAKE_EXPECTED_PROJECT_ID")
if expected_project_id and project_id != expected_project_id:
    raise RuntimeError(
        f"Test key project {project_id} does not match {expected_project_id}"
    )
with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as env_file:
    env_file.write(f"TENSORLAKE_ORGANIZATION_ID={organization_id}\n")
    env_file.write(f"TENSORLAKE_PROJECT_ID={project_id}\n")
print(
    f"Integration tests: {api_url}, organization {organization_id}, project {project_id}"
)
