from typing import Any, Dict, List

from .manifests.application import ApplicationManifest

_app_manifest_cache: Dict[str, ApplicationManifest] = {}


def has_app_manifest(application_name: str) -> bool:
    global _app_manifest_cache
    return application_name in _app_manifest_cache


def get_app_manifest(application_name: str) -> ApplicationManifest:
    global _app_manifest_cache
    return _app_manifest_cache[application_name]


def set_app_manifest(application_name: str, manifest: ApplicationManifest) -> None:
    global _app_manifest_cache
    _app_manifest_cache[application_name] = manifest
