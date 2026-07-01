"""Unit tests for the process-selector back-compat shim (`pid` -> `process`)."""

import warnings

import pytest

from tensorlake.sandbox.sandbox import _PROCESS_ARG_UNSET, _resolve_process_arg


def test_process_arg_accepts_pid_and_name():
    assert _resolve_process_arg(1234, _PROCESS_ARG_UNSET) == "1234"
    assert _resolve_process_arg("web", _PROCESS_ARG_UNSET) == "web"


def test_deprecated_pid_keyword_still_works_with_warning():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _resolve_process_arg(_PROCESS_ARG_UNSET, 42) == "42"
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_both_process_and_pid_is_error():
    with pytest.raises(TypeError):
        _resolve_process_arg(1, 2)


def test_missing_selector_is_error():
    with pytest.raises(TypeError):
        _resolve_process_arg(_PROCESS_ARG_UNSET, _PROCESS_ARG_UNSET)
