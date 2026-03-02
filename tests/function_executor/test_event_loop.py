import unittest

from tensorlake.applications import InternalError, RequestContext
from tensorlake.applications.interface.exceptions import TensorlakeError
from tensorlake.applications.interface.function import (
    Function,
    _FunctionConfiguration,
)
from tensorlake.applications.interface.image import Image
from tensorlake.applications.internal_logger import InternalLogger
from tensorlake.applications.registry import (
    _function_registry,
    register_function,
)
from tensorlake.applications.runtime_hooks import (
    clear_await_future_hook,
    clear_coroutine_to_future_hook,
    clear_register_coroutine_hook,
    clear_run_future_hook,
    clear_wait_futures_hook,
    set_await_future_hook,
    set_coroutine_to_future_hook,
    set_register_coroutine_hook,
    set_run_future_hook,
    set_wait_futures_hook,
)
from tensorlake.function_executor.allocation_runner.event_loop import (
    AllocationEventLoop,
    FunctionCallCollectionRef,
    FunctionCallRef,
    InputEventFunctionCallCreated,
    InputEventFunctionCallWatcherResult,
    OutputEventBatch,
    OutputEventCreateFunctionCall,
    OutputEventCreateFunctionCallWatcher,
    OutputEventFinishAllocation,
)


def _make_test_function(name: str, fn) -> Function:
    """Create a Function object with minimal config for testing."""
    func = Function(fn)
    func._function_config = _FunctionConfiguration(
        class_name=None,
        class_method_name=None,
        class_init_timeout=None,
        function_name=name,
        description="",
        image=Image(),
        secrets=[],
        retries=None,
        timeout=300,
        cpu=1.0,
        memory=1.0,
        ephemeral_disk=1.0,
        gpu=None,
        region=None,
        cacheable=False,
        max_concurrency=1,
        warm_containers=None,
        min_containers=None,
        max_containers=None,
    )
    return func


def _make_test_logger() -> InternalLogger:
    return InternalLogger(
        context={},
        destination=InternalLogger.LOG_FILE.NULL,
        as_cloud_event=False,
    )


def _make_test_event_loop(
    func: Function, function_call_id: str = "fc_id_1"
) -> AllocationEventLoop:
    return AllocationEventLoop(
        function=func,
        function_call_id=function_call_id,
        allocation_id="test_allocation_id",
        request_context=RequestContext(),
        logger=_make_test_logger(),
    )


class _EventLoopDriver:
    """Helper that drives the AllocationEventLoop from the 'AllocationRunner' side.

    Installs global runtime hooks that forward to the event loop (same as
    service.py does for AllocationRunner), starts the event loop, and
    processes commands by delivering pre-configured or callback-generated results.
    """

    def __init__(self, loop: AllocationEventLoop):
        self.loop = loop
        self.command_batches: list[OutputEventBatch] = []
        # Callback: (command) -> result. If None, auto-generate success results.
        self.result_callback = None

    def run(self, args: list, kwargs: dict) -> OutputEventFinishAllocation:
        """Run the event loop to completion, processing all commands."""
        self._install_hooks()
        try:
            return self._run_loop(args, kwargs)
        finally:
            self._clear_hooks()

    def _install_hooks(self) -> None:
        set_run_future_hook(self.loop.run_future_runtime_hook)
        set_wait_futures_hook(self.loop.wait_futures_runtime_hook)
        set_await_future_hook(self.loop.await_future_runtime_hook)
        set_register_coroutine_hook(self.loop.register_coroutine_runtime_hook)
        set_coroutine_to_future_hook(self.loop.coroutine_to_future_runtime_hook)

    def _clear_hooks(self) -> None:
        clear_run_future_hook()
        clear_wait_futures_hook()
        clear_await_future_hook()
        clear_register_coroutine_hook()
        clear_coroutine_to_future_hook()

    def _run_loop(self, args: list, kwargs: dict) -> OutputEventFinishAllocation:
        self.loop.start(args, kwargs)

        while True:
            batch = self.loop.wait_for_output_event_batch()
            self.command_batches.append(batch)
            for cmd in batch.events:
                if isinstance(cmd, OutputEventFinishAllocation):
                    return cmd
                result = self._make_result(cmd)
                self.loop.add_input_event(result)

    def _make_result(self, cmd):
        if self.result_callback is not None:
            return self.result_callback(cmd)

        if isinstance(cmd, OutputEventCreateFunctionCall):
            return InputEventFunctionCallCreated(
                durable_id=cmd.durable_id,
                exception=None,
            )
        elif isinstance(cmd, OutputEventCreateFunctionCallWatcher):
            return InputEventFunctionCallWatcherResult(
                function_call_durable_id=cmd.function_call_durable_id,
                output=f"result_for_{cmd.function_call_durable_id}",
                exception=None,
            )


class TestEventLoopBasic(unittest.TestCase):
    def setUp(self):
        # Save and restore function registry.
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_simple_return_value(self):
        """User function returns a plain value (no futures)."""
        func = _make_test_function("my_func", fn=lambda: 42)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertIsNone(output.tail_call)
        self.assertEqual(output.value, 42)
        # Only the finish batch (no IO commands).
        self.assertEqual(len(driver.command_batches), 1)

    def test_simple_return_value_with_args(self):
        """User function receives and uses args/kwargs."""
        func = _make_test_function("my_func", fn=lambda x, y=0: x + y)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([10], {"y": 5})

        self.assertIsNone(output.user_exception)
        self.assertEqual(output.value, 15)

    def test_user_exception(self):
        """User function raises an exception."""

        def failing_func():
            raise ValueError("user error")

        func = _make_test_function("my_func", fn=failing_func)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsInstance(output.user_exception, ValueError)
        self.assertEqual(str(output.user_exception), "user error")


class TestEventLoopRunFutures(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_single_function_call_future(self):
        """Future.run() generates a single OutputEventCreateFunctionCall."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            future = child_func.future(1, key="val")
            future.run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(len(driver.command_batches), 2)

        batch = driver.command_batches[0]
        self.assertEqual(len(batch.events), 1)

        cmd = batch.events[0]
        self.assertIsInstance(cmd, OutputEventCreateFunctionCall)
        self.assertEqual(cmd.function_name, "child_func")
        self.assertEqual(cmd.args, [1])
        self.assertEqual(cmd.kwargs, {"key": "val"})
        self.assertFalse(cmd.is_tail_call)
        self.assertIsNone(cmd.start_delay)

    def test_function_call_with_future_arg(self):
        """A function call with another future as argument generates FunctionCallRef."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            f1 = child_func.future(1)
            f2 = child_func.future(f1, 2)
            f1.run()
            f2.run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        # f1.run() generates a batch, then f2.run() generates another batch,
        # plus the finish batch.
        self.assertEqual(len(driver.command_batches), 3)

        # First batch: f1
        cmd1 = driver.command_batches[0].events[0]
        self.assertIsInstance(cmd1, OutputEventCreateFunctionCall)
        self.assertEqual(cmd1.function_name, "child_func")
        self.assertEqual(cmd1.args, [1])

        # Second batch: f2 with f1 as FunctionCallRef
        cmd2 = driver.command_batches[1].events[0]
        self.assertIsInstance(cmd2, OutputEventCreateFunctionCall)
        self.assertEqual(cmd2.function_name, "child_func")
        self.assertEqual(len(cmd2.args), 2)
        self.assertIsInstance(cmd2.args[0], FunctionCallRef)
        self.assertEqual(cmd2.args[0].durable_id, cmd1.durable_id)
        self.assertEqual(cmd2.args[1], 2)

    def test_creation_failure_raises(self):
        """Failed InputEventFunctionCallCreated raises InternalError."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            child_func.future(1).run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def fail_creation(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id,
                    exception=InternalError("server error"),
                )

        driver.result_callback = fail_creation
        output = driver.run([], {})

        self.assertIsInstance(output.user_exception, InternalError)
        self.assertIn("server error", str(output.user_exception))

    def test_map_creation_failure_raises(self):
        """Failed creation of a map child function call raises InternalError."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            child_func.future.map([1, 2, 3]).run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def fail_creation(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id,
                    exception=InternalError("map creation failed"),
                )

        driver.result_callback = fail_creation
        output = driver.run([], {})

        self.assertIsInstance(output.user_exception, InternalError)
        self.assertIn("map creation failed", str(output.user_exception))

    def test_reduce_creation_failure_raises(self):
        """Failed creation of a reduce child function call raises InternalError."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            child_func.future.reduce([10, 20]).run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def fail_creation(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id,
                    exception=InternalError("reduce creation failed"),
                )

        driver.result_callback = fail_creation
        output = driver.run([], {})

        self.assertIsInstance(output.user_exception, InternalError)
        self.assertIn("reduce creation failed", str(output.user_exception))


class TestEventLoopWaitFutures(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_wait_single_future(self):
        """Future.result() generates OutputEventCreateFunctionCallWatcher and sets result."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        def user_code():
            future = child_func.future(1)
            future.run()
            captured["result"] = future.result()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        # Track the durable_id from the creation command for watcher result.
        created_durable_ids = {}

        def result_callback(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                created_durable_ids[cmd.durable_id] = True
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id, exception=None
                )
            elif isinstance(cmd, OutputEventCreateFunctionCallWatcher):
                return InputEventFunctionCallWatcherResult(
                    function_call_durable_id=cmd.function_call_durable_id,
                    output=99,
                    exception=None,
                )

        driver.result_callback = result_callback
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(captured["result"], 99)

        # Should have 3 batches: 1 for run, 1 for wait, 1 for finish.
        self.assertEqual(len(driver.command_batches), 3)
        watcher_cmd = driver.command_batches[1].events[0]
        self.assertIsInstance(watcher_cmd, OutputEventCreateFunctionCallWatcher)

    def test_wait_future_failure(self):
        """InputEventFunctionCallWatcherCreated with failure sets exception on future."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        def user_code():
            future = child_func.future(1)
            future.run()
            try:
                future.result()
            except TensorlakeError as e:
                captured["exception"] = e

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def result_callback(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id, exception=None
                )
            elif isinstance(cmd, OutputEventCreateFunctionCallWatcher):
                return InputEventFunctionCallWatcherResult(
                    function_call_durable_id=cmd.function_call_durable_id,
                    output=None,
                    exception=InternalError("child failed"),
                )

        driver.result_callback = result_callback
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertIn("exception", captured)
        self.assertIsInstance(captured["exception"], InternalError)


class TestEventLoopMapOperation(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_map_generates_batch_of_commands(self):
        """map([1, 2, 3]) generates a batch of 3 OutputEventCreateFunctionCall."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            # Create a map future and run it.
            map_future = child_func.future.map([10, 20, 30])
            map_future.run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(len(driver.command_batches), 2)

        batch = driver.command_batches[0]
        # 3 items mapped = 3 OutputEventCreateFunctionCall.
        call_cmds = [
            c for c in batch.events if isinstance(c, OutputEventCreateFunctionCall)
        ]
        self.assertEqual(len(call_cmds), 3)
        for cmd in call_cmds:
            self.assertEqual(cmd.function_name, "child_func")

    def test_map_result(self):
        """map([1, 2, 3]).result() returns list of results in order."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        def user_code():
            map_future = child_func.future.map([10, 20, 30])
            map_future.run()
            captured["result"] = map_future.result()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        call_order = []

        def result_callback(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                call_order.append(cmd.durable_id)
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id, exception=None
                )
            elif isinstance(cmd, OutputEventCreateFunctionCallWatcher):
                # Return result based on position.
                idx = call_order.index(cmd.function_call_durable_id)
                return InputEventFunctionCallWatcherResult(
                    function_call_durable_id=cmd.function_call_durable_id,
                    output=(idx + 1) * 100,
                    exception=None,
                )

        driver.result_callback = result_callback
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(captured["result"], [100, 200, 300])


class TestEventLoopResolveArg(unittest.TestCase):
    """Tests for _resolve_arg handling of ListFuture and ReduceOperationFuture args."""

    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_map_future_as_arg_generates_collection_ref(self):
        """Passing a map future as arg to another function generates FunctionCallCollectionRef."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        consumer_func = _make_test_function("consumer_func", fn=lambda x: x)
        register_function("consumer_func", consumer_func)

        def user_code():
            map_future = child_func.future.map([10, 20, 30])
            consumer_func.future(map_future).run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        # Find the consumer_func command
        consumer_cmds = []
        map_cmds = []
        for batch in driver.command_batches:
            for cmd in batch.events:
                if isinstance(cmd, OutputEventCreateFunctionCall):
                    if cmd.function_name == "consumer_func":
                        consumer_cmds.append(cmd)
                    elif cmd.function_name == "child_func":
                        map_cmds.append(cmd)

        self.assertEqual(len(map_cmds), 3)
        self.assertEqual(len(consumer_cmds), 1)

        # The consumer's arg should be a FunctionCallCollectionRef
        consumer_arg = consumer_cmds[0].args[0]
        self.assertIsInstance(consumer_arg, FunctionCallCollectionRef)
        self.assertEqual(len(consumer_arg.durable_ids), 3)
        # The durable_ids should match the map command durable_ids
        map_durable_ids = [cmd.durable_id for cmd in map_cmds]
        self.assertEqual(consumer_arg.durable_ids, map_durable_ids)

    def test_reduce_future_as_arg_generates_function_call_ref(self):
        """Passing a reduce future as arg to another function generates FunctionCallRef."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        consumer_func = _make_test_function("consumer_func", fn=lambda x: x)
        register_function("consumer_func", consumer_func)

        def user_code():
            reduce_future = child_func.future.reduce([10, 20])
            consumer_func.future(reduce_future).run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        # Find the consumer_func command
        consumer_cmds = []
        reduce_chain_cmds = []
        for batch in driver.command_batches:
            for cmd in batch.events:
                if isinstance(cmd, OutputEventCreateFunctionCall):
                    if cmd.function_name == "consumer_func":
                        consumer_cmds.append(cmd)
                    elif cmd.function_name == "child_func":
                        reduce_chain_cmds.append(cmd)

        self.assertEqual(len(consumer_cmds), 1)
        # reduce([10, 20]) creates 1 reduce call
        self.assertEqual(len(reduce_chain_cmds), 1)

        # The consumer's arg should be a FunctionCallRef pointing to the last reduce step
        consumer_arg = consumer_cmds[0].args[0]
        self.assertIsInstance(consumer_arg, FunctionCallRef)
        self.assertEqual(consumer_arg.durable_id, reduce_chain_cmds[0].durable_id)

    def test_single_item_reduce_as_arg_passes_value_through(self):
        """Passing a single-item reduce future as arg passes the plain value through."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        consumer_func = _make_test_function("consumer_func", fn=lambda x: x)
        register_function("consumer_func", consumer_func)

        def user_code():
            reduce_future = child_func.future.reduce([42])
            consumer_func.future(reduce_future).run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        # Find the consumer_func command
        consumer_cmds = [
            cmd
            for batch in driver.command_batches
            for cmd in batch.events
            if isinstance(cmd, OutputEventCreateFunctionCall)
            and cmd.function_name == "consumer_func"
        ]
        self.assertEqual(len(consumer_cmds), 1)

        # Single-item reduce with plain value: the arg should be the raw value 42
        self.assertEqual(consumer_cmds[0].args[0], 42)


class TestEventLoopReduceOperation(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_reduce_two_items(self):
        """reduce([a, b]) generates a single OutputEventCreateFunctionCall (a, b)."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            reduce_future = child_func.future.reduce([10, 20])
            reduce_future.run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(len(driver.command_batches), 2)

        call_cmds = [
            c
            for c in driver.command_batches[0].events
            if isinstance(c, OutputEventCreateFunctionCall)
        ]
        self.assertEqual(len(call_cmds), 1)
        self.assertEqual(call_cmds[0].args, [10, 20])

    def test_reduce_three_items(self):
        """reduce([a, b, c]) generates a chain: reduce(a, b) -> reduce(result, c)."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            reduce_future = child_func.future.reduce([10, 20, 30])
            reduce_future.run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(len(driver.command_batches), 2)

        call_cmds = [
            c
            for c in driver.command_batches[0].events
            if isinstance(c, OutputEventCreateFunctionCall)
        ]
        # reduce(a, b) and reduce(result_of_ab, c) = 2 commands.
        self.assertEqual(len(call_cmds), 2)
        # First: plain args.
        self.assertEqual(call_cmds[0].args, [10, 20])
        # Second: first arg is FunctionCallRef to first command.
        self.assertIsInstance(call_cmds[1].args[0], FunctionCallRef)
        self.assertEqual(call_cmds[1].args[0].durable_id, call_cmds[0].durable_id)
        self.assertEqual(call_cmds[1].args[1], 30)

    def test_reduce_with_initial(self):
        """reduce([a, b], initial=init) generates chain: reduce(init, a) -> reduce(result, b)."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            reduce_future = child_func.future.reduce([10, 20], 0)
            reduce_future.run()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        call_cmds = [
            c
            for c in driver.command_batches[0].events
            if isinstance(c, OutputEventCreateFunctionCall)
        ]
        # reduce(0, 10) and reduce(result, 20) = 2 commands.
        self.assertEqual(len(call_cmds), 2)
        self.assertEqual(call_cmds[0].args, [0, 10])

    def test_reduce_single_item_no_initial(self):
        """reduce([a]) with no initial skips to the single item."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        def user_code():
            reduce_future = child_func.future.reduce([42])
            reduce_future.run()
            captured["result"] = reduce_future.result()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        # Single item reduce doesn't generate any IO commands, only the finish batch.
        self.assertEqual(len(driver.command_batches), 1)
        self.assertEqual(captured["result"], 42)

    def test_reduce_result(self):
        """reduce([a, b]).result() waits for chain and returns final result."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        def user_code():
            reduce_future = child_func.future.reduce([10, 20])
            reduce_future.run()
            captured["result"] = reduce_future.result()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def result_callback(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id, exception=None
                )
            elif isinstance(cmd, OutputEventCreateFunctionCallWatcher):
                return InputEventFunctionCallWatcherResult(
                    function_call_durable_id=cmd.function_call_durable_id,
                    output=30,
                    exception=None,
                )

        driver.result_callback = result_callback
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(captured["result"], 30)


class TestEventLoopTailCall(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_tail_call_function_call(self):
        """Returning a Future produces tail call output."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            return child_func.future(1)

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertIsNotNone(output.tail_call)

        # Tail call is split into two batches: first creates function calls,
        # second finishes the allocation with the tail call reference.
        self.assertEqual(len(driver.command_batches), 2)
        create_batch = driver.command_batches[0]
        tail_call_cmds = [
            c
            for c in create_batch.events
            if isinstance(c, OutputEventCreateFunctionCall)
        ]
        self.assertEqual(len(tail_call_cmds), 1)
        self.assertEqual(tail_call_cmds[0].function_name, "child_func")
        self.assertTrue(tail_call_cmds[0].is_tail_call)
        self.assertEqual(output.tail_call.durable_id, tail_call_cmds[0].durable_id)

    def test_tail_call_reduce_creation_failure(self):
        """Tail call returning a reduce future with creation failure reports internal_exception."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            return child_func.future.reduce([10, 20])

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def fail_creation(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id,
                    exception=InternalError("tail call reduce failed"),
                )

        driver.result_callback = fail_creation
        output = driver.run([], {})

        self.assertIsInstance(output.internal_exception, InternalError)
        self.assertIn("tail call reduce failed", str(output.internal_exception))

    def test_tail_call_already_started_raises(self):
        """Returning an already-started Future raises SDKUsageError."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            future = child_func.future(1)
            future.run()
            return future

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNotNone(output.user_exception)


class TestEventLoopDeterminism(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_same_inputs_same_commands(self):
        """Running the same function twice produces identical command sequences."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        def user_code():
            f1 = child_func.future(1, key="a")
            f2 = child_func.future(2, key="b")
            f1.run()
            f2.run()

        func = _make_test_function("my_func", fn=user_code)

        # Run 1
        loop1 = _make_test_event_loop(func)
        driver1 = _EventLoopDriver(loop1)
        driver1.run([], {})

        # Run 2
        loop2 = _make_test_event_loop(func)
        driver2 = _EventLoopDriver(loop2)
        driver2.run([], {})

        # Same number of batches.
        self.assertEqual(len(driver1.command_batches), len(driver2.command_batches))

        for b1, b2 in zip(driver1.command_batches, driver2.command_batches):
            self.assertEqual(len(b1.events), len(b2.events))
            for c1, c2 in zip(b1.events, b2.events):
                self.assertEqual(type(c1), type(c2))
                if isinstance(c1, OutputEventCreateFunctionCall):
                    self.assertEqual(c1.durable_id, c2.durable_id)
                    self.assertEqual(c1.function_name, c2.function_name)
                    self.assertEqual(c1.is_tail_call, c2.is_tail_call)


class TestEventLoopAsync(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_async_function_return_value(self):
        """Async user function returning a plain value."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)

        async def user_code():
            return 42

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(output.value, 42)

    def test_async_function_with_await(self):
        """Async function that awaits a future."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        async def user_code():
            result = await child_func.future(1)
            captured["result"] = result
            return result

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        driver = _EventLoopDriver(loop)

        def result_callback(cmd):
            if isinstance(cmd, OutputEventCreateFunctionCall):
                return InputEventFunctionCallCreated(
                    durable_id=cmd.durable_id, exception=None
                )
            elif isinstance(cmd, OutputEventCreateFunctionCallWatcher):
                return InputEventFunctionCallWatcherResult(
                    function_call_durable_id=cmd.function_call_durable_id,
                    output=99,
                    exception=None,
                )

        driver.result_callback = result_callback
        output = driver.run([], {})

        self.assertIsNone(output.user_exception)
        self.assertEqual(output.value, 99)
        self.assertEqual(captured["result"], 99)


class TestEventLoopResultDeliveryOrder(unittest.TestCase):
    def setUp(self):
        self._saved_registry = dict(_function_registry)

    def tearDown(self):
        clear_run_future_hook()
        clear_wait_futures_hook()
        clear_await_future_hook()
        clear_register_coroutine_hook()
        clear_coroutine_to_future_hook()
        _function_registry.clear()
        _function_registry.update(self._saved_registry)

    def test_results_delivered_in_reverse_order(self):
        """Results can be delivered in any order; batch unblocks when all arrive."""
        child_func = _make_test_function("child_func", fn=lambda x: x)
        register_function("child_func", child_func)
        captured = {}

        def user_code():
            map_future = child_func.future.map([1, 2, 3])
            map_future.run()
            captured["result"] = map_future.result()

        func = _make_test_function("my_func", fn=user_code)

        loop = _make_test_event_loop(func)
        set_run_future_hook(loop.run_future_runtime_hook)
        set_wait_futures_hook(loop.wait_futures_runtime_hook)
        set_await_future_hook(loop.await_future_runtime_hook)
        set_register_coroutine_hook(loop.register_coroutine_runtime_hook)
        set_coroutine_to_future_hook(loop.coroutine_to_future_runtime_hook)

        # Manually drive the event loop to deliver results in reverse order.
        loop.start([], {})

        # Batch 1: creation commands (from run).
        batch1 = loop.wait_for_output_event_batch()
        self.assertEqual(len(batch1.events), 3)

        # Deliver creation results in order (required by durable ID ordering contract).
        for cmd in batch1.events:
            loop.add_input_event(
                InputEventFunctionCallCreated(durable_id=cmd.durable_id, exception=None)
            )

        # Watcher batches: each map item is waited for sequentially
        # (one watcher command per batch).
        creation_durable_ids = [cmd.durable_id for cmd in batch1.events]
        for i, expected_durable_id in enumerate(creation_durable_ids):
            watcher_batch = loop.wait_for_output_event_batch()
            self.assertEqual(len(watcher_batch.events), 1)
            watcher_cmd = watcher_batch.events[0]
            self.assertIsInstance(watcher_cmd, OutputEventCreateFunctionCallWatcher)
            self.assertEqual(watcher_cmd.function_call_durable_id, expected_durable_id)
            loop.add_input_event(
                InputEventFunctionCallWatcherResult(
                    function_call_durable_id=watcher_cmd.function_call_durable_id,
                    output=(i + 1) * 10,
                    exception=None,
                )
            )

        # Wait for finish.
        final_batch = loop.wait_for_output_event_batch()
        self.assertEqual(len(final_batch.events), 1)
        finish_cmd = final_batch.events[0]
        self.assertIsInstance(finish_cmd, OutputEventFinishAllocation)

        # Results should be in original command order.
        self.assertIsNone(finish_cmd.user_exception)
        self.assertEqual(captured["result"], [10, 20, 30])


if __name__ == "__main__":
    unittest.main()
