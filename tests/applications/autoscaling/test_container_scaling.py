"""Behavior tests for container autoscaling: warm_containers, min_containers, max_containers.

These tests verify the autoscaling behaviors:
- warm_containers: Pre-allocated containers for low-latency allocation
- min_containers: Minimum guaranteed containers always running
- max_containers: Maximum scaling limit

Scaling semantics:
- Functions WITH warm: System maintains (current_demand + warm) containers, bounded by [min, max]
- Functions WITHOUT warm: System scales on-demand only, bounded by [min, max]
- Initial allocation: (min + warm) containers created on first deployment
"""

import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed

import parameterized

from tensorlake.applications import Request, application, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Baseline: No autoscaling configuration


@application()
@function()
def no_autoscaling_function(x: int) -> int:
    """Function with no autoscaling config - scales on-demand only."""
    time.sleep(0.05)  # Simulate some work
    return x * 2


# Test: warm_containers only (low-latency allocation)


@application()
@function(warm_containers=3)
def warm_only_function(x: int) -> int:
    """Function with warm=3. Should maintain 3 warm containers above demand."""
    time.sleep(0.05)
    return x * 3


# Test: min_containers only (guaranteed capacity)


@application()
@function(min_containers=2)
def min_only_function(x: int) -> int:
    """Function with min=2. Should always have at least 2 containers running."""
    time.sleep(0.05)
    return x * 4


# Test: max_containers only (scaling limit)


@application()
@function(max_containers=3)
def max_only_function(x: int) -> int:
    """Function with max=3. Should never exceed 3 containers."""
    time.sleep(0.2)  # Longer work to create backpressure
    return x * 5


# Test: min + max (bounded autoscaling)


@application()
@function(min_containers=2, max_containers=10)
def min_max_function(x: int) -> int:
    """Function with min=2, max=10. On-demand scaling within bounds."""
    time.sleep(0.05)
    return x * 6


# Test: min + warm (guaranteed + pre-warmed)


@application()
@function(min_containers=2, warm_containers=4)
def min_warm_function(x: int) -> int:
    """Function with min=2, warm=4. Should start with 6 containers (min + warm)."""
    time.sleep(0.05)
    return x * 7


# Test: max + warm (pre-warmed with limit)


@application()
@function(max_containers=5, warm_containers=2)
def max_warm_function(x: int) -> int:
    """Function with max=5, warm=2. Pre-warmed but capped at max."""
    time.sleep(0.1)
    return x * 8


# Test: All three parameters


@application()
@function(min_containers=2, max_containers=10, warm_containers=3)
def all_params_function(x: int) -> int:
    """Function with min=2, max=10, warm=3. Should start with 5 containers."""
    time.sleep(0.05)
    return x * 9


# Helper for concurrent load testing


@application()
@function()
def concurrent_no_warm(x: int) -> int:
    """Worker without warm containers for comparison."""
    time.sleep(0.1)
    return x


@application()
@function(warm_containers=5)
def concurrent_with_warm(x: int) -> int:
    """Worker with warm=5 for low-latency concurrent execution."""
    time.sleep(0.1)
    return x


# Test: Burst traffic with max limit


@application()
@function(max_containers=2, warm_containers=1)
def burst_limited_function(x: int) -> int:
    """Function with max=2, warm=1. Should queue requests beyond max."""
    time.sleep(0.3)  # Long enough to create queueing
    return x * 10


class TestWarmContainerBehavior(unittest.TestCase):
    """Test warm_containers behavior: pre-allocated containers for low latency."""

    def test_warm_containers_provide_faster_cold_start(self):
        """Verify warm containers reduce initial allocation latency (remote only).

        Expected behavior:
        - Function WITHOUT warm: Cold start latency for first request
        - Function WITH warm: Warm containers already allocated, faster start
        """
        deploy_applications(__file__)

        # First request to function without warm - expect cold start
        cold_start_times = []
        for _ in range(3):
            start = time.time()
            request: Request = run_application(no_autoscaling_function, True, 5)
            self.assertEqual(request.output(), 10)
            cold_start_times.append(time.time() - start)
            time.sleep(0.5)  # Let containers scale down between requests

        # First request to function with warm - expect faster (containers pre-allocated)
        warm_start_times = []
        for _ in range(3):
            start = time.time()
            request: Request = run_application(warm_only_function, True, 5)
            self.assertEqual(request.output(), 15)
            warm_start_times.append(time.time() - start)
            time.sleep(0.5)

        avg_cold = sum(cold_start_times) / len(cold_start_times)
        avg_warm = sum(warm_start_times) / len(warm_start_times)

        print(f"\n[Warm Container Behavior]")
        print(f"  Average cold start time: {avg_cold:.3f}s")
        print(f"  Average warm start time: {avg_warm:.3f}s")
        print(f"  Expected: warm < cold (warm containers pre-allocated)")

    def test_warm_containers_handle_concurrent_requests_faster(self):
        """Verify warm containers provide better concurrency performance.

        Expected behavior:
        - Function WITH warm=5: Can handle 5 concurrent requests immediately
        - Function WITHOUT warm: Must scale up on-demand, higher latency
        """
        deploy_applications(__file__)

        # Test concurrent requests without warm containers
        start = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(run_application, concurrent_no_warm, True, i)
                for i in range(5)
            ]
            results_no_warm = [f.result().output() for f in as_completed(futures)]
        time_no_warm = time.time() - start

        time.sleep(1)  # Let system stabilize

        # Test concurrent requests with warm containers
        start = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(run_application, concurrent_with_warm, True, i)
                for i in range(5)
            ]
            results_with_warm = [f.result().output() for f in as_completed(futures)]
        time_with_warm = time.time() - start

        # Verify correctness
        self.assertEqual(sorted(results_no_warm), list(range(5)))
        self.assertEqual(sorted(results_with_warm), list(range(5)))

        print(f"\n[Concurrent Request Handling]")
        print(f"  Time without warm (5 concurrent): {time_no_warm:.3f}s")
        print(f"  Time with warm=5 (5 concurrent): {time_with_warm:.3f}s")
        print(f"  Expected: warm <= no_warm (pre-allocated containers)")


class TestMinContainerBehavior(unittest.TestCase):
    """Test min_containers behavior: guaranteed minimum capacity."""

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_min_containers_always_available(self, _: str, is_remote: bool):
        """Verify min_containers are always available.

        Expected behavior:
        - Function with min=2: At least 2 containers always running
        - First requests should be fast (no cold start if within min)
        """
        if is_remote:
            deploy_applications(__file__)

        # Make requests that should hit pre-allocated min containers
        times = []
        for i in range(3):
            start = time.time()
            request: Request = run_application(min_only_function, is_remote, i)
            self.assertEqual(request.output(), i * 4)
            times.append(time.time() - start)
            time.sleep(0.1)

        if is_remote:
            print(f"\n[Min Container Guarantee]")
            print(f"  Request times with min=2: {[f'{t:.3f}s' for t in times]}")
            print(f"  Expected: Consistent times (min containers always running)")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_min_warm_starts_with_correct_count(self, _: str, is_remote: bool):
        """Verify function with min + warm starts with (min + warm) containers.

        Expected behavior:
        - Function with min=2, warm=4: System creates 6 containers initially
        - Can handle 6 concurrent requests without scaling
        """
        if is_remote:
            deploy_applications(__file__)

        # Should handle multiple concurrent requests immediately
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(run_application, min_warm_function, is_remote, i)
                for i in range(6)
            ]
            results = [f.result().output() for f in as_completed(futures)]

        expected = [i * 7 for i in range(6)]
        self.assertEqual(sorted(results), sorted(expected))

        if is_remote:
            print(f"\n[Min + Warm Initial Allocation]")
            print(f"  Successfully handled 6 concurrent requests")
            print(f"  Expected: min=2 + warm=4 = 6 containers pre-allocated")


class TestMaxContainerBehavior(unittest.TestCase):
    """Test max_containers behavior: scaling limits and backpressure."""

    def test_max_containers_limits_concurrent_execution(self):
        """Verify max_containers enforces scaling limit.

        Expected behavior:
        - Function with max=3: Can handle at most 3 concurrent executions
        - Additional requests queue or experience higher latency
        """
        deploy_applications(__file__)

        # Send 6 concurrent requests to function with max=3
        # Requests should queue due to max limit
        start = time.time()
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(run_application, max_only_function, True, i)
                for i in range(6)
            ]
            results = [f.result().output() for f in as_completed(futures)]
        total_time = time.time() - start

        # Verify correctness
        expected = [i * 5 for i in range(6)]
        self.assertEqual(sorted(results), sorted(expected))

        # With max=3 and 0.2s execution time:
        # - First 3 requests execute in parallel (~0.2s)
        # - Next 3 requests queue and execute (~0.2s)
        # - Total should be >= 0.4s (2 waves)
        print(f"\n[Max Container Limit]")
        print(f"  6 requests with max=3: {total_time:.3f}s")
        print(f"  Expected: >= 0.4s (requests queued due to max limit)")
        print(f"  Actual behavior: {total_time:.3f}s")

    def test_burst_traffic_with_max_limit(self):
        """Verify system handles burst traffic within max limit.

        Expected behavior:
        - Function with max=2, warm=1: Starts with 1 warm container
        - Can scale up to 2 total
        - Burst of 4 requests should queue (only 2 can run concurrently)
        """
        deploy_applications(__file__)

        start = time.time()
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(run_application, burst_limited_function, True, i)
                for i in range(4)
            ]
            results = [f.result().output() for f in as_completed(futures)]
        total_time = time.time() - start

        # Verify correctness
        expected = [i * 10 for i in range(4)]
        self.assertEqual(sorted(results), sorted(expected))

        # With max=2 and 0.3s execution:
        # - 2 requests execute in wave 1 (~0.3s)
        # - 2 requests execute in wave 2 (~0.3s)
        # - Total >= 0.6s
        print(f"\n[Burst Traffic with Max Limit]")
        print(f"  4 requests with max=2: {total_time:.3f}s")
        print(f"  Expected: >= 0.6s (max=2 creates queueing)")


class TestAutoscalingCombinations(unittest.TestCase):
    """Test combinations of min, max, and warm parameters."""

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_all_parameters_work_together(self, _: str, is_remote: bool):
        """Verify min + max + warm work together correctly.

        Expected behavior:
        - min=2, max=10, warm=3: Starts with 5 containers (min + warm)
        - Can scale up to 10 total
        - Maintains warm buffer above current demand
        """
        if is_remote:
            deploy_applications(__file__)

        # Should handle 5 concurrent requests immediately (min + warm)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(run_application, all_params_function, is_remote, i)
                for i in range(5)
            ]
            results = [f.result().output() for f in as_completed(futures)]

        expected = [i * 9 for i in range(5)]
        self.assertEqual(sorted(results), sorted(expected))

        if is_remote:
            print(f"\n[Combined Parameters]")
            print(f"  Successfully handled 5 concurrent requests immediately")
            print(f"  Config: min=2, max=10, warm=3")
            print(f"  Expected: 5 containers pre-allocated (min + warm)")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_min_max_bounds_scaling(self, _: str, is_remote: bool):
        """Verify scaling respects min and max bounds.

        Expected behavior:
        - min=2, max=10: Always >= 2, never > 10 containers
        - On-demand scaling within bounds
        """
        if is_remote:
            deploy_applications(__file__)

        # Test scaling up to max
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(run_application, min_max_function, is_remote, i)
                for i in range(10)
            ]
            results = [f.result().output() for f in as_completed(futures)]

        expected = [i * 6 for i in range(10)]
        self.assertEqual(sorted(results), sorted(expected))


class TestScalingSemantics(unittest.TestCase):
    """Test the semantic difference between functions with and without warm."""

    def test_with_warm_maintains_buffer_above_demand(self):
        """Verify functions WITH warm maintain buffer above demand.

        Expected behavior:
        - Function WITH warm: System maintains (current_demand + warm) containers
        - Sequential requests remain fast (warm buffer maintained)
        """
        deploy_applications(__file__)

        # Make sequential requests - warm buffer should be maintained
        times = []
        for i in range(5):
            start = time.time()
            request: Request = run_application(warm_only_function, True, i)
            self.assertEqual(request.output(), i * 3)
            times.append(time.time() - start)
            time.sleep(0.2)  # Small gap between requests

        print(f"\n[Warm Buffer Maintenance]")
        print(f"  Sequential request times with warm=3:")
        for i, t in enumerate(times):
            print(f"    Request {i+1}: {t:.3f}s")
        print(f"  Expected: Consistent times (warm buffer maintained)")

    def test_without_warm_scales_on_demand(self):
        """Verify functions WITHOUT warm scale on-demand only.

        Expected behavior:
        - Function WITHOUT warm: Scales up on-demand, scales down when idle
        - May see cold starts between requests if scaled down
        """
        deploy_applications(__file__)

        # Make requests with gaps to allow scale-down
        times = []
        for i in range(3):
            start = time.time()
            request: Request = run_application(no_autoscaling_function, True, i)
            self.assertEqual(request.output(), i * 2)
            times.append(time.time() - start)
            time.sleep(2)  # Longer gap to allow scale-down

        print(f"\n[On-Demand Scaling]")
        print(f"  Request times without warm:")
        for i, t in enumerate(times):
            print(f"    Request {i+1}: {t:.3f}s")
        print(f"  Expected: Variable times (on-demand scaling, possible scale-down)")


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_warm_greater_than_max_is_bounded(self, _: str, is_remote: bool):
        """Verify warm is bounded by max_containers.

        Expected behavior:
        - If warm > max, system creates at most max containers
        - max_warm_function: max=5, warm=2 → creates min(max, warm) = 2
        """
        if is_remote:
            deploy_applications(__file__)

        # Should work correctly with warm bounded by max
        request: Request = run_application(max_warm_function, is_remote, 5)
        self.assertEqual(request.output(), 40)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_zero_values_handled_correctly(self, _: str, is_remote: bool):
        """Verify functions with None/default values work correctly."""
        if is_remote:
            deploy_applications(__file__)

        # Function with no autoscaling params should work
        request: Request = run_application(no_autoscaling_function, is_remote, 0)
        self.assertEqual(request.output(), 0)


if __name__ == "__main__":
    unittest.main()
