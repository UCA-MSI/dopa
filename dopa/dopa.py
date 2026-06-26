import concurrent.futures
from configparser import ConfigParser
from inspect import signature
import os
from typing import Any, Callable, Optional, Sequence

import numpy as np

__all__ = ['MalformedArgListError', 'parallelize']

_config = ConfigParser()
_config.read(os.path.join(os.path.dirname(__file__), 'config.cfg'))

try:
    MAX_TH_DEG: int = int(_config['DEFAULT']['MAX_TH_DEG'])
    MAX_PR_DEG: int = int(_config['DEFAULT']['MAX_PR_DEG'])
    MAX_JOB_NUMBER: int = int(_config['DEFAULT']['MAX_JOB_NUMBER'])
except KeyError:
    _cpu = os.cpu_count() or 1
    MAX_TH_DEG = _cpu * 2
    MAX_PR_DEG = max(_cpu - 1, 1)
    MAX_JOB_NUMBER = max(MAX_TH_DEG, MAX_PR_DEG) * 2


class MalformedArgListError(TypeError):
    pass


def _is_sequence(x: Any) -> bool:
    return isinstance(x, (list, tuple))


def _check_all_same_type(sequence: Sequence) -> bool:
    """Check that all elements in sequence share the same type."""
    it = iter(sequence)
    first_type = type(next(it))
    return all(type(x) is first_type for x in it)


def _check_argument_list(runs: Sequence, func: Callable) -> None:
    """
    Validate that all runs items are compatible with the func parameters.
    :param runs: the list of function parameters
    :param func: the target function
    :raises MalformedArgListError: when runs is inconsistent or incompatible with func
    """
    first = runs[0]

    if isinstance(first, np.ndarray):
        if not all(x.shape == first.shape for x in runs):
            raise MalformedArgListError('Inconsistent ndarray shapes across runs')
        return

    if not _check_all_same_type(runs):
        raise MalformedArgListError('Inconsistent types across runs')


def _do_parallel(
    runs: Sequence,
    func: Callable,
    use_threads: bool,
    max_workers: Optional[int] = None,
    max_jobs: Optional[int] = None,
) -> list:
    """
    Execute in parallel the list of jobs.
    :param runs: list of function parameters, one item per call
    :param func: the target function to be parallelized
    :param use_threads: if True uses threads, otherwise processes
    :param max_workers: override pool size
    :param max_jobs: override max queued jobs
    :return: list of results
    """
    workers = max_workers or (MAX_TH_DEG if use_threads else MAX_PR_DEG)
    batch_size = max_jobs or MAX_JOB_NUMBER
    n_params = len(signature(func).parameters)
    unpack = n_params > 1

    executor_cls = (
        concurrent.futures.ThreadPoolExecutor
        if use_threads
        else concurrent.futures.ProcessPoolExecutor
    )
    indexed: list = []

    with executor_cls(max_workers=workers) as executor:
        active: dict = {}  # future -> original index

        def _submit(idx: int, run: Any) -> None:
            future = executor.submit(func, *run) if unpack else executor.submit(func, run)
            active[future] = idx

        for idx, run in enumerate(runs):
            _submit(idx, run)
            if len(active) >= batch_size:
                completed = next(concurrent.futures.as_completed(active))
                indexed.append((active[completed], completed.result()))
                del active[completed]

        for future in concurrent.futures.as_completed(active):
            indexed.append((active[future], future.result()))

    indexed.sort(key=lambda x: x[0])
    return [result for _, result in indexed]


def parallelize(
    runs: Sequence,
    func: Callable,
    use_threads: bool = True,
    max_workers: Optional[int] = None,
    max_jobs: Optional[int] = None,
) -> list:
    """
    Parallelizes ``func`` over the argument sets in ``runs``.
    :param runs: list of function parameters, one item per call
    :param func: the target function to be parallelized
    :param use_threads: if True uses threads, otherwise processes
    :param max_workers: override pool size (default: module-level MAX_TH_DEG or MAX_PR_DEG)
    :param max_jobs: override max queued jobs (default: module-level MAX_JOB_NUMBER)
    :return: list of results in the same order as ``runs``
    :raises MalformedArgListError: when runs is malformed or incompatible with func
    """
    _check_argument_list(runs, func)
    return _do_parallel(runs, func, use_threads, max_workers=max_workers, max_jobs=max_jobs)
