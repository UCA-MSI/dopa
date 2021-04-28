import concurrent.futures
from configparser import ConfigParser
from inspect import signature, Parameter, getmro
import os
import numpy as np

__all__ = ['MalformedArgListError', 'parallelize']

try:
    config = ConfigParser()
    config.read('dopa/config.cfg')
    MAX_TH_DEG = int(config['DEFAULT']['MAX_TH_DEG'])
    MAX_PR_DEG = int(config['DEFAULT']['MAX_PR_DEG'])
    MAX_JOB_NUMBER = int(config['DEFAULT']['MAX_JOB_NUMBER'])
except (FileNotFoundError, KeyError) as e:
    MAX_TH_DEG = os.cpu_count() * 2
    MAX_PR_DEG = os.cpu_count() - 1
    MAX_JOB_NUMBER = max(MAX_TH_DEG, MAX_PR_DEG) * 2


class MalformedArgListError(TypeError):
    pass


def _check_argument_default_value(param):
    return param.default is not param.empty


def _check_all_same_type(sequence):
    """
    Check consistency of types across a sequence
    :param sequence: the iterable
    :return: bool
    """
    iter_seq = iter(sequence)
    first_type = type(next(iter_seq))
    return first_type if all((type(x) is first_type) for x in iter_seq) else False


def _check_argument_list(runs, func):
    """
    Check if all runs items are compatible with the func parameters.
    :param runs: the list of function parameters
    :param func: the target function
    :return: bool
    :raise MalformedArgListError(TypeError)
    """
    first = runs[0]
    is_consistent = False
    is_loosely_consistent = False

    sig = signature(func).parameters.values()
    
    if isinstance(first, np.ndarray):
        is_consistent = all([x.shape == first.shape for x in runs])
        if not is_consistent:
            raise MalformedArgListError('Inconsistent shapes of ndarrays')
    else:
        is_consistent = all([isinstance(x, type(first)) for x in runs])
        if is_consistent:
            if isinstance(first, list) or isinstance(first, tuple) or isinstance(first, dict):
                try: 
                    assert all([len(x) == len(first) for x in runs])
                except AssertionError:
                    is_consistent = False
                    is_loosely_consistent = True
                    
    # TODO: check is_loosely_consistent. Maybe we can leave this to the function.
    
    return is_consistent or is_loosely_consistent


def do_parallel(runs, func, use_threads):
    """
    Execute in parallel the list of jobs, submitting each item in the `runs` list to the target function `func`.
    If `use_threads` is set to True, it uses threads. (False -> processes)
    :param runs: list of function parameters for the target function. One item for each function call
    :param func: the target function to be parallelized
    :param use_threads: bool
    :return: bool
    """
    if use_threads:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_TH_DEG)
    else:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PR_DEG)

    done = []

    first = runs[0]
    is_iterable = isinstance(first, list) or isinstance(first, tuple)

    with executor:
        jobs = {}
        runs_left = len(runs)
        runs_iter = iter(runs)

        while runs_left:
            for run in runs_iter:
                if is_iterable:
                    future = executor.submit(func, *run)
                else:
                    future = executor.submit(func, run)

                jobs[future] = run
                if len(jobs) > MAX_JOB_NUMBER:
                    break

            for future in concurrent.futures.as_completed(jobs):
                runs_left -= 1
                result = future.result()
                run = jobs[future]
                del jobs[future]
                done.append(result)
                break

    return done


def parallelize(runs, func, use_threads=True):
    """
    Calls the `do_parallel` function after inspecting the consistency of the `runs` list.
    :param runs: list of function parameters for the target function. One item for each function call
    :param func: the target function to be parallelized
    :param use_threads: bool
    :return: bool
    """
    try:
        if _check_argument_list(runs, func):
            return do_parallel(runs, func, use_threads)
    except MalformedArgListError:
        raise RuntimeError('Something bad happened')


    