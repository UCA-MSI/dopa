import concurrent.futures
from configparser import ConfigParser
from inspect import signature, Parameter, getmro

try:
    config = ConfigParser()
    config.read('config.cfg')
    MAX_TH_DEG = int(config['DEFAULT']['MAX_TH_DEG'])
    MAX_PR_DEG = int(config['DEFAULT']['MAX_PR_DEG'])
    MAX_JOB_NUMBER = int(config['DEFAULT']['MAX_JOB_NUMBER'])
except FileNotFoundError:
    MAX_TH_DEG = 4
    MAX_PR_DEG = 2
    MAX_JOB_NUMBER = max(MAX_TH_DEG, MAX_PR_DEG) * 2


class MalformedArgListError(TypeError):
    pass


def _check_argument_default_value(param):
    return param.default is not param.empty


def _check_all_same_type(sequence):
    iter_seq = iter(sequence)
    first_type = type(next(iter_seq))
    return first_type if all((type(x) is first_type) for x in iter_seq) else False


def _check_argument_list(runs, func):
    try:
        length = max([len(x) for x in runs])
        is_consistent = all([len(x) == length for x in runs])
    except TypeError:
        length = 1
        is_consistent = _check_all_same_type(runs)
    sig = signature(func)
    if not is_consistent:
        if all([isinstance(param.default, Parameter.empty) for param in sig.parameters.values()]):
            raise MalformedArgListError(f"Inconsistent number of arguments passed to function {func.__name__}")
        else:
            # print([param for param in sig.parameters.values()])
            missing = [_check_argument_default_value(param) for param in sig.parameters.values()].count(True)
            is_loosely_consistent = all([((len(x) == length) or (len(x) + missing == length)) for x in runs])
            if not is_loosely_consistent:
                raise MalformedArgListError(f"Inconsistent number of arguments passed to function {func.__name__}")
    return True


def do_parallel(runs, func, use_threads):
    if use_threads:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_TH_DEG)
    else:
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PR_DEG)

    done = []

    with executor:
        jobs = {}
        runs_left = len(runs)
        runs_iter = iter(runs)

        while runs_left:
            for run in runs_iter:
                try:
                    future = executor.submit(func, *run)
                except TypeError:
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
    try:
        if _check_argument_list(runs, func):
            return do_parallel(runs, func, use_threads)
    except MalformedArgListError:
        raise RuntimeError('Something bad happened')

