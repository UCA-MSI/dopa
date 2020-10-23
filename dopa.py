import concurrent.futures
from configparser import ConfigParser


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

def do_parallel(runs, func, threaded=True):
    if threaded:
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


