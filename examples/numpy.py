import numpy as np
import dopa


# create 10 matrices (3,2)
runs = [np.random.rand(3,2) for _ in range(10)]

def func(mat):
    return mat.T

def two_arg_func(mat1, mat2):
    return np.dot(mat1, mat2)


results = dopa.parallelize(runs, func)
print( all([x.shape == (2,3) for x in results]))


tup_runs = [(np.random.rand(3,2), np.random.rand(2,3) for _ in range(10)]
results = dopa.parallelize(tup_runs, two_arg_func)
print(results[0])

