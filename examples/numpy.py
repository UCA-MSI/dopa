import numpy as np
import dopa


# create 10 matrices (3,2)
runs = [np.random.rand(3,2) for _ in range(10)]

def func(mat):
    return mat.T


results = dopa.parallelize(runs, func)
print( all([x.shape == (2,3) for x in results]))


