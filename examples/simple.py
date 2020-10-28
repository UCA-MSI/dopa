import dopa
def f(x):
    return x + 1

runs = [1,2,3]
print(dopa.parallelize(runs, f))

