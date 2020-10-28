## DOPA: parallelize jobs easily

`DO PArallel` stuff. This is as simple as it sounds.

  * Decide if you need Threads or Processes. 
       * Rule of Thumb: 
            * IO-bound -> threads; 
            * CPU-bound -> processes
  * [*OPTIONAL*] read maximum numbers of threads/processes and jobs on a `config.cfg` file.
  * Prepare a `list` of arguments to be feed to your function `func`
  * Call it with  `dopa.parallelize(arglist, func)`

The module will unpack `arglist` into its components and execute in parallel
`[func(arg) for arg in arglist]`.

The module checks consistency through the list of function parameters, by 
  * inspecting the signature of the target function
  * checking the consistency (`same type` equivalence) of all the passed arguments

Returns a list of results.

#### To run the tests:

```
    python3 -m unittest tests/test_dopa.py
```


### Installation
```
git clone https://github.com/UCA-MSI/dopa.git
cd dopa
python3 setup.py install [--user]
```

or

```
pip install git+https://github.com/UCA-MSI/dopa.git
```

### Basic usage:
```
import dopa
def f(x):
    return x + 1

runs = [1,2,3]

print(dopa.parallelize(runs, f))

# output: [4,3,2]
```
