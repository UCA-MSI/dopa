## DOPA: parallelize jobs easily

This is as simple as it sounds.

  * Decide if you need Threads or Processes
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
