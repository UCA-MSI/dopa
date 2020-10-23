## DOPA: parallelize jobs easily

This is as simple as it sounds.

  * Decide if you need Threads or Processes
  * Prepare a `list` of arguments to be feed to your function `func`
  * Submit as  `dopa.do_parallel(arglist, func)`

The module will unpack `arglist` into its components and execute in parallel
`[func(arg) for arg in arglist]`.

Returns a list of results.

 
