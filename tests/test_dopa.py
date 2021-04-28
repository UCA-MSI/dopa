import unittest
from dopa import dopa  
import numpy as np
import os
import pandas as pd

def func1(x):
    return x + 1

def func2(x, y, z):
    return x + y + z

def func3(x, y, z=0):
    return func2(x, y, z)

def func4(A,B):
    return A+B

def func5(fname):
    return os.stat(fname).st_mode

def func6(pd_row):
    return pd_row['a'] + pd_row['b']

class TestDopa(unittest.TestCase):
    
    def setUp(self):
        self.singlearg = [1, 2, 3]
        self.multi = [(1, 2, 3), (4, 5, 6)]
        self.kw = [(1,2), (4,5,6)]
        self.bad = [(1,2,3), 4]
        self.d = dict.fromkeys(self.singlearg, 2)
        self.arraylist_single = [(np.array([[1,2,3],[4,5,6]]), np.ones(shape = (2,3)))]
        self.df = pd.DataFrame({'a': [1,2,3], 'b':[4,5,6]})

    def test_single_arg_thread(self):
        res = dopa.parallelize(self.singlearg, func1)
        self.assertEqual(set(res), {2, 3, 4})

    def test_single_arg_process(self):
        res = dopa.parallelize(self.singlearg, func1, use_threads=False)
        self.assertEqual(set(res), {2, 3, 4})
    
    def test_multiple_arg_thread(self):
        res = dopa.parallelize(self.multi, func2)
        self.assertEqual(set(res), {6, 15})

    def test_multiple_arg_process(self):
        res = dopa.parallelize(self.multi, func2, use_threads=False)
        self.assertEqual(set(res), {6, 15})

    def test_multiple_arg_kwarg(self):
        res = dopa.parallelize(self.kw, func3)
        self.assertEqual(set(res), {3, 15})

    def test_check_all_same_type(self):
        self.assertFalse(dopa._check_all_same_type(self.bad))
        self.assertTrue(dopa._check_all_same_type(self.kw))
    
    def test_array_arg_thread(self):
        res = dopa.parallelize(self.arraylist_single, func4)
        res = np.array(res)[0]  # dopa.parallelize return a list 
        self.assertIsNone(np.testing.assert_almost_equal(res, np.array([[2,3,4],[5,6,7]])))
    
    def test_path_fnames(self):
        tmpdir = './'
        content = [os.path.join(tmpdir, f) for f in os.listdir(tmpdir)]
        fnames = [f for f in content if os.path.isfile(f)]
        res = dopa.parallelize(fnames, func5)
        self.assertEqual(len(res), len(fnames))

    def test_pandas_rows(self):
        all_rows = [row for idx, row in self.df.iterrows()]
        res = dopa.parallelize(all_rows, func6)
        self.assertEqual(len(res), len(self.df))
        self.assertListEqual(sorted(res), [5,7,9])

