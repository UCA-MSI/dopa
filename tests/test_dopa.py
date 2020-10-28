import unittest
from dopa import dopa  


def func1(x):
    return x + 1


def func2(x, y, z):
    return x + y + z


def func3(x, y, z=0):
    return func2(x, y, z)


class TestDopa(unittest.TestCase):
    
    def setUp(self):
        self.singlearg = [1, 2, 3]
        self.multi = [(1, 2, 3), (4, 5, 6)]
        self.kw = [(1,2), (4,5,6)]
        self.bad = [(1,2,3), 4]
        self.d = dict.fromkeys(self.singlearg, 2)

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

    def test_check_argument_list(self):
        with self.assertRaises(dopa.MalformedArgListError):
            dopa._check_argument_list(self.kw, func2)
        self.assertTrue(dopa._check_argument_list(self.kw, func3))
