import unittest
import dopa  


def func1(x):
    return x + 1

def func2(x,y,z):
    return x + y + z


class TestDopa(unittest.TestCase):
    
    def setUp(self):
        self.singlearg = [1,2,3]
        self.multi = [(1,2,3), (4,5,6)]
        self.d = dict.fromkeys(self.singlearg, 2)

    def test_single_arg_thread(self):
        res = dopa.do_parallel(self.singlearg, func1)
        self.assertEqual(set(res), {2,3,4})

    def test_single_arg_process(self):
        res = dopa.do_parallel(self.singlearg, func1, threaded=False)
        self.assertEqual(set(res), {2,3,4})
    
    def test_multiple_arg_thread(self):
        res = dopa.do_parallel(self.multi, func2)
        self.assertEqual(set(res), {6, 15})

    def test_multiple_arg_process(self):
        res = dopa.do_parallel(self.multi, func2, threaded=False)
        self.assertEqual(set(res), {6, 15})

        
