
#http://docs.python-guide.org/en/latest/writing/structure/
from context import stompworker

import stompworker.stomplistener as slistener
headers={'a':'b'}
slistener.HeaderStruct(headers)

import unittest

def fun(x):
    return x + 1

class MyTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(3), 4)

