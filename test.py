#!/usr/bin/python

import unittest

class TestScore(unittest.TestCase):
    def test_null(self):
        self.assertEqual('woof', 'woof')

unittest.main()
