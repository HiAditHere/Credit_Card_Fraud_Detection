import sys
import os
import unittest

# Get the path to the project's root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

class Temp(unittest.TestCase):

    def sample(self):
        self.assertEqual(1+1,2)

if __name__ == 'main':
    unittest.main()