import unittest
import requests

class TestpyGHRCCatalogueModules(unittest.TestCase):
    testESIndex="test_index"
    testEStype="test_type"
    testDatasetId="dummyEntry"


    def test_ghrc_configFile(self):
        
        self.assertTrue(1==2)
    def test_number_2(self):
        A=1
        B=3
        self.assertEqual(A+B,5)
