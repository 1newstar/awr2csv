#!/usr/bin/env python

import os
import unittest
import awrhtml2csv
import awrtext2csv

class AWRTest(unittest.TestCase):
    def test_html(self):
        filelist = [os.path.join('sample', x) for x in os.listdir('sample') if x.endswith('.html')]
        
        ##### check if the script actually "runs"
        output = awrhtml2csv.parse(filelist)
        
        ##### check if csv actually contains something
        for csvname in output:
            self.assertGreater(len(output[csvname]), 0)

    def test_text(self):
        filelist = [os.path.join('sample', x) for x in os.listdir('sample') if x.endswith('.txt')]
        
        ##### check if the script actually "runs"
        output = awrtext2csv.parse(filelist)
        
        ##### check if csv actually contains something
        for csvname in output:
            self.assertGreater(len(output[csvname]), 0)

if __name__ == '__main__':
    unittest.main()