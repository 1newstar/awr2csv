#!/usr/bin/env python

import os
import sqlite3
import unittest
import awrhtml2csv
import awrtext2csv
import sqlcsv

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

    def test_sqlcsv(self):
        file = os.path.join('sample', 'load_profile.csv')

        ##### check if the script actually "runs"
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        sqlcsv.parse(conn, cursor, [file])

if __name__ == '__main__':
    unittest.main()
