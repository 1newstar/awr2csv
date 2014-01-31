#!/usr/bin/env python
#
# Create CSV from AWR html reports
#
# Supported Versions
#   11.2.0.3 RAC
#   11.2.0.4 RAC
#
# Note
#   searches for <table summary=XXXX> and converts <th><td> into csv.
#   only the first 50 chars are used as a key.
#   if the table's <th> is not appropriate, you can override as shown below.
#
######################################################################################################
#  01234567890123456789012345678901234567890123456789 => first 50 chars only
t = {}
t['This table displays load profile']                   = ('load_profile.csv'     , 'Name,Per Second,Per Transaction,Per Exec,Per Call')
t['Top 5 Timed Foreground Events']                      = ('events_topn.csv'      , '')  # -11.2.0.3
t['This table displays top 10 wait events by total wa'] = ('events_topn.csv'      , '')  # 11.2.0.4 only
t['This table displays Foreground Wait Events and the'] = ('events_foreground.csv', '')
t['This table displays background wait events statist'] = ('events_background.csv', '')
t['This table displays different time model statistic'] = ('time_model.csv'       , '')
t['This table displays top SQL by elapsed time']        = ('sql_elapsed.csv'      , '')
t['This table displays top SQL by CPU time']            = ('sql_cpu.csv'          , '')
t['This table displays top SQL by user I/O time']       = ('sql_user_io.csv'      , '')
t['This table displays top SQL by buffer gets']         = ('sql_gets.csv'         , '')
t['This table displays top SQL by physical reads']      = ('sql_reads.csv'        , '')
t['This table displays top SQL by unoptimized read re'] = ('sql_unoptimized.csv'  , '')
t['This table displays top SQL by number of execution'] = ('sql_executions.csv'   , '')
t['This table displays top SQL by number of parse cal'] = ('sql_parses.csv'       , '')
t['This table displays top SQL by version counts']      = ('sql_version.csv'      , '')
t['This table displays top SQL by cluster wait time']   = ('sql_cluster.csv'      , '')
t['This table displays Key Instance activity statisti'] = ('key_stats.csv'        , '')  # 11.2.0.4 only
t['This table displays PGA aggregate target statistic'] = ('pga_aggr_stats.csv'   , 'B or E,PGA Aggr Target(M),Auto PGA Target(M),PGA Mem Alloc(M),W/A PGA Used(M),%PGA W/A Mem,%Auto W/A Mem,%Man W/A Mem,Global Mem Bound(K)')
t['This table displays top segments by logical reads.'] = ('seg_logical.csv'      , '')
t['This table displays top segments by physical reads'] = ('seg_phys_reads.csv'   , '')
t['This table displays top segments by unoptimized re'] = ('seg_unoptimized.csv'  , '')
t['This table displays top segments by physical write'] = ('seg_phys_writes.csv'  , '')
t['This table displays top segments by CR blocks rece'] = ('seg_cr_rcvd.csv'      , '')
t['This table displays top segments by current blocks'] = ('seg_current_rcvd.csv' , '')
t['This table displays information about global cache'] = ('load_profile_g.csv'   , 'Name,Per Second,Per Transaction')
t['This table displays global cache efficiency percen'] = ('efficiency_g.csv'     , 'Name,Value')
#####################################################################################################

import glob
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

##### extract text from <th> or <td> tag
def tfix(elem):
    # use the tag's own text, if it exists
    text = elem.text or ''
    # if <a> exists as a child, use its text instead (ie SQLID)
    for a in elem.iter('a'):
        text = a.text
    # format
    text = text.strip()
    text = re.sub(r'\s+', ' ', text)
    for c in ['&#160;',',',':']:
        text = text.replace(c, '')
    return text

##### sys.argv[] filelist does not work in Windows, use glob
filelist = sys.argv[1:]
if filelist[0].find('*') >= 0:
    filelist = glob.glob(filelist[0])

##### iterate over files
f = {}  # file
for filename in filelist:
    print('Processing {0}...'.format(filename))
    tree = ET.parse(filename)
    root = tree.getroot()
    b_header = False  # begin header
    l_base = []       # report-specific info (list)
    d_base = ''       # report-specific info (string)

    ##### iterate over <table> tag
    for table in root.iter('table'):
        ##### extract <table summary=XXXXXX>
        section = ''
        if 'summary' in table.attrib:
            section = table.attrib['summary'][:50]
        ##### extract DBName, etc from the 2nd row, columns1-4
        if section == 'This table displays database instance information':
            l_base = [tfix(x) for x in table.iter('td')][:4]
        ##### extract begin/end snap time from 2nd-3rd row, column3
        elif section == 'This table displays snapshot information':
            for tr in list(table.iter('tr'))[1:3]:
                snap = list(tr.iter('td'))[2]
                st = datetime.strptime(snap.text, '%d-%b-%y %H:%M:%S')
                l_base.extend(str(x) for x in (st.year, st.month, st.day, st.hour, st.minute, st.second))
            d_base = ','.join(l_base) + ','
        ##### for other sections, convert <th><td> structure into a CSV
        elif section in t:
            (csvname, header) = t[section]
            ##### create CSV on a first access
            if section not in f:
                f[section] = open(csvname, 'wb')
                b_header = True
                print('  Created: ' + csvname)
            ##### iterate over <tr> tag
            for tr in table.iter('tr'):
                ##### override header if specified by a grand table, otherwise use <th>
                if b_header:
                    h_base = 'DB_NAME,DB_ID,INSTANCE_NAME,INST_NUM,B_Y,B_MO,B_D,B_H,B_MI,B_S,E_Y,E_MO,E_D,E_H,E_MI,E_S,'
                    h_data = header or ','.join(tfix(x) for x in tr.iter('th'))
                    f[section].write((h_base + h_data + '\n').encode('UTF-8'))
                    b_header = False
                ##### extract <td> data
                l_td = [tfix(x) for x in tr.iter('td')]
                if len(l_td) > 0:
                    d_data = ','.join(l_td)
                    f[section].write((d_base + d_data + '\n').encode('UTF-8'))
