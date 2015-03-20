#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Create CSV from AWR text reports
#
# Tested Versions
#   10.2.0.1 RAC
#   10.2.0.4 RAC
#
# Note
#   extracts values specified with "===" sequence.
#
######################################################################################################
t = {}
t['Events_TopN']     = ('events_topn.csv', 'Event,Waits,Time(s),Avg Wait(ms),% Total Call Time,Wait Class', '============================== ============ =========== ====== ====== ==========')
t['Load Profile']    = ('load_profile.csv', 'Name,Per Second,Per Transaction', '===========================        ===============       ===============')
t['Inst_Stats']      = ('inst_stats.csv', 'Statistic,Total,per Second,per Trans', '================================ ================== ============== =============')
t['PGA_Aggr']        = ('pga_aggr_stats.csv', 'B or E,PGA Aggr Target(M),Auto PGA Target(M),PGA Mem Alloc(M),W/A PGA Used(M),%PGA W/A Mem,%Auto W/A Mem,%Man W/A Mem,Global Mem Bound(K)', '= ========== ========== ========== ========== ====== ====== ====== ==========')
t['GlobalLP']        = ('load_profile_g.csv', 'Name,Per Second,Per Transaction', '==============================        ===============       ===============')
t['GlobalEP']        = ('efficiency_g.csv', 'Name,Value', '==============================  =======')
t['SQL_Elapsed']     = ('sql_elapsed.csv', 'Elapsed Time (s),CPU Time (s),Executions,Elap per Exec (s),%Total,SQL Id,SQL Module', '========== ========== ============ ========== ======= =============')
t['SQL_CPU']         = ('sql_cpu.csv', 'CPU Time (s),Elapsed Time (s),Executions,CPU per Exec (s),%Total,SQL Id,SQL Module', '========== ========== ============ =========== ======= =============')
t['SQL_Gets']        = ('sql_gets.csv', 'Buffer Gets,Executions,Gets per Exec,%Total,CPU Time (s),Elapsed Time (s),SQL Id,SQL Module', '============== ============ ============ ====== ======== ========= =============')
t['SQL_Reads']       = ('sql_reads.csv', 'Physical Reads,Executions,Reads per Exec,%Total,CPU Time (s),Elapsed Time (s),SQL Id,SQL Module', '============== =========== ============= ====== ======== ========= =============')
t['SQL_Cluster']       = ('sql_cluster.csv', 'Cluster Wait Time (s),CWT % of Elapsed Time,Elapsed Time (s),CPU Time (s),Executions,SQL Id,SQL Module', '============ =========== =========== =========== ============== =============')

#####################################################################################################

import codecs
import glob
import os
import re
import sys
from datetime import datetime

##### extract
##### =====       ====== ======
##### aaaaa   123   12.3  4,567  -> ['aaaaa', '12.3', '4567']
def line2list(line, mask):
    ret = []
    re_eq = re.compile(r'=+')
    for x in re_eq.finditer(mask):
        (b, e) = x.span()
        text = line[b:e].strip().replace(',', '')
        text = re.sub(r'\s+', ' ', text)
        ret.append(text)
    return ret

##### parse files
def parse(filelist):
    ##### common header
    h_base = 'DB_NAME,DB_ID,INSTANCE_NAME,INST_NUM,B_Y,B_MO,B_D,B_H,B_MI,B_S,E_Y,E_MO,E_D,E_H,E_MI,E_S,'

    ##### DB name, Snaptime, SQL Module extract helper
    m_dbname = '============ =========== ============ ======== ==========='
    m_snaptm = '                      ==================='
    m_module = '        ========================================================================'

    ##### output
    output = {}
    for section in t:
        (csvname, header, mask) = t[section]
        output[csvname] = [h_base + header]

    ##### iterate over files
    for filename in filelist:
        print('Processing {0}...'.format(filename))
        db_ver = ''    # DB Versoin
        section = ''   # section Name
        l_base = []    # report-specific info (list)
        d_base = ''    # report-specific info (string)
        b_data = False # begin data
        l_data = []    # section-specific data (list)

        ##### iterate over lines
        for line in open(filename, 'r'):
            if section in t:
                (csvname, header, mask) = t[section]

            ##### DB Name
            # ============ =========== ============ ======== ===========
            # DB Name         DB Id    Instance     Inst Num Release     RAC Host
            # ------------ ----------- ------------ -------- ----------- --- ------------
            # DB0           9901230123 DB01                1 10.2.0.1.0  YES host1
            #
            if line.startswith('DB Name'):
                section = 'DB Name'
            elif section == 'DB Name':
                if not line.startswith('---'):
                    l_line = line2list(line, m_dbname)
                    l_base = l_line[:4]
                    db_ver = l_line[4]
                    print('  DB Version: ' + db_ver)
                    section = ''
            
            ##### Snap Time
            #                       ===================
            #               Snap Id      Snap Time      Sessions Curs/Sess
            #             --------- ------------------- -------- ---------
            # Begin Snap:      3726 16-2月 -13 05:00:50      640        .1
            #   End Snap:      3727 16-2月 -13 06:00:16      672        .2
            #    Elapsed:               59.43 (mins)
            #    DB Time:               25.21 (mins)
            #
            elif line.startswith('Begin Snap:') or line.startswith('  End Snap:'):
                dt = datetime.strptime(line2list(line, m_snaptm)[0], '%d-%b-%y %H:%M:%S')
                l_base.extend(str(x) for x in (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second))
                d_base = ','.join(l_base) + ','
            
            ##### Load Profile
            # 
            # ===========================        ===============       ===============
            # Load Profile
            # ~~~~~~~~~~~~                            Per Second       Per Transaction
            #                                    ---------------       ---------------
            #                   Redo size:             68,225.00             12,794.53
            #               Logical reads:             19,994.77              3,749.71
            #               Block changes:                222.80                 41.78
            #              Physical reads:                 11.35                  2.13
            #
            # <EOS>
            #
            #
            elif line.startswith('Load Profile'):
                section = 'Load Profile'
            elif section == 'Load Profile':
                ##### blank line => section end
                if len(line.strip()) == 0:
                    section = ''
                    b_data = False
                    l_data = []
                ##### begin data
                elif line.startswith('                                   ---------------'):
                    b_data = True
                ##### extract data
                elif b_data:
                    l_data = line2list(line, mask)
                    output[csvname].append(d_base + ','.join(l_data))
            
            ##### Top N Events
            # 
            # ============================== ============ =========== ====== ====== ==========
            # Top 5 Timed Events                                         Avg %Total
            # ~~~~~~~~~~~~~~~~~~                                        wait   Call
            # Event                                 Waits    Time (s)   (ms)   Time Wait Class
            # ------------------------------ ------------ ----------- ------ ------ ----------
            # CPU time                                          1,405          92.9
            # db file sequential read              40,533         181      4   12.0   User I/O
            # log file sync                        36,722          56      2    3.7     Commit
            # log file parallel write              36,838          50      1    3.3 System I/O
            # SQL*Net more data from client       294,888          21      0    1.4    Network
            #           -------------------------------------------------------------
            # <^L>
            # <EOS>
            # 
            elif line.startswith('Top 5 Timed Events'):
                section = 'Events_TopN'
            elif section == 'Events_TopN':
                ##### "     " => section end
                if line.startswith('   '):
                    section = ''
                    b_data = False
                    l_data = []
                ##### begin data
                elif line.startswith('---'):
                    b_data = True
                ##### extract data
                elif b_data:
                    l_data = line2list(line, mask)
                    output[csvname].append(d_base + ','.join(l_data))
            
            ##### SQL ordered by x
            # ========== ========== ============ ========== ======= =============
            #         ========================================================================
            # <^L>SQL ordered by Elapsed Time         DB/Inst: DB/db1  Snaps: 914-938
            # -> Resources reported for PL/SQL code includes the resources used by all SQL
            #    statements called by the code.
            # -> :
            #
            #   Elapsed      CPU                  Elap per  % Total
            #   Time (s)   Time (s)  Executions   Exec (s)  DB Time    SQL Id
            # ---------- ---------- ------------ ---------- ------- -------------
            #      1,001        989        9,238        0.1    66.2 0gu4sdfsdfsz4
            # Module: xxxxxxx
            #  SQL...
            #
            #           -------------------------------------------------------------
            # <EOS>
            #
            # ========== ========== ============ =========== ======= =============
            # <^L>SQL ordered by CPU Time             DB/Inst: DB/db1  Snaps: 914-938
            #  :
            #     CPU      Elapsed                  CPU per  % Total
            #   Time (s)   Time (s)  Executions     Exec (s) DB Time    SQL Id
            # ---------- ---------- ------------ ----------- ------- -------------
            #        989      1,001        9,238        0.11    66.2 0gu4sdfsdfsz4
            #           -------------------------------------------------------------
            # <EOS>
            #
            # ============== ============ ============ ====== ======== ========= =============
            # <^L>SQL ordered by Gets                 DB/Inst: DB/db1  Snaps: 936-937
            #  :
            #                                 Gets              CPU     Elapsed
            #   Buffer Gets   Executions    per Exec   %Total Time (s)  Time (s)    SQL Id
            # -------------- ------------ ------------ ------ -------- --------- -------------
            #     64,090,911        9,238      6,937.7   89.9   989.48   1000.92 0gu4sdfsdfsz4
            #           -------------------------------------------------------------
            # <EOS>
            #
            # ============== =========== ============= ====== ======== ========= =============
            # <^L>SQL ordered by Reads                DB/Inst: DB/db1  Snaps: 914-938
            #  :
            #                                Reads              CPU     Elapsed
            # Physical Reads  Executions    per Exec   %Total Time (s)  Time (s)    SQL Id
            # -------------- ----------- ------------- ------ -------- --------- -------------
            #          9,501      18,521           0.5   23.5    27.68     51.66 0gu4sdfsdfsz4
            #           -------------------------------------------------------------
            # <EOS>
            #
            # ============ =========== =========== =========== ============== =============
            # <^L>SQL ordered by Cluster Wait Time      DB/Inst: DB1/db1  Snaps: 726-727
            #
            #       Cluster   CWT % of     Elapsed         CPU
            # Wait Time (s) Elapsd Tim     Time(s)     Time(s)     Executions    SQL Id
            # ------------- ---------- ----------- ----------- -------------- -------------
            #           -------------------------------------------------------------
            # <EOS>
            #
            elif line.startswith(chr(12) + 'SQL ordered by Elapsed Time'):
                section = 'SQL_Elapsed'
            elif line.startswith(chr(12) + 'SQL ordered by CPU Time'):
                section = 'SQL_CPU'
            elif line.startswith(chr(12) + 'SQL ordered by Gets'):
                section = 'SQL_Gets'
            elif line.startswith(chr(12) + 'SQL ordered by Reads'):
                section = 'SQL_Reads'
            elif line.startswith(chr(12) + 'SQL ordered by Cluster Wait Time'):
                section = 'SQL_Cluster'
            elif section.startswith('SQL_'):
                ##### something like "    -----" => section end
                if re.match(r'^  +\-+', line):
                    ##### if missing a "Module:", extract now
                    if len(l_data) > 0:
                        output[csvname].append(d_base + ','.join(l_data + ['']))
                    section = ''
                    l_data = []
                    b_data = False
                ##### "Module:" => SQL end
                elif line.startswith('Module:'):
                    l_data.append(line2list(line, m_module)[0])
                    output[csvname].append(d_base + ','.join(l_data))
                ##### 2 numbers with spaces in between => data
                elif re.match(r'^ +[\d\.\,]+ +[\d\.\,]+', line):
                    ##### if missing a "Module:", extract now
                    if len(l_data) > 0:
                        output[csvname].append(d_base + ','.join(l_data + ['']))
                    ##### extract
                    l_data = line2list(line, mask)
            
            ##### Others (no random section headers within data)
            # ================================ ================== ============== =============
            # <^LInstance Activity Stats                   DB/Inst: DB0/DB01  Snaps: 3726-3727
            # 
            # Statistic                                     Total     per Second     per Trans
            # -------------------------------- ------------------ -------------- -------------
            # CPU used by this session                    130,788           36.7           6.9
            # CPU used when call started                  128,989           36.2           6.8
            # CR blocks created                             8,951            2.5           0.5
            # Cached Commit SCN referenced                 18,654            5.2           1.0
            # Commit SCN cached                                 2            0.0           0.0   :
            #           -------------------------------------------------------------
            # <EOS>
            #
            # = ========== ========== ========== ========== ====== ====== ====== ==========
            # PGA Aggr Target Stats                DB/Inst: DB/db1  Snaps: 936-937
            # -> B: Begin Snap   E: End Snap (rows dentified with B or E contain data
            #    which is absolute i.e. not diffed over the interval)
            # -> :
            #
            #                                                 %PGA  %Auto   %Man
            #     PGA Aggr   Auto PGA   PGA Mem    W/A PGA     W/A    W/A    W/A Global Mem
            #    Target(M)  Target(M)  Alloc(M)    Used(M)     Mem    Mem    Mem   Bound(K)
            # - ---------- ---------- ---------- ---------- ------ ------ ------ ----------
            # B      4,096      3,536      360.8        0.0     .0     .0     .0    419,430
            # E      4,096      3,530      358.4        0.0     .0     .0     .0    419,430
            #           --------------------------------------------------------------
            # <EOS>
            #
            elif line.startswith(chr(12) + 'Instance Activity Stats     '):
                section = 'Inst_Stats'
                b_data = False
                l_data = []
            elif line.startswith('PGA Aggr Target Stats'):
                section = 'PGA_Aggr'
            elif section in ['Inst_Stats', 'PGA_Aggr']:
                ##### something like "    -----" => section end
                if re.match(r'^  +\-+', line):
                    section = ''
                    b_data = False
                    l_data = []
                ##### begin data
                elif line.startswith('---') or line.startswith('- -'):
                    b_data = True
                ##### extract data
                elif b_data:
                    l_data = line2list(line, mask)
                    output[csvname].append(d_base + ','.join(l_data))
            
            ##### Global Cache
            # ==============================        ===============       ===============
            # Global Cache Load Profile
            # ~~~~~~~~~~~~~~~~~~~~~~~~~                  Per Second       Per Transaction
            #                                       ---------------       ---------------
            #   Global Cache blocks received:                  0.34                  0.06
            #     Global Cache blocks served:                  2.37                  0.44
            #      GCS/GES messages received:                 14.01                  2.63
            #          GCS/GES messages sent:                 20.06                  3.76
            #             DBWR Fusion writes:                  0.06                  0.01
            #  Estd Interconnect traffic (KB)                 28.34
            #
            # <EOS>
            #
            # ==============================  =======
            # Global Cache Efficiency Percentages (Target local+remote 100%)
            # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            # Buffer access -  local cache %:   99.94
            # Buffer access - remote cache %:    0.00
            # Buffer access -         disk %:    0.06
            #
            # <EOS>
            #
            elif line.startswith('Global Cache Load Profile'):
                section = 'GlobalLP'
            elif line.startswith('Global Cache Efficiency Percentages'):
                section = 'GlobalEP'
            elif section in ['GlobalLP', 'GlobalEP']:
                ##### blank line => section end
                if len(line.strip()) == 0:
                    section = ''
                    b_data = False
                    l_data = []
                ##### begin data
                elif line.strip()[:3] in ['~~~', '---']:
                    b_data = True
                ##### extract data
                elif b_data:
                    l_data = line2list(line, mask)
                    output[csvname].append(d_base + ','.join(l_data))
    
    ##### return output
    return output

if __name__ == '__main__':
    ##### sys.argv[] filelist does not work in Windows, use glob
    filelist = sys.argv[1:]
    if filelist[0].find('*') >= 0:
        filelist = glob.glob(filelist[0])
    
    ##### parse & write to files
    output = parse(filelist)
    for csvname in output:
        print('  Created: ' + csvname)
        f = codecs.open(csvname, 'w', encoding='utf-8')
        for line in output[csvname]:
            try:
                f.write(line + '\n')
            except UnicodeDecodeError, e:
                print("Skipped:" + line)
        f.close()
