#!/usr/bin/env python
#
# Create CSV from AWR text reports
#
# Supported Versions
#   11.2.0.1 - 11.2.0.4
#
# Note
#   extracts values specified with "===" sequence.
#
######################################################################################################
t = {}
t['Load Profile_04'] = ('load_profile.csv', 'Name,Per Second,Per Transaction', '=======================    ===============   ===============')
t['Load Profile_03'] = ('load_profile.csv', 'Name,Per Second,Per Transaction', '================     ===============    ===============')
t['Events_TopN_04']  = ('events_topn.csv', 'Event,Waits,Total Wait Time (sec),Wait Avg(ms),% DB time,Wait Class', '============================== ============ ==== ======= ====== ==========')
t['Events_TopN_03']  = ('events_topn.csv', 'Event,Waits,Total Wait Time (sec),Wait Avg(ms),% DB time,Wait Class', '============================== ============ =========== ====== ====== ==========')
t['Events_FG']       = ('events_foreground.csv', 'Event,Waits,%Time -outs,Total Wait Time (s),Avg wait (ms),Waits /txn,% DB time', '========================== ============ ===== ========== ======= ======== ======')
t['Events_BG']       = ('events_background.csv', 'Event,Waits,%Time -outs,Total Wait Time (s),Avg wait (ms),Waits /txn,% bg time', '========================== ============ ===== ========== ======= ======== ======')
t['Time_Model']      = ('time_model.csv', 'Statistic Name,Time (s),% of DB Time', '========================================== ================== ============')
t['SQL_Elapsed']     = ('sql_elapsed.csv', 'Elapsed Time (s),Executions,Elapsed Time per Exec (s),%Total,%CPU,%IO,SQL Id,SQL Module', '================ ============== ============= ====== ====== ====== =============')
t['SQL_CPU']         = ('sql_cpu.csv', 'CPU Time (s),Executions,CPU per Exec (s),%Total,Elapsed Time (s),%CPU,%IO,SQL Id,SQL Module', '========== ============ ========== ====== ========== ====== ====== =============')
t['SQL_UserIO']      = ('sql_user_io.csv', 'User I/O Time (s),Executions,UIO per Exec (s),%Total,Elapsed Time (s),%CPU,%IO,SQL Id,SQL Module', '========== ============ ========== ====== ========== ====== ====== =============')
t['SQL_Gets']        = ('sql_gets.csv', 'Buffer Gets,Executions,Gets per Exec,%Total,Elapsed Time (s),%CPU,%IO,SQL Id,SQL Module', '=========== =========== ============ ====== ========== ===== ===== =============')
t['SQL_Reads']       = ('sql_reads.csv', 'Physical Reads,Executions,Reads per Exec,%Total,Elapsed Time (s),%CPU,%IO,SQL Id,SQL Module', '========== ============ ========== ====== ========== ====== ====== =============')
t['SQL_Unoptim']     = ('sql_unoptimized.csv', 'UnOptimized Read Reqs,Physical Read Reqs,Executions,UnOptimized Reqs per Exec,%Opt,%Total,SQL Id,SQL Module', '=========== =========== ========== ============ ====== ====== =============')
t['SQL_Execs']       = ('sql_executions.csv', 'Executions,Rows Processed,Rows per Exec,Elapsed Time (s),%CPU,%IO,SQL Id,SQL Module', '============ =============== ============== ========== ===== ===== =============')
t['SQL_Parses']      = ('sql_parses.csv', 'Parse Calls,Executions,% Total Parses,SQL Id,SQL Module', '============ ============ ========= =============')
t['SQL_Version']     = ('sql_version.csv', 'Version Count,Executions,SQL Id,SQL Module', '======== ============ =============')
t['SQL_Cluster']     = ('sql_cluster.csv', 'Cluster Wait Time (s),Executions,%Total,Elapsed Time(s),%Clu,%CPU,%IO,SQL Id,SQL Module', '============== ============ ====== ========== ====== ====== ====== =============')
t['Key_Stats']       = ('key_stats.csv', 'Statistic,Total,per Second,per Trans', '================================ ================== ============== =============') # 11.2.0.4 only
t['PGA_Aggr']        = ('pga_aggr_stats.csv', 'B or E,PGA Aggr Target(M),Auto PGA Target(M),PGA Mem Alloc(M),W/A PGA Used(M),%PGA W/A Mem,%Auto W/A Mem,%Man W/A Mem,Global Mem Bound(K)', '= ========== ========== ========== ========== ====== ====== ====== ==========')
t['Seg_Logical']     = ('seg_logical.csv', 'Owner,Tablespace Name,Object Name,Subobject Name,Obj. Type,Logical Reads,%Total', '========== ========== ==================== ========== ===== ============ =======')
t['Seg_Physical']    = ('seg_phys_reads.csv', 'Owner,Tablespace Name,Object Name,Subobject Name,Obj. Type,Physical Reads,%Total', '========== ========== ==================== ========== ===== ============ =======')
t['Seg_UnOptim']     = ('seg_unoptimized.csv', 'Owner,Tablespace Name,Object Name,Subobject Name,Obj. Type,UnOptimized Reads,%Total', '========== ========== ==================== ========== ===== ============ =======')
t['Seg_Writes']      = ('seg_phys_writes.csv', 'Owner,Tablespace Name,Object Name,Subobject Name,Obj. Type,Physical Writes,%Total', '========== ========== ==================== ========== ===== ============ =======')
t['Seg_CR']          = ('seg_cr_rcvd.csv', 'Owner,Tablespace Name,Object Name,Subobject Name,Obj. Type,CR Blocks Received,%Total', '========== ========== ==================== ========== ===== ============ =======')
t['Seg_Cur']         = ('seg_current_rcvd.csv', 'Owner,Tablespace Name,Object Name,Subobject Name,Obj. Type,Current Blocks Received,%Total', '========== ========== ==================== ========== ===== ============ =======')
t['GlobalLP']        = ('load_profile_g.csv', 'Name,Per Second,Per Transaction', '==============================        ===============       ===============')
t['GlobalEP']        = ('efficiency_g.csv', 'Name,Value', '==============================  =======')
#####################################################################################################

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

##### DB name, Snaptime, SQL Module extract helper
m_dbname = '============ =========== ============ ========                 ==========='
m_snaptm = '                      ==================='
m_module = '        ========================================================================'

##### create CSV
f = {}  # file
m = {}  # masking
h_base = 'DB_NAME,DB_ID,INSTANCE_NAME,INST_NUM,B_Y,B_MO,B_D,B_H,B_MI,B_S,E_Y,E_MO,E_D,E_H,E_MI,E_S,'
for section in t.keys():
    (csvname, header, mask) = t[section]
    f[section] = open(csvname, 'wb')
    f[section].write((h_base + header + '\n').encode('UTF-8'))
    m[section] = mask

##### sys.argv[] filelist does not work in Windows, use glob
filelist = sys.argv[1:]
if filelist[0].find('*') >= 0:
    filelist = glob.glob(filelist[0])

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
        #####
        ##### DB Name
        # ============ =========== ============ ========                 ===========
        # DB Name         DB Id    Instance     Inst Num Startup Time    Release     RAC
        # ------------ ----------- ------------ -------- --------------- ----------- ---
        # DB             719522592 db1                 1 15-Jan-14 15:19 11.2.0.4.0  YES
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
        #####
        ##### Snap Time
        #                       ===================
        #               Snap Id      Snap Time      Sessions Curs/Sess Instances
        #             --------- ------------------- -------- --------- ---------
        # Begin Snap:       936 17-Jan-14 12:00:05        63       1.8         2
        #   End Snap:       937 17-Jan-14 13:00:10        61       1.8         2
        #    Elapsed:               60.09 (mins)
        #    DB Time:                1.49 (mins)
        #
        elif line.startswith('Begin Snap:') or line.startswith('  End Snap:'):
            dt = datetime.strptime(line2list(line, m_snaptm)[0], '%d-%b-%y %H:%M:%S')
            l_base.extend(str(x) for x in (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second))
            d_base = ','.join(l_base) + ','
        #####
        ##### Load Profile
        # 
        # -- 11.2.0.4 --
        # =======================    ===============   ===============
        # Load Profile                    Per Second   Per Transaction  Per Exec  Per Call
        # ~~~~~~~~~~~~~~~            ---------------   --------------- --------- ---------
        #              DB Time(s):               0.0               0.0      0.00      0.00
        #               DB CPU(s):               0.0               0.0      0.00      0.00
        #       Redo size (bytes):           4,450.4           1,216.2
        #                    :
        #            Transactions:               3.7
        #
        # <EOS>
        #
        # -- 11.2.0.1-3 --
        # ================     ===============    ===============
        # Load Profile              Per Second    Per Transaction   Per Exec   Per Call
        # ~~~~~~~~~~~~         ---------------    --------------- ---------- ----------
        #       DB Time(s):                2.3                0.0       0.00       0.00
        #        DB CPU(s):                1.5                0.0       0.00       0.00
        #        Redo size:          271,811.9              968.3
        #           :
        # <EOS>
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
            elif line.startswith('---'):
                b_data = True
            ##### extract data
            elif b_data:
                secv = section + '_03'
                if db_ver.startswith('11.2.0.4'):
                    secv = section + '_04'
                l_data = line2list(line, m[secv])
                f[secv].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
        #####
        ##### Top N Events
        # 
        # -- 11.2.0.4 --
        # ============================== ============ ==== ======= ====== ==========
        # Top 10 Foreground Events by Total Wait Time
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        #                                             Tota    Wait   % DB
        # Event                                 Waits Time Avg(ms)   time Wait Class
        # ------------------------------ ------------ ---- ------- ------ ----------
        # log file sync                       194,357 1024       5   52.6 Commit
        # DB CPU                                      558.           28.7
        # direct path read                     27,988 272.      10   14.0 User I/O
        #  :
        # <^L>
        # <EOS>
        # 
        # -- 11.2.0.1-3 --
        # ============================== ============ =========== ====== ====== ==========
        # Top 5 Timed Foreground Events
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        #                                                            Avg
        #                                                           wait   % DB
        # Event                                 Waits     Time(s)   (ms)   time Wait Class
        # ------------------------------ ------------ ----------- ------ ------ ----------
        # DB CPU                                              988          83.9
        # log file sync                        49,479          65      1    5.5 Commit
        #  :
        # <^L>
        # <EOS>
        #
        elif line.startswith('Top'):
            section = 'Events_TopN'
        elif section == 'Events_TopN':
            ##### <^L> (form feed, ascii=12) => section end
            if ord(line[0]) == 12:
                section = ''
                b_data = False
                l_data = []
            ##### begin data
            elif line.startswith('---'):
                b_data = True
            ##### extract data
            elif b_data:
                secv = section + '_03'
                if db_ver.startswith('11.2.0.4'):
                    secv = section + '_04'
                l_data = line2list(line, m[secv])
                f[secv].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
        #####
        ##### Foreground / Background Wait Events
        # ========================== ============ ===== ========== ======= ======== ======
        # <^L>Foreground Wait Events              DB/Inst: DB/db1  Snaps: 936-937
        # -> s  - second, ms - millisecond -    1000th of a second
        # -> :
        #
        #                                                              Avg
        #                                         %Time Total Wait    wait    Waits   % DB
        # Event                             Waits -outs   Time (s)    (ms)     /txn   time
        # -------------------------- ------------ ----- ---------- ------- -------- ------
        # log file sync                    12,310     0         44       4      0.9   49.3
        # direct path read                  1,676     0         16       9      0.1   17.4
        # SQL*Net break/reset to cli        6,978     0          2       0      0.5    1.7
        #   :
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ========================== ============ ===== ========== ======= ======== ======
        # <^L>Background Wait Events              DB/Inst: DB/db1  Snaps: 936-937
        # -> ordered by wait time desc, waits desc (idle events last)
        # -> :
        #
        #                                                              Avg
        #                                         %Time Total Wait    wait    Waits   % bg
        # Event                             Waits -outs   Time (s)    (ms)     /txn   time
        # -------------------------- ------------ ----- ---------- ------- -------- ------
        # log file parallel write          13,645     0         48       4      1.0   25.6
        # db file parallel write            1,166     0         41      35      0.1   21.7
        # Streams AQ: qmn coordinato           14    29         22    1577      0.0   11.7
        #   :
        # <^L>Background Wait Events              DB/Inst: DB/db1  Snaps: 936-937
        # -> ordered by wait time desc, waits desc (idle events last)
        # ->  :
        # Event                             Waits -outs   Time (s)    (ms)     /txn   time
        # -------------------------- ------------ ----- ---------- ------- -------- ------
        # PX Idle Wait                      1,362     0      3,599    2642      0.1
        #   :
        #                           ------------------------------------------------------
        # <EOS>
        #
        elif line.startswith(chr(12) + 'Foreground Wait Events'):
            section = 'Events_FG'
        elif line.startswith(chr(12) + 'Background Wait Events'):
            section = 'Events_BG'
        elif section in ['Events_FG', 'Events_BG']:
            ##### something like "    -----" => section end
            if re.match(r'^  +\-+', line):
                section = ''
                b_data = False
                l_data = []
            ##### 2 numbers with spaces in between => data
            elif re.match(r'^.+[\d\.\,]+ +[\d\.\,]+', line):
                l_data = line2list(line, m[section])
                f[section].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
        #####
        ##### SQL ordered by x
        # ================ ============== ============= ====== ====== ====== =============
        #         ========================================================================
        # <^L>SQL ordered by Elapsed Time         DB/Inst: DB/db1  Snaps: 914-938
        # -> Resources reported for PL/SQL code includes the resources used by all SQL
        #    statements called by the code.
        # -> :
        #
        #         Elapsed                  Elapsed Time
        #         Time (s)    Executions  per Exec (s)  %Total   %CPU    %IO    SQL Id
        # ---------------- -------------- ------------- ------ ------ ------ -------------
        #            176.3            192          0.92    9.1    7.9   91.0 bngf5tg0zms42
        # Module: xxxxxxx
        #  SQL...
        #
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ========== ============ ========== ====== ========== ====== ====== =============
        # <^L>SQL ordered by CPU Time             DB/Inst: DB/db1  Snaps: 914-938
        #  :
        #     CPU                   CPU per           Elapsed
        #   Time (s)  Executions    Exec (s) %Total   Time (s)   %CPU    %IO    SQL Id
        # ---------- ------------ ---------- ------ ---------- ------ ------ -------------
        #       26.3          684       0.04    4.7       45.4   58.0   31.3 6gvch1xu9ca3g
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ========== ============ ========== ====== ========== ====== ====== =============
        # <^L>SQL ordered by User I/O Wait Time   DB/Inst: DB/db1  Snaps: 936-937
        #  :
        #   User I/O                UIO per           Elapsed
        #   Time (s)  Executions    Exec (s) %Total   Time (s)   %CPU    %IO    SQL Id
        # ---------- ------------ ---------- ------ ---------- ------ ------ -------------
        #        9.8           12       0.82   35.4       10.7    7.2   91.8 9qw3r2ps37x3b
        #                           ------------------------------------------------------
        # <EOS>
        #
        # =========== =========== ============ ====== ========== ===== ===== =============
        # <^L>SQL ordered by Gets                 DB/Inst: DB/db1  Snaps: 936-937
        #  :
        #      Buffer                 Gets              Elapsed
        #       Gets   Executions   per Exec   %Total   Time (s)  %CPU   %IO    SQL Id
        # ----------- ----------- ------------ ------ ---------- ----- ----- -------------
        #     131,293          12     10,941.1   25.7       10.7   7.5  91.6 bngf5tg0zms42
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ========== ============ ========== ====== ========== ====== ====== =============
        # <^L>SQL ordered by Reads                DB/Inst: DB/db1  Snaps: 914-938
        #  :
        #    Physical              Reads              Elapsed
        #       Reads  Executions per Exec   %Total   Time (s)   %CPU    %IO    SQL Id
        # ----------- ----------- ---------- ------ ---------- ------ ------ -------------
        #   1,257,084         192    6,547.3   78.5      176.3    7.9   91.0 bngf5tg0zms42
        #                           ------------------------------------------------------
        # <EOS>
        #
        # =========== =========== ========== ============ ====== ====== =============
        # <^L>SQL ordered by Physical Reads (UnOptimized)DB/Inst: DB/db1  Snaps:
        #  :
        # UnOptimized   Physical              UnOptimized
        #   Read Reqs   Read Reqs Executions Reqs per Exe   %Opt %Total    SQL Id
        # ----------- ----------- ---------- ------------ ------ ------ -------------
        #       2,148       2,148         12        179.0    0.0   80.5 9qw3r2ps37x3b
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ============ =============== ============== ========== ===== ===== =============
        # <^L>SQL ordered by Executions           DB/Inst: DB/db1  Snaps: 936-937
        #  :
        #                                               Elapsed
        #  Executions   Rows Processed  Rows per Exec   Time (s)  %CPU   %IO    SQL Id
        # ------------ --------------- -------------- ---------- ----- ----- -------------
        #       17,435               0            0.0        0.7  22.4     0 adkz90xkcpx70
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ============ ============ ========= =============
        # <^L>SQL ordered by Parse Calls          DB/Inst: DB/db1  Snaps: 936-937
        #  :
        #                             % Total
        #  Parse Calls  Executions     Parses    SQL Id
        # ------------ ------------ --------- -------------
        #       17,435       17,435     33.93 adkz90xkcpx70
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ======== ============ =============
        # <^L>SQL ordered by Version Count     DB/Inst: DB/db2  Snaps: 5352-5353
        #  :
        #  Version
        #   Count   Executions     SQL Id
        # -------- ------------ -------------
        #       84            4 6mvfay19q3v4n
        #           -------------------------------------------------------------
        # <EOS>
        #
        # ============== ============ ====== ========== ====== ====== ====== =============
        # <^L>SQL ordered by Cluster Wait Time    DB/Inst: DB/db1  Snaps: 936-937
        #  :
        #        Cluster                        Elapsed
        #  Wait Time (s)   Executions %Total    Time(s)   %Clu   %CPU    %IO    SQL Id
        # -------------- ------------ ------ ---------- ------ ------ ------ -------------
        #             .5           77   33.9        0.5   90.9    3.8    5.7 3s58mgk0uy2ws
        #                           ------------------------------------------------------
        # <EOS>
        #
        elif line.startswith(chr(12) + 'SQL ordered by Elapsed Time'):
            section = 'SQL_Elapsed'
        elif line.startswith(chr(12) + 'SQL ordered by CPU Time'):
            section = 'SQL_CPU'
        elif line.startswith(chr(12) + 'SQL ordered by User I/O Wait Time'):
            section = 'SQL_UserIO'
        elif line.startswith(chr(12) + 'SQL ordered by Gets'):
            section = 'SQL_Gets'
        elif line.startswith(chr(12) + 'SQL ordered by Reads'):
            section = 'SQL_Reads'
        elif line.startswith(chr(12) + 'SQL ordered by Physical Reads (UnOptimized)'):
            section = 'SQL_Unoptim'
        elif line.startswith(chr(12) + 'SQL ordered by Executions'):
            section = 'SQL_Execs'
        elif line.startswith(chr(12) + 'SQL ordered by Parse Calls'):
            section = 'SQL_Parses'
        elif line.startswith(chr(12) + 'SQL ordered by Version Count'):
            section = 'SQL_Version'
        elif line.startswith(chr(12) + 'SQL ordered by Cluster Wait Time'):
            section = 'SQL_Cluster'
        elif section.startswith('SQL_'):
            ##### something like "    -----" => section end
            if re.match(r'^  +\-+', line):
                ##### if missing a "Module:", extract now
                if len(l_data) > 0:
                    f[section].write((d_base + ','.join(l_data + ['']) + '\n').encode('UTF-8'))
                section = ''
                l_data = []
                b_data = False
            ##### "Module:" => SQL end
            elif line.startswith('Module:'):
                l_data.append(line2list(line, m_module)[0])
                f[section].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
            ##### 2 numbers with spaces in between => data
            elif re.match(r'^ +[\d\.\,]+ +[\d\.\,]+', line):
                ##### if missing a "Module:", extract now
                if len(l_data) > 0:
                    f[section].write((d_base + ','.join(l_data + ['']) + '\n').encode('UTF-8'))
                ##### extract
                l_data = line2list(line, m[section])
        #####
        ##### Others (no random section headers within data)
        # ========================================== ================== ============
        # <^L>Time Model Statistics               DB/Inst: DB/db1  Snaps: 936-937
        # -> Total time in database user-calls (DB Time): 89.4s
        # ->  :
        #
        # Statistic Name                                       Time (s) % of DB Time
        # ------------------------------------------ ------------------ ------------
        # sql execute elapsed time                                 30.4         34.0
        # DB CPU                                                   30.2         33.8
        # parse time elapsed                                        2.9          3.3
        #    :
        # background elapsed time                                 391.4
        # background cpu time                                      65.4
        #                           ------------------------------------------------------
        # <EOS>
        #
        # ================================ ================== ============== =============
        # <^L>Key Instance Activity Stats         DB/Inst: DB/db1  Snaps: 936-937
        # -> Ordered by statistic name
        #
        # Statistic                                     Total     per Second     per Trans
        # -------------------------------- ------------------ -------------- -------------
        # db block changes                             89,906           24.9           6.8
        # execute count                                96,364           26.7           7.3
        # gc cr block receive time                         32            0.0           0.0
        #    :
        #                           ------------------------------------------------------
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
        # B        199         12      329.2        0.0     .0     .0     .0     40,755
        # E        199         12      326.1        0.0     .0     .0     .0     40,755
        #                           ------------------------------------------------------
        # <EOS>
        #
        elif line.startswith(chr(12) + 'Time Model Statistics'):
            section = 'Time_Model'
        elif line.startswith(chr(12) + 'Key Instance Activity Stats'):
            section = 'Key_Stats'
        elif line.startswith('PGA Aggr Target Stats'):
            section = 'PGA_Aggr'
        elif section in ['Time_Model', 'Key_Stats', 'PGA_Aggr']:
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
                l_data = line2list(line, m[section])
                f[section].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
        #####
        ##### Segment Statistics
        # ========== ========== ==================== ========== ===== ============ =======
        # <^L>Segments by Logical Reads        DB/Inst: DB/db1  Snaps: 5353-5354
        # -> Total Logical Reads:         206,082
        # -> Captured Segments account for   94.5% of Total
        #
        #            Tablespace                      Subobject  Obj.       Logical
        # Owner         Name    Object Name            Name     Type         Reads  %Total
        # ---------- ---------- -------------------- ---------- ----- ------------ -------
        # SYS        SYSTEM     USER$                           TABLE       85,072   41.28
        # SYS        SYSTEM     SYN$                            TABLE       28,288   13.73
        # SYS        SYSTEM     I_SYN1                          INDEX       24,960   12.11
        # SYS        SYSTEM     ATTRIBUTE$                      TABLE       11,776    5.71
        # SYS        SYSAUX     WRI$_ADV_PARAMETERS             TABLE        9,152    4.44
        #           -------------------------------------------------------------
        # <EOS>
        #
        # ========== ========== ==================== ========== ===== ============ =======
        # Segments by Physical Reads        DB/Inst: DB/db1  Snaps: 5353-5354
        # :
        #            Tablespace                      Subobject  Obj.      Physical
        # Owner         Name    Object Name            Name     Type         Reads  %Total
        # ---------- ---------- -------------------- ---------- ----- ------------ -------
        # SYS        SYSAUX     WRH$_SQLSTAT_INDEX   39982_1205 INDEX           56   17.83
        #           -------------------------------------------------------------
        # <EOS>
        #
        # ========== ========== ==================== ========== ===== ============ =======
        # Segments by UnOptimized Reads     DB/Inst: DB/db1  Snaps: 5353-5354
        # :
        #            Tablespace                      Subobject  Obj.   UnOptimized
        # Owner         Name    Object Name            Name     Type         Reads  %Total
        # ---------- ---------- -------------------- ---------- ----- ------------ -------
        # SYS        SYSAUX     WRH$_SQLSTAT_INDEX   39982_1205 INDEX           56   17.83
        #           -------------------------------------------------------------
        # <EOS>
        #
        # ========== ========== ==================== ========== ===== ============ =======
        # Segments by Physical Writes       DB/Inst: DB/db1  Snaps: 5353-5354
        # :
        #            Tablespace                      Subobject  Obj.      Physical
        # Owner         Name    Object Name            Name     Type        Writes  %Total
        # ---------- ---------- -------------------- ---------- ----- ------------ -------
        # SYS        SYSAUX     WRH$_SQLSTAT_INDEX   39982_1205 INDEX           61    5.91
        #           -------------------------------------------------------------
        # <EOS>
        #
        # ========== ========== ==================== ========== ===== ============ =======
        # <^L>Segments by CR Blocks Received   DB/Inst: DB/db1  Snaps: 5353-5354
        # :
        #                                                                    CR
        #            Tablespace                      Subobject  Obj.       Blocks
        # Owner         Name    Object Name            Name     Type      Received  %Total
        # ---------- ---------- -------------------- ---------- ----- ------------ -------
        # SYS        SYSAUX     AQ$_SYS$SERVICE_METR            TABLE          240   40.54
        #           -------------------------------------------------------------
        # <EOS>
        #
        # ========== ========== ==================== ========== ===== ============ =======
        # Segments by Current Blocks ReceivedDB/Inst: DB/db1  Snaps: 5353-535
        # :
        #                                                                  Current
        #            Tablespace                      Subobject  Obj.       Blocks
        # Owner         Name    Object Name            Name     Type      Received  %Total
        # ---------- ---------- -------------------- ---------- ----- ------------ -------
        # SYS        SYSAUX     SYS_IOT_TOP_13276               INDEX          138   16.81
        #           -------------------------------------------------------------
        # <EOS>
        #
        elif line.startswith(chr(12) + 'Segments by Logical Reads'):
            section = 'Seg_Logical'
        elif line.startswith('Segments by Physical Reads'):
            section = 'Seg_Physical'
        elif line.startswith('Segments by UnOptimized Reads'):
            section = 'Seg_UnOptim'
        elif line.startswith('Segments by Physical Writes'):
            section = 'Seg_Writes'
        elif line.startswith(chr(12) + 'Segments by CR Blocks Received'):
            section = 'Seg_CR'
        elif line.startswith('Segments by Current Blocks Received'):
            section = 'Seg_Cur'
        elif section.startswith('Seg_'):
            ##### something like "    -----" => section end
            if re.match(r'^  +\-+', line):
                section = ''
                b_data = False
                l_data = []
            ##### begin data
            elif line.startswith('---'):
                b_data = True
            ##### extract data
            elif b_data:
                l_data = line2list(line, m[section])
                f[section].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
        #####
        ##### Global Cache
        # ==============================        ===============       ===============
        # Global Cache Load Profile
        # ~~~~~~~~~~~~~~~~~~~~~~~~~                  Per Second       Per Transaction
        #                                       ---------------       ---------------
        #   Global Cache blocks received:                  0.53                  0.14
        #     Global Cache blocks served:                  0.70                  0.19
        #      GCS/GES messages received:                  7.90                  2.14
        #          GCS/GES messages sent:                  8.24                  2.23
        #             DBWR Fusion writes:                  0.09                  0.02
        #  Estd Interconnect traffic (KB)                 12.99
        #
        # <EOS>
        #
        # ==============================  =======
        # Global Cache Efficiency Percentages (Target local+remote 100%)
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Buffer access -  local cache %:   99.07
        # Buffer access - remote cache %:    0.41
        # Buffer access -         disk %:    0.51
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
                l_data = line2list(line, m[section])
                f[section].write((d_base + ','.join(l_data) + '\n').encode('UTF-8'))
