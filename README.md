awr2csv
=======
[![Build Status](https://travis-ci.org/yasushiyy/awr2csv.svg?branch=master)](https://travis-ci.org/yasushiyy/awr2csv)

Converts Automatic Workload Repository (AWR) reports into csv.  Works with html/text version.

## Usage

HTML Reports
```
$ python awrhtml2csv.py sample/*.html
Processing sample/awr_11204_1.html...
Processing sample/awr_11204_2.html...
  Created: seg_phys_reads.csv
  Created: sql_reads.csv
  Created: sql_elapsed.csv
  Created: events_background.csv
  Created: sql_cpu.csv
  Created: sql_executions.csv
  Created: sql_unoptimized.csv
  Created: seg_cr_rcvd.csv
  Created: seg_current_rcvd.csv
  Created: sql_gets.csv
  Created: load_profile_g.csv
  Created: key_stats.csv
  Created: events_topn.csv
  Created: seg_unoptimized.csv
  Created: time_model.csv
  Created: efficiency_g.csv
  Created: sql_parses.csv
  Created: seg_logical.csv
  Created: events_foreground.csv
  Created: pga_aggr_stats.csv
  Created: sql_user_io.csv
  Created: load_profile.csv
  Created: sql_cluster.csv
  Created: seg_phys_writes.csv
```

Text Reports
```
$ python awrtext2csv.py sample/*.txt
Processing sample/awr_11204_1.txt...
  DB Version: 11.2.0.4.0
Processing sample/awr_11204_2.txt...
  DB Version: 11.2.0.4.0
  Created: seg_phys_reads.csv
  Created: sql_reads.csv
  Created: sql_elapsed.csv
  Created: events_background.csv
  Created: sql_cpu.csv
  Created: events_foreground.csv
  Created: sql_unoptimized.csv
  Created: seg_cr_rcvd.csv
  Created: seg_current_rcvd.csv
  Created: sql_gets.csv
  Created: load_profile_g.csv
  Created: key_stats.csv
  Created: events_topn.csv
  Created: seg_unoptimized.csv
  Created: time_model.csv
  Created: efficiency_g.csv
  Created: sql_parses.csv
  Created: seg_logical.csv
  Created: seg_phys_writes.csv
  Created: sql_executions.csv
  Created: sql_version.csv
  Created: load_profile.csv
  Created: sql_user_io.csv
  Created: sql_cluster.csv
  Created: pga_aggr_stats.csv
```

Run sql against CSV
```
$ python sqlcsv.py "select inst_num, b_h, name, per_second from csv where name like 'Execute%'" sample/load_profile.csv
Processing sample/load_profile.csv...
Create: CREATE TABLE csv (CSVNAME TEXT,DB_NAME TEXT,DB_ID INTEGER,INSTANCE_NAME TEXT,INST_NUM INTEGER,B_Y INTEGER,B_MO INTEGER,B_D INTEGER,B_H INTEGER,B_MI INTEGER,B_S INTEGER,E_Y INTEGER,E_MO INTEGER,E_D INTEGER,E_H INTEGER,E_MI INTEGER,E_S INTEGER,Name TEXT,Per_Second REAL,Per_Transaction REAL,Per_Exec REAL,Per_Call REAL)
Insert: 42 rows.
INST_NUM,B_H,Name,Per_Second
1,4,Executes (SQL),1.0
2,4,Executes (SQL),4.4
```

## Comment

HTML version is so much simpler.  Text version is a pure nightmare.  Use HTML.  It's the 21st century.
