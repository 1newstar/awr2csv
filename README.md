awr2csv
=======
[![Build Status](https://travis-ci.org/yasushiyy/awr2csv.svg?branch=master)](https://travis-ci.org/yasushiyy/awr2csv)

AWR text/html reports to csv converter

## Usage

HTML Reports
```
$ python awrhtml2csv.py *.html
Processing awr_11204_1.html...
Processing awr_11204_2.html...
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
$ python awrtext2csv.py *.txt
Processing awr_11204_1.txt...
  DB Version: 11.2.0.4.0
Processing awr_11204_2.txt...
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

## Comment

As you can see in the code, HTML version is so much simpler.  Text version is a pure nightmare.  Use HTML.  It's 21st century.

