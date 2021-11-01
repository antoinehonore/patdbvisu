# Upload
## Method

- Start the upload watches
```bash
$ ./upload.sh dbfiles
```

- Start the watches on the directory  the changes in directory. Wait for the message: `Watches established.`
```bash
$ ./notif.sh "<folders to monitor>"  5 prep
[...]
Watches established.
```

- `touch` the files in the list below sequentially. Make sure that the watches are started on the folder containing the files.


## Plan

- [x] data_monitor_past_pat**/LF__*.csv
- [x] data_monitor_past_pat**/HF__*.csv
- [x] data_monitor_offset_queries_pat**/LF__*.csv
- [x] data_monitor_offset_queries_pat**/HF__*.csv
- [x] data_monitor_pat{1..1000}/LF__*.csv
- [x] data_monitor_pat{1..1000}/HF__*.csv
- [x] data_monitor_pat{1000..1500}/LF__*.csv
- [x] data_monitor_pat{1000..1500}/HF__*.csv
- [ ] **(on going...)** data_monitor_191SG-1_pat{1..1000}/LF__*.csv
- [ ] **(on going...)** data_monitor_191SG-1_pat{1..1000}/HF__*.csv
- [ ] data_monitor_191SG-1_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_191SG-1_pat{1000..2000}/HF__*.csv
- [ ] data_monitor_NEO_1901_pat{1..1000}/LF__*.csv
- [ ] data_monitor_NEO_1901_pat{1..1000}/HF__*.csv
- [ ] data_monitor_NEO_1901_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_NEO_1901_pat{1000..2000}/HF__*.csv
- [ ] data_monitor_NEO_191SG-1_pat{1..1000}/LF__*.csv
- [ ] data_monitor_NEO_191SG-1_pat{1..1000}/HF__*.csv
- [ ] data_monitor_NEO_191SG-1_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_NEO_191SG-1_pat{1000..2000}/HF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1..1000}/LF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1..1000}/HF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1000..2000}/HF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1..1000}/LF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1..1000}/HF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1000..2000}/HF__*.csv


# New category
See `queries/set_view.sql` and `queries/drop_view.sql`


## Associated to ids__uid only
- Add a view listing the unique patients of interest (column: ids__uid)
- Add a case in the view with binary column ids__uid categories


## Associated with sequential data 
### monitor, clinisoft
- Add a view listing the interval and patients of interest (columns: ids__uid, ids__interval) 
- Add a case in the view with binary column ids__uid categories 
- Add a view listing the unique patients of interest (column: ids__uid)
- Add a case in the view with binary column ids__uid categories


### takecare
See `bin/views.py`
- Add a key/value pair in dictionary `d`. The key should the name you want for your category. 
The value should be a regexp to apply to the column names of the takecare table.
- 




