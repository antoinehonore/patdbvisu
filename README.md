# Overview

# Dev
## Features
- [x] Database overview
  - [x] Total size and number of patients
  - [x] Number of patients over time
    - [x] Views
    - [x] Display 
      - [x] More/less button to hide/show the display
      - [x] Figure with number of patient vs time
      - [x] Bar chart of the length of stay
  - [x] Data source overlap (Based on coinciding `ids__interval`)
    - [x] Views
    - [x] Dropdown list for source selection
    - [x] Table display of the number of overlapping patid/and intervals
- [x] Population study
  - [x] Choice of categories
    - [x] Views
    - [x] Front end
  - [ ] Several categories comparison
    - [x] Front end for populations choices (drop down lists) x N
    - [x] Download button (doesn t work in the Nextcloud embedded version)
    - [ ] Nice Display of demographics tables
- [ ] Patient display
  - [x] Patid search function
    - [x] Prevent SQL injection w strict casting (could use a long drop down list instead ?)
      - [x] for `ids__uid` (len==64) and ^[0-9a-zA-Z]*$
      - [x] for `pn` (len==13) and ^[0-9]?-[0-9]?$
    - [x] Input error display
    - [x] Return the overview row with only non-null columns
  - [ ] Port the patid convert function
    - [ ] **(partially done)** Load the patid/PN database in postgresql
    - [ ] Automatic db update upon new `ids__uid` encounter when loading `monitor_meta`
    - [x] Display the PN results
  - [ ] Patients intervals viewer
    - [ ] Selection of the data to plot
    - [ ] Display of the data


#  Data Upload
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
### Bulk load
- [x] data_monitor_past_pat**/LF__*.csv
- [x] data_monitor_past_pat**/HF__*.csv
- [x] data_monitor_offset_queries_pat**/LF__*.csv
- [x] data_monitor_offset_queries_pat**/HF__*.csv
- [x] data_monitor_pat{1..1000}/LF__*.csv
- [x] data_monitor_pat{1..1000}/HF__*.csv
- [x] data_monitor_pat{1000..1500}/LF__*.csv
- [x] data_monitor_pat{1000..1500}/HF__*.csv
- [x]  data_monitor_191SG-1_pat{1..1000}/LF__*.csv
- [x]  data_monitor_191SG-1_pat{1..1000}/HF__*.csv
- [x] data_monitor_191SG-1_pat{1000..2000}/LF__*.csv
- [x] data_monitor_191SG-1_pat{1000..2000}/HF__*.csv
- [x] data_monitor_NEO_1901_pat{1..1000}/LF__*.csv
- [x] data_monitor_NEO_1901_pat{1..1000}/HF__*.csv
- [ ] **(errors)** data_monitor_NEO_1901_pat{1000..2000}/LF__*.csv
- [ ] **(errors)** data_monitor_NEO_1901_pat{1000..2000}/HF__*.csv
- [x] data_monitor_NEO_191SG-1_pat{1..1000}/LF__*.csv
- [x] data_monitor_NEO_191SG-1_pat{1..1000}/HF__*.csv
- [x] data_monitor_NEO_191SG-1_pat{1000..2000}/LF__*.csv
- [x] data_monitor_NEO_191SG-1_pat{1000..2000}/HF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1..1000}/LF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1..1000}/HF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_2021__NEO_1901_pat{1000..2000}/HF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1..1000}/LF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1..1000}/HF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1000..2000}/LF__*.csv
- [ ] data_monitor_2021__NEO_191SG-1_pat{1000..2000}/HF__*.csv


## Fixing errors
1. From the start, some empty monitor data cells got the value "eJzjAgAACwAL" instead of NULL which fools the notnull filters used to find empty cells.
2. Database access got interrupted which caused many upload failures.
```bash
touch dbfiles/data_monitor_**/*.csv
```


# Administration

## Update monitor data
Assuming that monitoring data is to be added from a folder `new_folder`

### Prerequisites
#### Run the first parsing step
- Compute the summary files and the first parsed version for the data in `new_folder`
```bash
make -C ../patdb_bin/scripts -f update.mk data_folders_stem=<new_folder> mode=LF
[...]
make -C ../patdb_bin/scripts -f update.mk data_folders_stem=<new_folder> mode=HF
```

#### Watches
- In a tmux subpanel: Watch the `dbfiles` folder 
```bash
./upload.sh dbfiles 
```

- In a tmux subpanel: Watch the `data/monitor_meta` folder 
```bash
./notif.sh data/monitor_meta <n_jobs> prep
```

### Insert metadata 

- Compute the metadata file
```bash
make -C ../patdb_sync/scripts -f update.mk <new_folder>_list
ls data/monitor_meta/<new_folder>_meta_details.xlsx 
```

- The `prep` watch should be triggered and a file in `dbfiles` created.
- The `upload` watch should be triggered after the previous step.


### Insert data 
- In a tmux subpanel: Watch the `data/monitor/<`new_folder`>_pat**` files 
```bash
./notif.sh `data/monitor/<new_folder>_pat**` <n_jobs> prep
```

- Make sure the a watch in the `dbfiles` is active.

- If the folder contains lots of patients (> 1000), touch the files bulk by bulk:
```bash
touch data/monitor_parsed/<new_folder>_pat{1..1000}/LF__*.csv
touch data/monitor_parsed/<new_folder>_pat{1..1000}/HF__*.csv
touch data/monitor_parsed/<new_folder>_pat{1000..2000}/LF__*.csv
touch data/monitor_parsed/<new_folder>_pat{1000..2000}/HF__*.csv
...
```
This should trigger the `prep.py` script first and then the `upload.py`.  


## New category
See `queries/set_view.sql` and `queries/drop_view.sql`


### Associated to ids__uid only
- Add a view listing the unique patients of interest (column: ids__uid)
- Add a case in the view with binary column ids__uid categories


### Associated with sequential data 
#### monitor, clinisoft
- Add a view listing the interval and patients of interest (columns: ids__uid, ids__interval) 
- Add a case in the view with binary column ids__uid categories 
- Add a view listing the unique patients of interest (column: ids__uid)
- Add a case in the view with binary column ids__uid categories


#### takecare
See `bin/views.py`
- Add a key/value pair in dictionary `d`. The key should the name you want for your category. 
The value should be a regexp to apply to the column names of the takecare table.
- 




