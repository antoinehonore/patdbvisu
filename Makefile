SHELL=/bin/bash

infolder=data
endfolder=dbfiles
PYTHON=pyenv/bin/python


upload: $(endfolder)/overview.csv $(endfolder)/monitor_meta.csv
	$(PYTHON) bin/upload.py -i $^

prep: $(endfolder)/overview.csv $(endfolder)/monitor_meta.csv

$(endfolder)/%.csv:$(infolder)/%.xlsx
	$(PYTHON) bin/prep.py -i $^ -o $@

