SHELL=/bin/bash

infolder=data
endfolder=dbfiles
PYTHON=pyenv/bin/python

ifndef fname
	fname=$(infolder)/20575_takecare.csv
endif


upload: upload-overview upload-overview2 upload-monitor_meta

chunk: $(fname)
	$(PYTHON) bin/chunk.py -i $^ -o $(endfolder)/$(shell basename $(fname))


upload-%: $(endfolder)/%.flag
	@echo ""


$(endfolder)/%.flag: $(endfolder)/%.csv
	$(PYTHON) bin/upload.py -i $^
	touch $@


prep-%: $(endfolder)/%.csv
	@echo ""

$(endfolder)/%.csv: $(infolder)/%.xlsx
	$(PYTHON) bin/prep.py -i $^ -o $@

.SECONDARY: