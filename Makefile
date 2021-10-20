SHELL=/bin/bash

infolder=data
endfolder=dbfiles
PYTHON=pyenv/bin/python

ifndef fname
	fname=$(infolder)/20575_takecare.csv
endif

interm_fname=$(endfolder)/$(shell basename $(fname))

chunk: $(fname)
	$(PYTHON) bin/chunk.py -i $^ -o $(interm_fname)

upload: $(interm_fname).flag
	@echo ""

$(interm_fname).flag:$(interm_fname)
	$(PYTHON) bin/upload.py -i $^
	touch $@

prep: $(fname)
	$(PYTHON) bin/prep.py -i $^ -o $(shell echo $(interm_fname) | sed 's/.xlsx/.csv/g')

.SECONDARY: