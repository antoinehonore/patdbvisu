SHELL=/bin/bash

infolder=data
endfolder=dbfiles
PYTHON=pyenv/bin/python

ifndef fname
	fname=$(infolder)/20575_takecare.csv
endif

ifndef logfile
	logfile=log/patdbvisu_$(shell date +"%Y%m%d").log
endif


interm_fname=$(shell echo $(endfolder)/$(shell basename $(shell dirname $(fname)))/$(shell basename $(fname) | sed -e 's/[,()]//g' -e 's/:/-/g' -e 's/ /__/g' -e 's/%/perc/g' -e 's/.*/\L&/') | sed 's/.xlsx/.csv/g')


test:
	@echo  $(interm_fname)
	@echo $(fname)

run:
	gunicorn -b 127.0.0.1:8050 app:server --timeout 600

upload: $(interm_fname).flag
	@echo ""

$(interm_fname).flag: $(interm_fname)
	$(PYTHON) bin/upload.py -i $^ 2>> $(logfile)
	touch $@

prep: $(interm_fname)
	@echo ""

$(interm_fname):
	$(PYTHON) bin/prep.py -i "$(fname)" -o $@  2>> $(logfile)


.SECONDARY:


