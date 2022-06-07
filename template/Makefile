.PHONY: docs clean

VIRTUALENV_DIR=.env
PYTHON=${VIRTUALENV_DIR}/bin/python
PIP=${VIRTUALENV_DIR}/bin/pip
stage=dev
env=local


all:
	pip3 install virtualenv
	virtualenv -p python3 $(VIRTUALENV_DIR)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

run:
	pip install -r requirements.txt
	python -B main.py $(stage) $(env) $(task) $(taskArn) nobackfill

run_backfill:
	pip install -r requirements.txt	
	python -B main.py $(stage) $(env) $(task) $(taskArn) backfill