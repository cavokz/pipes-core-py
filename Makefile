SHELL := bash

ifneq ($(VENV),)
	PYTHON ?= $(VENV)/bin/python3
else
	PYTHON ?= python3
endif

all: lint

prereq:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black -q --check . || ($(PYTHON) -m black .; false)
	$(PYTHON) -m isort -q --check . || ($(PYTHON) -m isort .; false)

test:
	$(PYTHON) -m venv test-env
	source test-env/bin/activate; $(MAKE) test-ci

test-ci:
	pip install .
	elastic-pipes new -f test-pipe.py
	echo "test-result: ok" | $(PYTHON) test-pipe.py | [ "`tee >(cat 1>&2)`" = "test-result: ok" ]
	echo "test-result: ok" | elastic-pipes run test.yaml | [ "`tee >(cat 1>&2)`" = "test-result: ok" ]
	cat test.yaml | elastic-pipes run - | [ "`tee >(cat 1>&2)`" = "{}" ]

clean:
	rm -rf test-env test-pipe.py
