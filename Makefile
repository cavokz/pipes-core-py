ifneq ($(VENV),)
	PYTHON ?= $(VENV)/bin/python3
else
	PYTHON ?= python3
endif

all: lint

prereq:
	$(PYTHON) -m pip install -r requirements.txt

lint: FORCE
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black -q --check . || ($(PYTHON) -m black .; false)
	$(PYTHON) -m isort -q --check . || ($(PYTHON) -m isort .; false)

.PHONY: FORCE
