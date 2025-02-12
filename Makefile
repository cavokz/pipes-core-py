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
	./test-env/bin/pip install .
	./test-env/bin/elastic-pipes new -f test-env/bin/test-pipe
	echo "test-result: ok" | ./test-env/bin/python3 test-env/bin/test-pipe.py | [ "`tee /dev/stderr`" = "test-result: ok" ]
	echo "test-result: ok" | ./test-env/bin/elastic-pipes run test.yaml | [ "`tee /dev/stderr`" = "test-result: ok" ]

clean:
	rm -rf test-env
