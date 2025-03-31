PYTHON ?= python3

SHELL := bash
TEE_STDERR := tee >(cat 1>&2)

ifneq ($(findstring MSYS_NT,$(shell uname)),)
	ACTIVATE = $(VENV)/scripts/Activate.bat
else
	ACTIVATE = source $(VENV)/bin/activate
endif

ifeq ($(USERNAME),)
	USERNAME := $(USER)
endif

PYTEST_FLAGS_ := $(strip $(if $(filter-out 0,$(V)),-v -s,-q) $(PYTEST_FLAGS))

all: lint

prereq:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black -q --check . || ($(PYTHON) -m black .; false)
	$(PYTHON) -m isort -q --check . || ($(PYTHON) -m isort .; false)

test: FORCE
	$(PYTHON) -m pytest $(PYTEST_FLAGS_) test

$(VENV):
	$(PYTHON) -m venv $@

test-venv: VENV := test/venv
test-venv: $(VENV)
	$(ACTIVATE); $(MAKE) test-ci

test-ci:
	pip install -r requirements.txt .
	$(MAKE) pkg-test
	$(MAKE) test

pkg-build:
	$(PYTHON) -m build

pkg-install:
	$(PYTHON) -m pip install --force-reinstall dist/elastic_pipes-*.whl

pkg-test: FORMATS=json ndjson yaml
pkg-test:
	elastic-pipes version
	elastic-pipes new-pipe -f test/hello.py
	echo "test-result: ok" | $(PYTHON) test/hello.py | [ "`$(TEE_STDERR)`" = "test-result: ok" ]
	echo "name: $(USERNAME)" | $(PYTHON) test/hello.py | [ "`$(TEE_STDERR)`" = "name: $(USERNAME)" ]
	echo "test-result: ok" | elastic-pipes run --log-level=debug test/test.yaml | [ "`$(TEE_STDERR)`" = "test-result: ok" ]
	cat test/test.yaml | elastic-pipes run --log-level=debug - | [ "`$(TEE_STDERR)`" = "{}" ]
	@$(foreach SRC,$(FORMATS), \
		$(foreach DEST,$(FORMATS), \
			echo "$(SRC) -> $(DEST)"; \
			echo 'pipes: ["elastic.pipes.core.import": {"node": "documents", "file": "test/docs.$(SRC)"}, "elastic.pipes.core.export": {"node": "documents", "format": "$(DEST)"}]' | elastic-pipes run --log-level=debug - | [ "`$(TEE_STDERR)`" = "`cat test/docs.$(DEST)`" ] || exit 1; \
		) \
	)

package: VENV := test/venv
package: pkg-build
	rm -rf $(VENV)
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE); $(MAKE) pkg-install pkg-test
	rm -rf $(VENV)

clean:
	rm -rf build *.egg-info test/venv test/hello.py

.PHONY: FORCE
