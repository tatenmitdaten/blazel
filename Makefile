VENV_PATH = $(VENV_ROOT)/tatenmitdaten/packages/blazel

venv:
	rm -rf $(VENV_PATH)
	uv venv $(VENV_PATH) --python 3.12
	ln -sfn $(VENV_PATH) venv
	uv pip install setuptools

install:
	uv pip install -e .[cli,entra,sqlserver,dev]

setup: venv install

test:
	$(VENV_PATH)/bin/pytest -W "ignore::DeprecationWarning"

check:
	$(VENV_PATH)/bin/flake8 src/blazel --ignore=E501
	$(VENV_PATH)/bin/mypy src/blazel --check-untyped-defs --python-executable $(VENV_PATH)/bin/python

lock:
	$(VENV_PATH)/bin/pip-compile --upgrade --strip-extras --build-isolation \
		--output-file example/extractload-layer/requirements.txt example/extractload-layer/requirements.in

driver:
	docker build -t pyodbc-3-12 -f driver/Dockerfile driver/.
	docker run --rm --user $(shell id -u):$(shell id -g) --entrypoint bash \
		-v $(shell pwd)/driver/odbc:/odbc pyodbc-3-12 \
		-c "cp -R /opt/* /odbc"

.PHONY: setup venv install test check lock driver