venv_path = $(VENV_PATH)/tatenmitdaten/packages/blazel

venv:
	rm -rf $(venv_path)
	python3.12 -m venv $(venv_path)
	ln -sfn $(venv_path) venv
	$(venv_path)/bin/python -m pip install --upgrade pip-tools pip setuptools

install:
	$(venv_path)/bin/python -m pip install -e .[entra,sqlserver,dev]

setup: venv install

test:
	$(venv_path)/bin/pytest -W "ignore::DeprecationWarning"

check:
	$(venv_path)/bin/flake8 src/blazel --ignore=E501
	$(venv_path)/bin/mypy src/blazel --check-untyped-defs --python-executable $(venv_path)/bin/python

lock:
	$(venv_path)/bin/pip-compile --upgrade --strip-extras --build-isolation \
		--output-file example/extractload-layer/requirements.txt example/extractload-layer/requirements.in

driver:
	docker build -t pyodbc-3-12 -f driver/Dockerfile driver/.
	docker run --rm --user $(shell id -u):$(shell id -g) --entrypoint bash \
		-v $(shell pwd)/driver/odbc:/odbc pyodbc-3-12 \
		-c "cp -R /opt/* /odbc"

.PHONY: setup venv install test check lock driver