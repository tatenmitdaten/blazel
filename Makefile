venv_path = $(VENV_PATH)/tatenmitdaten/packages/extractload

venv:
	rm -rf $(venv_path)
	python3.12 -m venv $(venv_path)
	ln -sfn $(venv_path) venv
	$(venv_path)/bin/python -m pip install --upgrade pip-tools pip setuptools

install:
	$(venv_path)/bin/python -m pip install -e .[dev]

setup: venv install

test:
	$(venv_path)/bin/pytest -W "ignore::DeprecationWarning"

check:
	$(venv_path)/bin/flake8 "extracload" --ignore=E501
	$(venv_path)/bin/mypy "extracload" --check-untyped-defs --python-executable $(venv_path)/bin/python

lock:
	$(venv_path)/bin/pip-compile --upgrade --strip-extras --build-isolation \
		--output-file example/extractload-layer/requirements.txt example/extractload-layer/requirements.in

.PHONY: setup venv install test check lock