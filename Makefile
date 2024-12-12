dev-install:
	python -m venv .venv && \
	source .venv/bin/activate && \
	pip install poetry && \
	poetry install --no-root && \
	source .venv/bin/activate
	tcloud