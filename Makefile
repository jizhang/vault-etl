default: test

dev:
	poetry install

test:
	poetry run ruff --fix kiwi dw_run.py
	poetry run mypy kiwi dw_run.py
