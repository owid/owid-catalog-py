#
#  Makefile
#


include default.mk

SRC = owid tests

# watch:
# 	poetry run watchmedo shell-command -c 'clear; make unittest' --recursive --drop .

check-typing:
	@echo '==> Checking types'
	PYTHONPATH=. poetry run mypy --strict .

coverage:
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=owid --cov-report=term-missing tests
