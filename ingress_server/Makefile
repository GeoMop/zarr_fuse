.PHONY: venv install run clean

# Create virtual environment
venv:
	python3 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip setuptools wheel

# Install project dependencies + Airflow (Change constraints version as needed, default is for Python 3.12)
install: venv
	. .venv/bin/activate && \
	pip install ./services/ingress_service && \
	pip install ./services/extractor_service && \
	pip install ./packages/common && \
	pip install "apache-airflow[celery]==3.1.1" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.1/constraints-3.12.txt"

# Run your environment
run:
	@echo "Activating virtual environment..."
	@echo "Use 'deactivate' to exit."
	@bash --rcfile <(echo "source .venv/bin/activate && exec bash")

# Clean venv
clean:
	rm -rf .venv
