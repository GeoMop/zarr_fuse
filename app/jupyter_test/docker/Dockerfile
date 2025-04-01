FROM python:3.13.2

WORKDIR /app
COPY main.ipynb .
COPY pyproject.toml .

RUN pip install .

EXPOSE 8080
CMD ["voila", "main.ipynb", "--port=8080", "--no-browser", "--Voila.ip=0.0.0.0"]
