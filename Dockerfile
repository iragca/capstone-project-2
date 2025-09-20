FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install -r backend_requirements.txt
RUN python -m main install-torch-geometric-dependencies --pip

EXPOSE 8001

CMD ["fastapi", "run", "mvps/mvp3_backend.py", "--host", "0.0.0.0", "--port", "8001"]