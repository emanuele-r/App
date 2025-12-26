
FROM python:3.12-slim

WORKDIR /app


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


COPY . .


EXPOSE 4000


CMD ["uvicorn", "api_ps:app", "--host", "0.0.0.0", "--port", "4000", "--workers", "4"]










