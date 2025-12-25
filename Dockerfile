ROM python:3.12

WORKDIR /mpattern

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 4000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "4000"]












