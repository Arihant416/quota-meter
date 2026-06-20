FROM python:3.12-slim

WORKDIR /app

# install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy application code
COPY . .

# expose port
EXPOSE 8000

# default command — warmup then start
CMD ["sh", "-c", "python -m scripts.warmup && uvicorn app.main:app --host 0.0.0.0 --port 8000"]