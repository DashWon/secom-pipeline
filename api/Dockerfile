FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# 시작 시 모델 학습 → API 서버 실행
CMD ["sh", "-c", "python train_model.py && uvicorn main:app --host 0.0.0.0 --port 8000"]
