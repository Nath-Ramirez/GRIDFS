FROM python:3.11-slim

WORKDIR /app
COPY app.py .

RUN pip install flask==3.0.3

EXPOSE 5001
CMD ["python", "app.py"]
