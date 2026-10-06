FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# 平台会把外部可访问端口映射到 PORT；这里仅声明，实际由运行环境注入
ENV PORT=8080
EXPOSE 8080

CMD ["python", "app.py"]
