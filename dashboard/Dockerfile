FROM python:3.11-slim

WORKDIR /app

COPY dashboard/requirements.txt /tmp/dashboard-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dashboard-requirements.txt

COPY dashboard/app.py /app/app.py

CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--server.port=8501"]

