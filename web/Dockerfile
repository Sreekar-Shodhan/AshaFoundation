FROM python:3.12-slim-bookworm

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
