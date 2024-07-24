FROM python:3.9-slim-bookworm

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY /app .

ENV APP_TOKEN=ef0oV4r2jOuH9KGAEwWRQfrKl
ENV SERVER_NAME=sfo-crime-db.clwwaykoctjv.us-west-1.rds.amazonaws.com
ENV DATABASE_NAME=postgres
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=postgres
ENV PORT=5432
ENV LOGGING_SERVER_NAME=sfo-crime-db.clwwaykoctjv.us-west-1.rds.amazonaws.com
ENV LOGGING_DATABASE_NAME=postgres
ENV LOGGING_USERNAME=postgres
ENV LOGGING_PASSWORD=postgres
ENV LOGGING_PORT=5432

CMD ["python", "-m", "etl_project.pipelines.ETL_Pipeline"]