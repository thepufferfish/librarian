# Librarian

## About

This is a project I'm using to work with tools that aren't part of my typical workflow at work. The goal is to create a service merging multiple sources of data and machine learning algorithms to give users book reccomendations based on their reading history.

## Status

So far, I've created a web crawler using [scrapy](https://scrapy.org/) in Python to crawl over [Book Marks](https://bookmarks.reviews) which aggregates critical reviews of books. The crawl is slow with a 10s crawl delay so I broadcast the results as a JSON file using Kafka. Then, a Kafka consumer writes the raw data to a PostgreSQL database. This is overengineered&mdash;the crawler could write directly to the database&mdash;but I wanted to learn how to use Kafka. The whole process is monitored by Prometheus and Grafana and deployed via Docker Compose.

## Instructions

This is currently run on Ubuntu Server 24.02.2 LTS. To run, clone the repo and [install Docker](https://docs.docker.com/engine/install/ubuntu/). Once installed run the command:

```{bash}
docker compose up --build -d
```

## TODO:
* Use Spark to extract raw data, transform it, and load the transformed data into the database.
* Set up Airflow to orchestrate operations
* Introduce simple recommendation model and manage with MLflow