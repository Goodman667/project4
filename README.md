# Lightweight Message Broker

## Project Overview

This project is a course-demo version of a lightweight message broker for cloud microservices.

It is a single-machine, in-memory, topic-based broker built with FastAPI. The project demonstrates a simple message flow:

- producers publish messages to a topic
- consumers pull messages from a topic
- messages support `ack` and `nack`
- each topic exposes simple monitoring metrics
- publish requests can be throttled by a basic backpressure rule

The goal of this version is clarity and demo readiness, not production complexity.

## Project Structure

```text
app/
  main.py            FastAPI entrypoint
  api.py             HTTP API layer
  models.py          Pydantic models and internal topic state
  queue_manager.py   In-memory topic queue manager
  monitoring.py      Simple per-topic monitoring
  backpressure.py    Queue-depth based throttling
tests/
  test_queue.py
  test_monitoring.py
  test_backpressure.py
  test_api.py
scripts/
  producer_demo.py   Simple producer demo
  consumer_demo.py   Simple consumer demo
  burst_test.py      Burst-load demo
requirements.txt
README.md
```

This structure is reasonable for a course project because:

- `app/` keeps the service code grouped by responsibility
- `tests/` keeps unit and API tests separate from implementation
- `scripts/` provides ready-to-run demos for presentation
- the project stays small and easy to explain

## Features

This demo version includes:

- create topic
- publish message to topic
- consume one message from topic
- `ack` message
- `nack` message and requeue it
- per-topic queue depth query
- per-topic monitoring metrics
- simple backpressure based on queue depth

## Installation

Recommended environment:

- Python 3.11+
- Windows PowerShell or any shell that can run Python

Install dependencies:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If you do not want to create a virtual environment, you can still run:

```powershell
python -m pip install -r requirements.txt
```

## Run The Project

Start the FastAPI server:

```powershell
uvicorn app.main:app --reload
```

If `uvicorn` is not found in your terminal, use:

```powershell
python -m uvicorn app.main:app --reload
```

After startup, useful URLs are:

- [http://127.0.0.1:8000/](http://127.0.0.1:8000/) - browser control panel
- [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard) - browser control panel
- [http://127.0.0.1:8000/api/health](http://127.0.0.1:8000/api/health)
- [http://127.0.0.1:8000/api/docs](http://127.0.0.1:8000/api/docs)

Compatibility aliases are also available locally:

- `/health`
- `/docs`

## API Summary

Main endpoints:

- `POST /topics/{topic}`: create a topic
- `POST /topics/{topic}/publish`: publish one message
- `GET /topics/{topic}/consume`: consume one message
- `POST /messages/{message_id}/ack`: acknowledge one message
- `POST /messages/{message_id}/nack`: reject and requeue one message
- `GET /topics/{topic}/depth`: get queue depth
- `GET /topics/{topic}/metrics`: get topic metrics

## Run Tests

Run all tests with:

```powershell
python -m pytest -q
```

Current test coverage includes:

- queue manager behavior
- monitoring behavior
- backpressure behavior
- FastAPI endpoint behavior

## Demo Scripts

Make sure the FastAPI server is already running before using these scripts.

You can also do the whole demo from the browser control panel without switching between Swagger and terminal windows.

### 1. Producer Demo

Continuously publish messages to a topic:

```powershell
python scripts/producer_demo.py --topic demo-topic --count 10 --interval 0.5
```

What you will see:

- successful publish logs
- message ids for each published message
- throttling logs if queue depth becomes too large

### 2. Consumer Demo

Continuously pull messages from a topic and immediately `ack` them:

```powershell
python scripts/consumer_demo.py --topic demo-topic --consumer-id c1
```

What you will see:

- consumed message logs
- `ack` success logs
- idle logs when the queue is empty

### 3. Burst Test

Simulate burst traffic to observe queue depth growth and backpressure:

```powershell
python scripts/burst_test.py --topic burst-topic --count 150 --report-every 10
```

What you will see:

- accepted publish count
- throttled publish count
- queue depth growth
- simplified `message_rate`
- simplified `byte_throughput`

## Simplifications In This Version

This project intentionally stays in the course-demo scope.

What is included:

- single-process FastAPI service
- in-memory FIFO queue per topic
- simple `in_flight` tracking
- basic `ack` / `nack`
- basic monitoring counters
- queue-depth based backpressure

What is intentionally not included:

- persistence
- distributed broker nodes
- replication
- exactly-once delivery
- push-based consumer delivery
- tracing system
- authentication and authorization
- complex scheduling or adaptive control
- real production-grade fault tolerance

## Suggested Demo Flow

For a classroom presentation, a simple flow is:

1. Start the server
2. Create a topic
3. Run the producer demo
4. Run the consumer demo
5. Show `/topics/{topic}/depth`
6. Show `/topics/{topic}/metrics`
7. Run the burst test and demonstrate `429` backpressure behavior

## Notes

This version is designed to be easy to read, easy to test, and easy to explain in a short course presentation.
