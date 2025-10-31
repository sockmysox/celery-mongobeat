# Celery-MongoBeat

<!--
[![PyPI Version](https://img.shields.io/pypi/v/celery-mongobeat.svg)](https://pypi.org/project/celery-mongobeat/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/celery-mongobeat)](https://pypi.org/project/celery-mongobeat)
[![License](https://img.shields.io/pypi/l/celery-mongobeat.svg)](https://opensource.org/licenses/Apache-2.0)
-->

A modern, drop-in replacement for `celerybeat-mongo`. This project provides a Celery Beat scheduler that stores and retrieves task schedules from a MongoDB collection, allowing for dynamic management of periodic tasks without restarting the Celery Beat service.

## Why `celery-mongobeat`?

The original `celerybeat-mongo` library is no longer actively maintained and contains several critical bugs. This project was created to provide a stable, reliable, and modern alternative for the community, ensuring continued support for dynamic, database-backed Celery schedules.

## Features

- **Stable and Reliable**: Fixes critical bugs from `celerybeat-mongo`, such as the issue where disabling one task would prevent all tasks from running.
- **Dynamic Task Management**: Add, modify, and remove periodic tasks on the fly without restarting the beat service.
- **MongoDB Backend**: Leverages MongoDB for a robust and scalable schedule store.
- **Fine-Grained Control**:
  - **Run Count Limiting**: Use `max_run_count` to run a task a specific number of times and then automatically disable it.
- **Flexible Configuration**: Full support for advanced `pymongo.MongoClient` options (like SSL) via `mongodb_scheduler_client_kwargs`.
- **Backwards Compatible**: Supports legacy configuration variables from `celerybeat-mongo` for a smoother transition.
- **Modern Tooling**: Built with a modern Python packaging structure (`pyproject.toml`).
- **All Schedule Types**: Natively supports `interval`, `crontab`, and `solar` schedules.

## Installation

Install the package from PyPI:

```bash
pip install celery-mongobeat
```

## Configuration

To use this scheduler, set the `beat_scheduler` option in your Celery configuration.

### Recommended Configuration

```python
# celeryconfig.py

mongodb_scheduler_url = "mongodb://localhost:27017/"
mongodb_scheduler_db = "celery"
mongodb_scheduler_collection = "schedules"

beat_scheduler = "celery_mongobeat.beat:MongoScheduler"
```

## Migrating from `celerybeat-mongo`

`celery-mongobeat` is designed as a near drop-in replacement, but there is one important configuration change you must make when migrating:

*   **Update the Scheduler Path**: The import path for the scheduler has been updated to align with modern package structures and Celery best practices.

You must change your `beat_scheduler` setting from:
`'celerybeat_mongo.schedulers.MongoScheduler'` (the old path)
to:
`'celery_mongobeat.beat:MongoScheduler'` (the new path)
```

### Legacy (Backwards-Compatible) Configuration

If you are migrating from `celerybeat-mongo`, this library provides backward compatibility for the uppercase configuration variables. Modern, lowercase settings (e.g., `mongodb_scheduler_url`) will always take precedence.

```python
# celeryconfig.py

# Legacy uppercase individual settings (from celerybeat-mongo)
CELERY_MONGODB_SCHEDULER_URL = "mongodb://localhost:27017/"
CELERY_MONGODB_SCHEDULER_DB = "celery"
CELERY_MONGODB_SCHEDULER_COLLECTION = "schedules"

beat_scheduler = "celery_mongobeat.beat:MongoScheduler"
```

## Usage

Once configured, start Celery Beat as you normally would:

```bash
celery -A your_app beat -l info
```

You can now manage your schedules by adding, updating, or removing documents in the configured MongoDB collection.

## Programmatic Usage Example

For users who prefer a programmatic API over manually inserting documents into MongoDB, `celery-mongobeat` provides a convenient `ScheduleManager` helper class.

This allows you to easily create, update, and disable tasks from within your application code.

### Example Usage

```python
# In your application's setup code
from pymongo import MongoClient
from celery_mongobeat.helpers import ScheduleManager

client = MongoClient("mongodb://localhost:27017/")
db = client["celery"]
schedules_collection = db["schedules"]  # Must match your celery-mongobeat config

manager = ScheduleManager(schedules_collection)

# Example: Create a task to run every 30 seconds
manager.create_interval_task(
    name='my-periodic-task',
    task='your_project.tasks.some_task',
    every=30,
    period='seconds',
    args=[1, 2, 3]
)
print("Upserted interval task: 'my-periodic-task'")

# Example: Create a task that runs 5 times and then stops
manager.create_interval_task(
    name='run-five-times-task',
    task='your_project.tasks.some_task',
    every=60,
    period='seconds',
    max_run_count=5
)
print("Upserted limited-run task: 'run-five-times-task'")

# Example: Disable a task
manager.disable_task('my-periodic-task')
print("Disabled task: 'my-periodic-task'")

```