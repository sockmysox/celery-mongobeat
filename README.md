# Celery-MongoBeat

A modern, drop-in replacement for `celerybeat-mongo`. This project provides a Celery Beat scheduler that stores and retrieves task schedules from a MongoDB collection, allowing for dynamic management of periodic tasks without restarting the Celery Beat service.

## Features

- **Dynamic Task Scheduling**: Add, modify, and remove periodic tasks on the fly.
- **MongoDB Backend**: Leverages MongoDB for a robust and scalable schedule store.
- **Backwards Compatible**: Designed as a drop-in replacement for the deprecated `celerybeat-mongo`. It supports the legacy `mongodb_backend_settings` configuration.
- **Modern Tooling**: Built with a modern Python packaging structure (`pyproject.toml`).

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

beat_scheduler = "celery_mongobeat.schedulers:MongoScheduler"
```

### Legacy (Backwards-Compatible) Configuration

If you are migrating from `celerybeat-mongo`, you can use your existing configuration.

```python
# celeryconfig.py

mongodb_backend_settings = {
    "host": "mongodb://localhost:27017/",
    "database": "celery",
    "collection": "schedules"
}

beat_scheduler = "celery_mongobeat.schedulers:MongoScheduler"
```

## Usage

Once configured, start Celery Beat as you normally would:

```bash
celery -A your_app beat -l info
```

You can now manage your schedules by adding, updating, or removing documents in the configured MongoDB collection.

## Programmatic Usage Example

While you can manage schedules by inserting raw documents into MongoDB, it's often cleaner to use a helper class within your application. Here is an example of a `ScheduleManager` class that you could use in your project to programmatically create, update, and find tasks.

This example is framework-agnostic and uses `pymongo` directly.

```python
from pymongo.collection import Collection

class ScheduleManager:
    """A helper class to manage schedule entries in MongoDB."""

    def __init__(self, collection: Collection):
        self.collection = collection

    def create_interval_task(self, name: str, task: str, every: int, period: str = 'seconds', args=None, kwargs=None):
        """Creates a task that runs on a fixed interval."""
        args = args or []
        kwargs = kwargs or {}
        schedule_doc = {
            'name': name,
            'task': task,
            'enabled': True,
            'interval': {'every': every, 'period': period},
            'args': args,
            'kwargs': kwargs,
        }
        self.collection.update_one(
            {'name': name},
            {'$set': schedule_doc},
            upsert=True
        )
        print(f"Upserted interval task: '{name}'")

    def create_crontab_task(self, name: str, task: str, minute='*', hour='*', day_of_week='*', args=None, kwargs=None):
        """Creates a task that runs on a crontab schedule."""
        args = args or []
        kwargs = kwargs or {}
        schedule_doc = {
            'name': name,
            'task': task,
            'enabled': True,
            'crontab': {'minute': minute, 'hour': hour, 'day_of_week': day_of_week},
            'args': args,
            'kwargs': kwargs,
        }
        self.collection.update_one(
            {'name': name},
            {'$set': schedule_doc},
            upsert=True
        )
        print(f"Upserted crontab task: '{name}'")

    def disable_task(self, name: str):
        """Disables a task by its unique name."""
        self.collection.update_one(
            {'name': name},
            {'$set': {'enabled': False}}
        )
        print(f"Disabled task: '{name}'")

```

You could then use this manager in your application like so:

```python
# In your application's setup code
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["celery"]
schedules_collection = db["schedules"] # Must match your celery-mongobeat config

manager = ScheduleManager(schedules_collection)

# Example: Create a task to run every 30 seconds
manager.create_interval_task(
    name='my-periodic-task',
    task='your_project.tasks.some_task',
    every=30,
    period='seconds',
    args=[1, 2, 3]
)

```