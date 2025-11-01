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
# In your application code (e.g., a management script or view)
from celery import current_app
from celery_mongobeat.helpers import ScheduleManager

# The recommended way to get a manager instance.
# It automatically reads the database configuration from your Celery settings.
app = current_app._get_current_object()
manager = ScheduleManager.from_celery_app(app)

# Example: Create a task to run every 30 seconds
manager.create_interval_task(
    name='my-periodic-task',
    task='your_project.tasks.some_task',
    every=30,
    period='seconds',
    args=[1, 2, 3]
)
print("Upserted interval task: 'my-periodic-task'")

# Example: Create a task with a description
manager.create_crontab_task(
    name='daily-report',
    task='your_project.tasks.generate_report',
    minute='0',
    hour='4',  # Run at 4:00 AM daily
    description='This is a custom description for the daily report task.'
)

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

# Example: Get all enabled interval tasks
enabled_interval_tasks = manager.get_tasks(enabled=True, schedule_type='interval')
print(f"Found {len(enabled_interval_tasks)} enabled interval tasks.")
for task in enabled_interval_tasks:
    print(f" - {task['name']}")

### Creating Tasks from a Dictionary

The `create_*_task` methods are designed to be flexible. You can use Python's keyword argument unpacking (`**`) to create tasks from a dictionary. This is especially useful when processing data from an API or another data source.

Any extra keys in the dictionary that do not match a method parameter will be safely ignored.

```python
# Example data that might come from a web form or API
task_data = {
    'name': 'api-created-task',
    'task': 'your_project.tasks.process_data',
    'every': 15,
    'period': 'minutes',
    'args': [12345],
    'metadata': 'Created by API endpoint /tasks',  # This key will be ignored
    'request_id': 'xyz-789'  # This key will also be ignored
}

task_id = manager.create_interval_task(**task_data)
print(f"Successfully created task from dictionary with ID: {task_id}")
```

```

### Advanced Usage: Subclassing and Direct Database Access

The `ScheduleManager` is designed to be a flexible base. For more complex applications, it is highly recommended to subclass it to create a domain-specific API for your tasks. This encapsulates your application's scheduling logic, making your code cleaner and more maintainable.
 
**1. Subclassing `ScheduleManager`**

```python
# In your_app/scheduling.py

from celery_mongobeat.helpers import ScheduleManager

class AppScheduleManager(ScheduleManager):
    """A custom manager for our application's specific tasks."""

    def create_user_report_task(self, user_id: int):
        """Creates a recurring daily report for a specific user."""
        task_name = f"user-report-{user_id}"
        super().create_crontab_task(
            name=task_name,
            task='your_app.tasks.generate_report',
            kwargs={'user_id': user_id},
            minute='0',  # At the start of the hour
            hour='3'     # At 3 AM
        )
        print(f"Scheduled daily report for user {user_id}.")

# In your application code, you can now use this custom manager:
# from celery import current_app
# from your_app.scheduling import AppScheduleManager

# app = current_app._get_current_object()
# app_manager = AppScheduleManager.from_celery_app(app)
# app_manager.create_user_report_task(user_id=123)
```

**2. Direct Database Access**

For advanced queries, such as MongoDB aggregation pipelines, you can and should use the `pymongo` collection object that you used to initialize the manager. This gives you the full power of `pymongo` for any use case not directly covered by the helper.

```python
# For example, to find the most common task paths using an aggregation:
pipeline = [
    {"$group": {"_id": "$task", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
most_common_tasks = list(schedules_collection.aggregate(pipeline))
print("Most common tasks:", most_common_tasks)
```
