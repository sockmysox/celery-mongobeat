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
# In your application code (e.g., a Flask view, Django management command, etc.)
from celery_mongobeat.helpers import ScheduleManager

# Get a manager instance. This automatically finds the current Celery app
# and uses its configuration to connect to MongoDB.
manager = ScheduleManager.from_celery_app()

# Example: Create a task to run every 30 seconds
task_doc = manager.create_interval_task(
    name='my-periodic-task',
    task='your_project.tasks.some_task',
    every=30,
    period='seconds',
    args=[1, 2, 3]
)
print(f"Upserted task '{task_doc['name']}' with ID: {task_doc['_id']}")

# Example: Create a task with a description and other custom fields
manager.create_crontab_task(
    name='daily-report',
    task='your_project.tasks.generate_report',
    minute='0',
    hour='4',  # Run at 4:00 AM daily
    description='This is a custom description for the daily report task.',
    owner='data-science-team',  # Custom field
    version='1.2'               # Custom field
)

# Example: Create a task that runs 5 times and then stops
manager.create_interval_task(
    name='run-five-times-task',
    task='your_project.tasks.some_task',
    every=60,
    period='seconds',
    max_run_count=5
)
print("Upserted task 'run-five-times-task' that will run 5 times.")

# Example: Retrieve a task by its name or ID
task_by_name = manager.get_task(name='my-periodic-task')
task_by_id = manager.get_task(id=task_doc['_id'])
print(f"Retrieved task '{task_by_name['name']}' by name.")
print(f"Retrieved task '{task_by_id['name']}' by ID.")

# Example: Update an existing task's description
updated_task = manager.update_task(name='daily-report', description='This is the new description.')
if updated_task:
    print(f"Updated task '{updated_task['name']}' with new description: {updated_task['description']}")

# Example: Disable a task (the task remains in the database)
manager.disable_task(name='my-periodic-task')
print("Disabled task 'my-periodic-task'.")

# Example: Delete a task (the task is permanently removed)
manager.delete_task(id=task_doc['_id'])
print(f"Deleted task with ID: {task_doc['_id']}")

# Example: Get all remaining enabled tasks
remaining_tasks = manager.get_tasks(enabled=True)
print(f"\nFound {len(remaining_tasks)} remaining enabled tasks:")
for task in remaining_tasks:
    print(f" - {task['name']} (ID: {task['_id']})")

### Creating Tasks from a Dictionary

The `create_*_task` methods are designed to be flexible. You can use Python's keyword argument unpacking (`**`) to create tasks from a dictionary. This is especially useful when processing data from an API or another data source.

Any keys in the dictionary that do not match a defined method parameter (like `name`, `task`, `every`, etc.) will be saved as custom fields in the task document.

```python
# Example data that might come from a web form or API
task_data = {
    'name': 'api-created-task',
    'task': 'your_project.tasks.process_data',
    'every': 15,
    'period': 'minutes',
    'args': [12345],
    'description': 'Process data from the API.',
    'owner': 'api-service',          # This will be saved as a custom field
    'request_id': 'xyz-789'          # This will also be saved
}

task_doc = manager.create_interval_task(**task_data)
print(f"Successfully created task from dictionary with ID: {task_doc['_id']}")
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

You can access the collection object directly from the manager instance:

```python
# schedules_collection = manager.collection

# For example, to find the most common task paths using an aggregation:
pipeline = [
    {"$group": {"_id": "$task", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
# most_common_tasks = list(schedules_collection.aggregate(pipeline))
print("Most common tasks:", most_common_tasks)
```
