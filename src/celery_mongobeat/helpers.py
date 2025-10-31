"""
Provides helper classes for programmatically managing Celery Beat schedules
in MongoDB.
"""
from typing import Any, Dict, List, Optional

from pymongo.collection import Collection


class ScheduleManager:
    """
    A helper class to simplify creating, updating, and managing schedule
    entries in the MongoDB collection used by MongoScheduler.

    This provides a programmatic API for users who prefer not to construct
    the MongoDB documents manually.

    :param collection: A `pymongo.collection.Collection` instance pointing to
                       the Celery Beat schedule collection.
    """

    def __init__(self, collection: Collection):
        self.collection = collection

    def create_interval_task(
            self, name: str, task: str, every: int, period: str = 'seconds',
            args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None,
            max_run_count: Optional[int] = None
    ):
        """
        Creates or updates a task that runs on a fixed interval.

        :param name: The unique name for the task.
        :param task: The name of the Celery task to run (e.g., 'your_app.tasks.add').
        :param every: The frequency of the interval.
        :param period: The type of interval ('days', 'hours', 'minutes', 'seconds', 'microseconds').
        :param args: A list of positional arguments for the task.
        :param kwargs: A dictionary of keyword arguments for the task.
        :param max_run_count: The maximum number of times the task can run before being disabled.
        """
        schedule_doc = {
            'name': name,
            'task': task,
            'enabled': True,
            'interval': {'every': every, 'period': period},
            'args': args or [],
            'kwargs': kwargs or {},
        }
        if max_run_count is not None:
            schedule_doc['max_run_count'] = max_run_count

        self.collection.update_one({'name': name}, {'$set': schedule_doc}, upsert=True)

    def create_crontab_task(
            self, name: str, task: str, minute: str = '*', hour: str = '*',
            day_of_week: str = '*', day_of_month: str = '*', month_of_year: str = '*',
            args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None
    ):
        """
        Creates or updates a task that runs on a crontab schedule.

        :param name: The unique name for the task.
        :param task: The name of the Celery task to run.
        :param minute: The minute(s) to run the task (0-59).
        :param hour: The hour(s) to run the task (0-23).
        :param day_of_week: The day(s) of the week to run the task (0-6 or names).
        :param day_of_month: The day(s) of the month to run the task (1-31).
        :param month_of_year: The month(s) of the year to run the task (1-12).
        :param args: A list of positional arguments for the task.
        :param kwargs: A dictionary of keyword arguments for the task.
        """
        schedule_doc = {
            'name': name,
            'task': task,
            'enabled': True,
            'crontab': {
                'minute': minute, 'hour': hour, 'day_of_week': day_of_week,
                'day_of_month': day_of_month, 'month_of_year': month_of_year
            },
            'args': args or [],
            'kwargs': kwargs or {},
        }
        self.collection.update_one({'name': name}, {'$set': schedule_doc}, upsert=True)

    def create_solar_task(
            self, name: str, task: str, event: str, lat: float, lon: float,
            args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None
    ):
        """
        Creates or updates a task that runs on a solar event schedule (e.g., sunrise, sunset).

        :param name: The unique name for the task.
        :param task: The name of the Celery task to run.
        :param event: The solar event ('sunrise', 'sunset', 'dawn_astronomical', etc.).
        :param lat: The latitude for the location.
        :param lon: The longitude for the location.
        :param args: A list of positional arguments for the task.
        :param kwargs: A dictionary of keyword arguments for the task.
        """
        schedule_doc = {
            'name': name, 'task': task, 'enabled': True,
            'solar': {'event': event, 'lat': lat, 'lon': lon},
            'args': args or [], 'kwargs': kwargs or {},
        }
        self.collection.update_one({'name': name}, {'$set': schedule_doc}, upsert=True)

    def disable_task(self, name: str):
        """
        Disables a task by its unique name, preventing it from being scheduled.

        :param name: The unique name of the task to disable.
        """
        self.collection.update_one({'name': name}, {'$set': {'enabled': False}})

    def enable_task(self, name: str):
        """
        Enables a task by its unique name, allowing it to be scheduled.

        :param name: The unique name of the task to enable.
        """
        self.collection.update_one({'name': name}, {'$set': {'enabled': True}})

    def get_task(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a task document from the database by its unique name.

        :param name: The unique name of the task to retrieve.
        :return: A dictionary representing the task document, or None if not found.
        """
        return self.collection.find_one({'name': name})

    def delete_task(self, name: str):
        """
        Permanently deletes a task from the schedule by its unique name.

        :param name: The unique name of the task to delete.
        """
        self.collection.delete_one({'name': name})

    def get_tasks(self, **filters: Any) -> List[Dict[str, Any]]:
        """
        Retrieves a list of task documents from the database, with optional filtering.

        This method allows for flexible querying using keyword arguments.
        - For simple filters: `get_tasks(enabled=True)`
        - For schedule type: `get_tasks(schedule_type='interval')`
        - For nested fields (like in kwargs): `get_tasks(kwargs__customer_id=123)`

        :param filters: Keyword arguments to use as a query filter.
        :return: A list of dictionaries, where each dictionary is a task document.
        """
        query: Dict[str, Any] = {}
        for key, value in filters.items():
            if key == 'schedule_type':
                if value not in ['interval', 'crontab', 'solar']:
                    raise ValueError("schedule_type must be one of 'interval', 'crontab', or 'solar'")
                query[value] = {'$exists': True}
            else:
                # Convert double-underscore notation to dot notation for nested queries
                # e.g., kwargs__customer_id -> kwargs.customer_id
                mongo_key = key.replace('__', '.')
                query[mongo_key] = value

        return list(self.collection.find(query))

    def count_tasks(self, **filters: Any) -> int:
        """
        Counts task documents in the database, with optional filtering.

        This is more efficient than `len(get_tasks(...))` as it performs
        the count on the database server. It allows for flexible querying
        using keyword arguments, similar to `get_tasks`.

        :param filters: Keyword arguments to use as a query filter.
        :return: The number of tasks matching the filter.
        """
        query: Dict[str, Any] = {}
        for key, value in filters.items():
            if key == 'schedule_type':
                if value not in ['interval', 'crontab', 'solar']:
                    raise ValueError("schedule_type must be one of 'interval', 'crontab', or 'solar'")
                query[value] = {'$exists': True}
            else:
                mongo_key = key.replace('__', '.')
                query[mongo_key] = value

        return self.collection.count_documents(query)