"""
Provides helper classes for programmatically managing Celery Beat schedules
in MongoDB.
"""
import datetime
from typing import Any, Dict, List, Optional, Union

from bson import ObjectId
from celery import Celery, current_app as celery_current_app
from pymongo import MongoClient
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

    def _sanitize_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively converts a MongoDB task document into a JSON-serializable dictionary.
        This handles BSON types like ObjectId and datetime.
        """
        if not task:
            return {}

        sanitized = {}
        for key, value in task.items():
            if isinstance(value, ObjectId):
                sanitized[key] = str(value)
            elif isinstance(value, datetime.datetime):
                sanitized[key] = value.isoformat()
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_task(value)
            elif isinstance(value, list):
                sanitized[key] = [
                    self._sanitize_task(item) if isinstance(item, dict) else item
                    for item in value
                ]
            elif isinstance(value, (str, int, float, bool, type(None))):
                sanitized[key] = value
            else:
                # Fallback for any other non-serializable types
                sanitized[key] = str(value)

        return sanitized

    @classmethod
    def from_celery_app(cls, app: Optional[Celery] = None, client: Optional[MongoClient] = None) -> 'ScheduleManager':
        """
        Creates a ScheduleManager instance from a Celery app object.

        This is the recommended way to get a manager instance, as it automatically
        uses the database configuration from your Celery settings.

        :param app: The Celery application instance. If not provided, it will be
                    retrieved automatically from `celery.current_app`.
        :param client: An optional, existing `MongoClient` instance. If not provided,
                       a new one will be created based on the app configuration.
        :return: An initialized ScheduleManager instance.
        """
        if app is None:
            app = celery_current_app._get_current_object()

        conf = app.conf
        # Use the same robust `or` logic as the main scheduler to ensure consistent config loading.
        mongo_uri = (conf.get('mongodb_scheduler_url') or
                     conf.get('CELERY_MONGODB_SCHEDULER_URL') or
                     'mongodb://localhost:27017/')

        db_name = (conf.get('mongodb_scheduler_db') or
                   conf.get('CELERY_MONGODB_SCHEDULER_DB') or
                   'celery')

        collection_name = (conf.get('mongodb_scheduler_collection') or
                           conf.get('CELERY_MONGODB_SCHEDULER_COLLECTION') or
                           'schedules')
        client_kwargs = conf.get('mongodb_scheduler_client_kwargs', {})

        if client is None:
            # Ensure appname is set, but allow user to override it.
            final_kwargs = {'appname': 'celery-mongobeat-helper', **client_kwargs}
            client = MongoClient(mongo_uri, **final_kwargs)

        db = client[db_name]
        collection = db[collection_name]

        # The user of the manager is responsible for the client's lifecycle (e.g., calling client.close()).
        return cls(collection)

    def create_interval_task(
            self, name: str, task: str, every: int, period: str = 'seconds',
            args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None,
            max_run_count: Optional[int] = None, description: Optional[str] = None, **custom_fields: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Creates or updates a task that runs on a fixed interval.

        :param name: The unique name for the task.
        :param task: The name of the Celery task to run (e.g., 'your_app.tasks.add').
        :param every: The frequency of the interval.
        :param period: The type of interval ('days', 'hours', 'minutes', 'seconds', 'microseconds').
        :param args: A list of positional arguments for the task.
        :param kwargs: A dictionary of keyword arguments for the task.
        :param max_run_count: The maximum number of times the task can run before being disabled.
        :param description: A human-readable description of the task.
        :param custom_fields: Any additional keyword arguments will be saved as fields in the task document.
        :return: The full document of the created or updated task, or None on failure.
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

        if description:
            schedule_doc['description'] = description

        # Merge any custom fields provided by the user
        schedule_doc.update(custom_fields)

        self.collection.update_one({'name': name}, {'$set': schedule_doc}, upsert=True)
        return self.collection.find_one({'name': name})

    def create_crontab_task(
            self, name: str, task: str, minute: str = '*', hour: str = '*',
            day_of_week: str = '*', day_of_month: str = '*', month_of_year: str = '*',
            args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None, description: Optional[str] = None,
            **custom_fields: Any
    ) -> Optional[Dict[str, Any]]:
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
        :param description: A human-readable description of the task.
        :param custom_fields: Any additional keyword arguments will be saved as fields in the task document.
        :return: The full document of the created or updated task, or None on failure.
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

        if description:
            schedule_doc['description'] = description

        # Merge any custom fields provided by the user
        schedule_doc.update(custom_fields)

        self.collection.update_one({'name': name}, {'$set': schedule_doc}, upsert=True)
        return self.collection.find_one({'name': name})

    def create_solar_task(
            self, name: str, task: str, event: str, lat: float, lon: float,
            args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None,
            description: Optional[str] = None, **custom_fields: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Creates or updates a task that runs on a solar event schedule (e.g., sunrise, sunset).

        :param name: The unique name for the task.
        :param task: The name of the Celery task to run.
        :param event: The solar event ('sunrise', 'sunset', 'dawn_astronomical', etc.).
        :param lat: The latitude for the location.
        :param lon: The longitude for the location.
        :param args: A list of positional arguments for the task.
        :param kwargs: A dictionary of keyword arguments for the task.
        :param description: A human-readable description of the task.
        :param custom_fields: Any additional keyword arguments will be saved as fields in the task document.
        :return: The full document of the created or updated task, or None on failure.
        """
        schedule_doc = {
            'name': name, 'task': task, 'enabled': True,
            'solar': {'event': event, 'lat': lat, 'lon': lon},
            'args': args or [], 'kwargs': kwargs or {},
        }

        if description:
            schedule_doc['description'] = description

        # Merge any custom fields provided by the user
        schedule_doc.update(custom_fields)

        self.collection.update_one({'name': name}, {'$set': schedule_doc}, upsert=True)
        return self.collection.find_one({'name': name})

    def update_task(self, name: Optional[str] = None, id: Optional[Union[str, ObjectId]] = None, **data: Any) -> Optional[Dict[str, Any]]:
        """
        Updates an existing task in the database.

        You can identify the task to update in one of three ways:
        1. By providing the `name` keyword argument.
        2. By providing the `id` keyword argument.
        3. By including either `_id` or `name` in the `data` dictionary.

        :param name: The unique name (str) of the task to update.
        :param id: The _id (str or ObjectId) of the task to update.
        :param data: A dictionary of fields to update on the task document.
        :return: The full, updated document, or None if the task was not found.
        """
        query = {}
        update_data = data.copy()

        if id:
            query['_id'] = ObjectId(id) if isinstance(id, str) else id
        elif name:
            query['name'] = name
        elif '_id' in update_data:
            query['_id'] = ObjectId(update_data.pop('_id'))
        elif 'id' in update_data:
            # Also handle 'id' as an alias for '_id'
            query['_id'] = ObjectId(update_data.pop('id'))
        elif 'name' in update_data:
            query['name'] = update_data['name']
        else:
            raise ValueError("No identifier found. Provide 'name', 'id', or include '_id', 'id', or 'name' in the data payload.")

        # Ensure immutable or identifying fields are not part of the $set payload
        update_data.pop('_id', None)
        update_data.pop('name', None)

        if not update_data:
            # If there's nothing to update, just return the existing task
            return self.collection.find_one(query)

        self.collection.update_one(query, {'$set': update_data})
        return self.collection.find_one(query)

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

    def get_task(self, name: Optional[str] = None, id: Optional[Union[str, ObjectId]] = None, serialize: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a task document from the database by its unique name or _id.

        Provide either the `name` or the `id` of the task.

        :param name: The unique name (str) of the task to retrieve.
        :param id: The _id (str or ObjectId) of the task to retrieve.
        :param serialize: If True, converts BSON types to JSON-friendly types.
        :return: A dictionary representing the task document, or None if not found.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("Provide either the 'name' or 'id' of the task, but not both.")

        query = {}
        if id:
            query['_id'] = ObjectId(id) if isinstance(id, str) else id
        else:
            query['name'] = name

        task = self.collection.find_one(query)
        if task and serialize:
            return self._sanitize_task(task)
        return task

    def delete_task(self, name: Optional[str] = None, id: Optional[Union[str, ObjectId]] = None):
        """
        Permanently deletes a task from the schedule by its unique name or _id.

        Provide either the `name` or the `id` of the task.

        :param name: The unique name (str) of the task to delete.
        :param id: The _id (str or ObjectId) of the task to delete.
        """
        if (name is None and id is None) or (name is not None and id is not None):
            raise ValueError("Provide either the 'name' or 'id' of the task, but not both.")

        query = {}
        if id:
            query['_id'] = ObjectId(id) if isinstance(id, str) else id
        else:
            query['name'] = name

        self.collection.delete_one(query)

    def get_tasks(self, serialize: bool = False, **filters: Any) -> List[Dict[str, Any]]:
        """
        Retrieves a list of task documents from the database, with optional filtering.

        This method allows for flexible querying using keyword arguments.
        - For simple filters: `get_tasks(enabled=True)`
        - For schedule type: `get_tasks(schedule_type='interval')`
        - For nested fields (like in kwargs): `get_tasks(kwargs__customer_id=123)`

        :param serialize: If True, converts BSON types to JSON-friendly types for each document.
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

        tasks = list(self.collection.find(query))
        if serialize:
            return [self._sanitize_task(task) for task in tasks]
        return tasks

    def count_tasks(self, **filters: Any) -> int:  # count_tasks does not need a serialize flag
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