#!/usr/bin/env python
"""
Custom MongoDB Scheduler for Celery Beat.

This scheduler stores and retrieves task schedules from a MongoDB collection,
allowing for dynamic management of periodic tasks without restarting the
Celery Beat service. This implementation aims for feature parity with the
deprecated `celerybeat-mongo` library.
"""
import datetime
import time
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Union

from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab, schedule, solar
from celery.utils.log import get_logger
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

logger = get_logger(__name__)


class MongoScheduler(Scheduler):
    """
    A Celery Beat scheduler that uses MongoDB to store the schedule.

    This scheduler is designed as a drop-in replacement for the original
    `celerybeat-mongo` scheduler, providing compatibility with its database
    schema and configuration settings.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the scheduler, setting up the database connection.
        """
        logger.info("Initializing MongoScheduler...")
        self._client = None
        self._collection: Optional[Collection] = None
        # self._schedule is initialized by the parent `Scheduler` class, but we
        # declare it here to make it explicit for this subclass and to
        # satisfy static analysis tools.
        self._schedule = {}
        # Stores the last run time of tasks as loaded from the database.
        self._last_run_times: Dict[str, Optional[datetime.datetime]] = {}
        # Stores the max run count for tasks that have one.
        self._max_run_counts: Dict[str, Optional[int]] = {}
        # Stores the total run count of tasks as loaded from the database.
        self._total_run_counts: Dict[str, int] = {}
        super().__init__(*args, **kwargs)
        self._last_db_load = time.monotonic()
        self.max_interval = self.app.conf.beat_max_loop_interval or 300

    def setup_schedule(self):
        """
        Connects to MongoDB using settings from the Celery app configuration.
        This is called automatically by the parent class `__init__`.
        """
        # Prioritize modern (lowercase) settings, falling back to legacy uppercase settings.
        # The `or` operator correctly handles cases where a modern setting exists but is `None`.
        mongo_uri = (self.app.conf.get('mongodb_scheduler_url') or
                     self.app.conf.get('CELERY_MONGODB_SCHEDULER_URL') or
                     'mongodb://localhost:27017/')

        db_name = (self.app.conf.get('mongodb_scheduler_db') or
                   self.app.conf.get('CELERY_MONGODB_SCHEDULER_DB') or
                   'celery')

        collection_name = (self.app.conf.get('mongodb_scheduler_collection') or
                           self.app.conf.get('CELERY_MONGODB_SCHEDULER_COLLECTION') or
                           'schedules')

        self.replace_dots = self.app.conf.get('mongodb_scheduler_replace_dots', False)
        client_kwargs = self.app.conf.get('mongodb_scheduler_client_kwargs', {})

        # Redact the password from the URI before logging to prevent credential exposure.
        parsed_uri = urlparse(mongo_uri)
        if parsed_uri.password:
            safe_uri = parsed_uri._replace(netloc=f"{parsed_uri.username}:****@{parsed_uri.hostname}:{parsed_uri.port}").geturl()
        else:
            safe_uri = mongo_uri

        logger.info(f"Connecting to MongoDB for scheduler: {safe_uri}")
        logger.info(f"Using database: '{db_name}', collection: '{collection_name}'")
        try:
            # Ensure appname is set, but allow user to override it.
            final_kwargs = {'appname': 'celery-mongobeat', **client_kwargs}
            self._client = MongoClient(mongo_uri, **final_kwargs)
            db = self._client[db_name]
            self._collection = db[collection_name]
            # Ensure an index on the 'name' field for efficient lookups
            self._collection.create_index('name', unique=True)
        except ConnectionFailure as e:
            logger.error(f"Could not connect to MongoDB: {e}")
            self._collection = None

    def _entry_from_document(self, doc: Dict[str, Any]) -> ScheduleEntry:
        """Converts a MongoDB document into a Celery ScheduleEntry."""
        # For DocumentDB compatibility, we might need to decode field names
        # if the user has stored them with a replacement character.
        if self.replace_dots:
            doc = self._decode_keys(doc)

        schedule = self._schedule_from_document(doc)

        # Construct the entry with only the arguments that ScheduleEntry expects.
        # This prevents TypeErrors if the document contains extra fields.
        entry_kwargs = {
            'name': doc['name'],
            'task': doc['task'],
            'schedule': schedule,
            'args': doc.get('args', []),
            'kwargs': doc.get('kwargs', {}),
            'options': doc.get('options', {}),
            'last_run_at': doc.get('last_run_at', self.app.now()),
            'total_run_count': doc.get('total_run_count', 0),
            'app': self.app
        }

        return ScheduleEntry(**entry_kwargs)

    def _schedule_from_document(self, doc: Dict[str, Any]) -> Union[schedule, crontab, solar]:
        """Creates a Celery schedule object from a MongoDB document."""
        if 'interval' in doc:
            schedule_data = doc['interval']
            # Create a timedelta object from the interval data.
            # The period can be one of: days, hours, minutes, seconds, microseconds.
            delta = datetime.timedelta(
                **{schedule_data.get('period', 'seconds'): schedule_data['every']}
            )
            # Create a schedule object with the timedelta.
            return schedule(run_every=delta, app=self.app)
        elif 'crontab' in doc:
            schedule_data = doc['crontab']
            return crontab(
                minute=schedule_data.get('minute', '*'),
                hour=schedule_data.get('hour', '*'),
                day_of_week=schedule_data.get('day_of_week', '*'),
                day_of_month=schedule_data.get('day_of_month', '*'),
                month_of_year=schedule_data.get('month_of_year', '*'),
                app=self.app
            )
        elif 'solar' in doc:
            schedule_data = doc['solar']
            # Celery's solar schedule takes an event and lat/lon coordinates.
            return solar(
                event=schedule_data['event'],
                lat=schedule_data['lat'],
                lon=schedule_data['lon'],
                app=self.app
            )
        else:
            # This case should ideally not be hit if documents are validated on insert.
            raise ValueError(f"Task '{doc.get('name')}' is missing a valid schedule type (interval, crontab, or solar).")

    @property
    def schedule(self) -> Dict[str, ScheduleEntry]:
        """
        The schedule dictionary, called by Celery Beat on each tick.

        This method reads all enabled tasks from MongoDB, converts them to
        ScheduleEntry objects, merges them with any static tasks from the
        app configuration, and returns the final schedule.
        """
        # Update with any static schedule entries from the app config.
        if time.monotonic() - self._last_db_load > self.max_interval:
            logger.debug(f"Reloading schedule from database (last load was at {self._last_db_load}).")
            self._last_db_load = time.monotonic()
        else:
            return self._schedule
        self.install_default_entries(self.app.conf.beat_schedule)

        # On a reload, we start with a fresh schedule containing only the static entries.
        # This ensures that tasks deleted from the DB are removed from the scheduler.
        new_schedule = self.app.conf.beat_schedule.copy()

        if self._collection is None:
            logger.warning("MongoDB collection not available. Using static schedule only.")
            self._schedule = self.app.conf.beat_schedule
            return self._schedule

        logger.debug("Reading schedule from MongoDB...")
        # Clear previous in-memory state before reloading
        self._last_run_times.clear()
        self._max_run_counts.clear()
        self._total_run_counts.clear()
        for doc in self._collection.find({'enabled': True}):
            try:
                # Store the original last_run_at time for later comparison in sync()
                self._last_run_times[doc['name']] = doc.get('last_run_at')
                self._max_run_counts[doc['name']] = doc.get('max_run_count')
                self._total_run_counts[doc['name']] = doc.get('total_run_count', 0)
                entry = self._entry_from_document(doc)
                # Entries from the DB will overwrite static ones with the same name.
                new_schedule[entry.name] = entry
            except Exception as e:
                logger.error(
                    f"Failed to load schedule entry '{doc.get('name', 'N/A')}': {e}",
                    exc_info=True
                )

        # Replace the old schedule with the newly built one.
        self._schedule = new_schedule
        return self._schedule

    def sync(self):
        """
        This method is called by Celery Beat after each tick to persist state.

        It iterates through the schedule and saves the `last_run_at` and
        `total_run_count` for any task that has been run.
        """
        if self._collection is None:
            return

        logger.debug('Syncing schedule to MongoDB...')
        for entry in self.schedule.values():
            # Get the last run time as it was when we loaded it from the DB.
            last_run_from_db = self._last_run_times.get(entry.name, None)
            total_runs_from_db = self._total_run_counts.get(entry.name, 0)

            # A task needs to be synced if its last run time or run count has changed.
            has_run = entry.last_run_at != last_run_from_db
            count_changed = entry.total_run_count != total_runs_from_db
            if has_run or count_changed:
                logger.debug(f"Preparing to update task state for '{entry.name}' in database.")
                update_fields = {
                    'last_run_at': entry.last_run_at,
                    'total_run_count': entry.total_run_count
                }

                # If max_run_count is set and reached, disable the task.
                max_runs = self._max_run_counts.get(entry.name)
                if max_runs is not None and entry.total_run_count >= max_runs:
                    logger.info(
                        f"Task '{entry.name}' has reached its max_run_count of {max_runs}. "
                        f"Disabling task."
                    )
                    update_fields['enabled'] = False

                result = self._collection.update_one({'name': entry.name}, {'$set': update_fields})
                if result.modified_count:
                    # Update our in-memory copy of the last run time to prevent re-syncing.
                    self._last_run_times[entry.name] = entry.last_run_at
                    self._total_run_counts[entry.name] = entry.total_run_count
                    logger.info(f"Synced task '{entry.name}' to MongoDB.")


    def close(self):
        """Closes the database connection when the scheduler shuts down."""
        logger.info("Closing MongoScheduler MongoDB connection.")
        if self._client:
            self._client.close()

    def _encode_keys(self, data: Dict) -> Dict:
        """Recursively replace '.' with a replacement character in dictionary keys."""
        if not isinstance(data, dict):
            return data
        return {key.replace('.', '__dot__'): self._encode_keys(value) for key, value in data.items()}

    def _decode_keys(self, data: Dict) -> Dict:
        """Recursively replace the replacement character with '.' in dictionary keys."""
        if not isinstance(data, dict):
            return data
        return {key.replace('__dot__', '.'): self._decode_keys(value) for key, value in data.items()}
