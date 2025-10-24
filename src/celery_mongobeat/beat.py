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
from typing import Any, Dict, List, Optional, Union

from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab, schedule, solar
from celery.utils.log import get_logger
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

logger = get_logger(__name__)


class CustomMongoScheduler(Scheduler):
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
        logger.info("Initializing CustomMongoScheduler...")
        self._client = None
        self._collection: Optional[Collection] = None
        # self._schedule is initialized by the parent `Scheduler` class, but we
        # declare it here to make it explicit for this subclass and to
        # satisfy static analysis tools.
        self._schedule = {}
        super().__init__(*args, **kwargs)
        self._last_db_load = time.monotonic()
        self.max_interval = self.app.conf.beat_max_loop_interval or 300

    def setup_schedule(self):
        """
        Connects to MongoDB using settings from the Celery app configuration.
        This is called automatically by the parent class `__init__`.
        """
        # Modern configuration keys
        mongo_uri = self.app.conf.get('mongodb_scheduler_url', 'mongodb://localhost:27017/')
        db_name = self.app.conf.get('mongodb_scheduler_db', 'celery')
        collection_name = self.app.conf.get('mongodb_scheduler_collection', 'schedules')
        self.replace_dots = self.app.conf.get('mongodb_scheduler_replace_dots', False)
        client_kwargs = self.app.conf.get('mongodb_scheduler_client_kwargs', {})

        # Backwards compatibility with celerybeat-mongo's `mongodb_backend_settings`
        if 'mongodb_backend_settings' in self.app.conf:
            logger.warning(
                "Using deprecated 'mongodb_backend_settings'. Please update your "
                "configuration to use 'mongodb_scheduler_url', 'mongodb_scheduler_db', "
                "and 'mongodb_scheduler_collection' for better clarity."
            )
            settings = self.app.conf['mongodb_backend_settings']
            # The old library used 'host' for the URI.
            mongo_uri = settings.get('uri', settings.get('host', mongo_uri))
            db_name = settings.get('database', db_name)
            collection_name = settings.get('collection', collection_name)
            # For backwards compatibility, pass any other settings as kwargs,
            # allowing for things like SSL configuration.
            legacy_kwargs = {k: v for k, v in settings.items() if k not in ['uri', 'host', 'database', 'collection']}
            client_kwargs.update(legacy_kwargs)

        logger.info(f"Connecting to MongoDB for scheduler: {mongo_uri}/{db_name}")
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
        entry = ScheduleEntry(
            name=doc['name'],
            task=doc['task'],
            schedule=schedule,
            args=doc.get('args', []),  # type: ignore
            kwargs=doc.get('kwargs', {}),  # type: ignore
            options=doc.get('options', {}),  # type: ignore
            last_run_at=doc.get('last_run_at', self._default_now()),
            total_run_count=doc.get('total_run_count', 0),
            app=self.app
        )
        # Store the original last_run_at from the database on the entry.
        # This is used by sync() to detect if the task has run.
        entry._last_run_at = doc.get('last_run_at')
        return entry

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

        if not self._collection:
            logger.warning("MongoDB collection not available. Using static schedule only.")
            return self._schedule

        logger.debug("Reading schedule from MongoDB...")
        db_schedule: Dict[str, ScheduleEntry] = {}
        for doc in self._collection.find({'enabled': True}):
            try:
                entry = self._entry_from_document(doc)
                db_schedule[entry.name] = entry
            except Exception as e:
                logger.error(
                    f"Failed to load schedule entry '{doc.get('name', 'N/A')}': {e}",
                    exc_info=True
                )

        # Merge the dynamic schedule from the DB into the static one.
        # Entries from the DB will overwrite static ones with the same name.
        self._schedule.update(db_schedule)
        return self._schedule

    def sync(self):
        """
        This method is called by Celery Beat after each tick to persist state.

        It iterates through the schedule and saves the `last_run_at` and
        `total_run_count` for any task that has been run.
        """
        if not self._collection:
            return

        logger.debug('Syncing schedule to MongoDB...')
        updates = []
        for entry in self.schedule.values():
            # We only sync entries that were loaded from the database, marked by `_last_run_at`.
            if hasattr(entry, '_last_run_at') and entry.last_run_at != entry._last_run_at:
                logger.debug(f"Preparing to update task state for '{entry.name}' in database.")
                updates.append(UpdateOne(
                    {'name': entry.name},
                    {'$set': {
                        'last_run_at': entry.last_run_at,
                        'total_run_count': entry.total_run_count
                    }}
                ))
                # Update our in-memory copy to prevent re-syncing if the task doesn't run again.
                entry._last_run_at = entry.last_run_at

        if updates:
            logger.info(f"Syncing {len(updates)} updated task(s) to MongoDB.")
            self._collection.bulk_write(updates, ordered=False)

    def close(self):
        """Closes the database connection when the scheduler shuts down."""
        logger.info("Closing CustomMongoScheduler MongoDB connection.")
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
