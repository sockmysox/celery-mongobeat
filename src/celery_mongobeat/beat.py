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

from bson import ObjectId
from celery import current_app
from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import crontab, schedule, solar
from celery.utils.log import get_logger
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

logger = get_logger(__name__)


class MongoScheduleEntry(ScheduleEntry):
    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=None,
                 kwargs=None, options=None, app=None, **ext):

        # Extract specific fields for the Mongo entry
        self.enabled = ext.get('enabled', True)
        self.max_run_count = ext.get('max_run_count')
        self.start_after = ext.get('start_after')
        self.run_immediately = ext.get('run_immediately', False)
        self.description = ext.get('description')
        self.display_name = ext.get('display_name')
        self._id = ext.get('_id')

        self.scheduler = ext.get('scheduler')

        super().__init__(name=name, task=task, last_run_at=last_run_at,
                         total_run_count=total_run_count, schedule=schedule,
                         args=args, kwargs=kwargs, options=options, app=app)

    def is_due(self):
        if not self.enabled:
            return False, 5.0  # Retry later

        if self.max_run_count is not None and self.total_run_count >= self.max_run_count:
            return False, 5.0

        if self.start_after and self.app.now() < self.start_after:
            return False, 5.0

        if self.run_immediately:
            return True, 0

        return self.schedule.is_due(self.last_run_at)

    def __next__(self):
        cls = type(self)
        return cls(
            name=self.name,
            task=self.task,
            last_run_at=self.app.now(),
            total_run_count=self.total_run_count + 1,
            schedule=self.schedule,
            args=self.args,
            kwargs=self.kwargs,
            options=self.options,
            app=self.app,
            enabled=self.enabled,
            max_run_count=self.max_run_count,
            start_after=self.start_after,
            run_immediately=False,
            description=self.description,
            display_name=self.display_name,
            _id=self._id,
            scheduler=self.scheduler
        )

    def __repr__(self):
        return '<MongoScheduleEntry: {0.name} {0.schedule} {0.last_run_at}>'.format(self)

    def save(self):
        if self.scheduler:
            # Tell the scheduler to write THIS specific entry to the DB
            self.scheduler.save_entry(self)


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
        logger.info("Initializing AppScheduleManager (Custom v2)...")
        self._client = None
        self._collection: Optional[Collection] = None
        self._schedule = {}
        self._last_saved_state = {}
        super().__init__(*args, **kwargs)
        self.max_interval = self.app.conf.beat_max_loop_interval or 300
        self.reload_schedule()

    @classmethod
    def from_celery_app(cls, app=None):
        if app is None:
            app = current_app
        return cls(app=app)

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

    @property
    def schedule(self):
        # Return the internal variable. Logic is moved to tick().
        return self._schedule

    def _schedule_from_document(self, doc: Dict[str, Any]):
        if 'interval' in doc and doc['interval']:
            return schedule(run_every=datetime.timedelta(**{
                doc['interval']['period']: doc['interval']['every']
            }))
        if 'crontab' in doc and doc['crontab']:
            return crontab(**doc['crontab'])
        if 'solar' in doc and doc['solar']:
            return solar(**doc['solar'])
        raise ValueError(f"Task {doc.get('name')} has no valid schedule.")

    def _entry_from_document(self, doc: Dict[str, Any]) -> MongoScheduleEntry:
        """Ensures all DB tasks use our custom Entry class."""
        return MongoScheduleEntry(
            name=doc['name'],
            task=doc['task'],
            schedule=self._schedule_from_document(doc),
            args=doc.get('args', []),
            kwargs=doc.get('kwargs', {}),
            options=doc.get('options', {}),
            last_run_at=doc.get('last_run_at'),
            total_run_count=doc.get('total_run_count', 0),
            enabled=doc.get('enabled', True),
            max_run_count=doc.get('max_run_count'),
            start_after=doc.get('start_after'),
            run_immediately=doc.get('run_immediately', False),
            description=doc.get('description'),
            display_name=doc.get('display_name'),
            _id=doc.get('_id'),
            app=self.app,
            scheduler=self  # Crucial: allows entry.save() to work
        )

    def reserve(self, entry):
        """
        Update the schedule with the next execution time for the task.
        This is called after the task has been sent to the broker.
        """
        new_entry = super().reserve(entry)
        # Explicitly log what we got back from super().reserve()
        if isinstance(new_entry, MongoScheduleEntry):
            logger.info(f"Reserving task {new_entry.name}. Updating state in DB...")
            new_entry.save()
        else:
            logger.warning(f"Reserved entry {new_entry.name} is NOT a MongoScheduleEntry. It is {type(new_entry)}. DB update skipped.")
        return new_entry

    def save_entry(self, entry):
        """The actual DB write operation."""
        if self._collection is not None:
            # Check if state has changed to prevent duplicate DB writes
            current_state = (entry.last_run_at, entry.total_run_count, entry.run_immediately)
            if self._last_saved_state.get(entry.name) == current_state:
                logger.debug(f"Skipping save for {entry.name}, state unchanged.")
                return

            logger.info(f"Writing entry {entry.name} to MongoDB. last_run_at={entry.last_run_at}, count={entry.total_run_count}")
            
            update_fields = {
                'last_run_at': entry.last_run_at,
                'total_run_count': entry.total_run_count,
                'run_immediately': entry.run_immediately
            }
            if entry.max_run_count and entry.total_run_count >= entry.max_run_count:
                update_fields['enabled'] = False

            self._collection.update_one(
                {'name': entry.name},
                {'$set': update_fields}
            )
            self._last_saved_state[entry.name] = current_state

    def tick(self, *args, **kwargs):
        """The heart of the scheduler loop."""
        if time.monotonic() - self._last_db_load > self.max_interval:
            # Sync any unsaved memory changes before we reload
            self.sync()
            self.reload_schedule()

        return super().tick(*args, **kwargs)

    def reload_schedule(self):
        """Smart reload that preserves in-memory progress."""
        self._last_db_load = time.monotonic()
        logger.debug("Reloading schedule from MongoDB...")

        if self._collection is None: return

        # Load dynamic tasks from DB
        db_tasks = {doc['name']: doc for doc in self._collection.find({'enabled': True})}

        # Start with static tasks from settings.py
        new_schedule = self.app.conf.beat_schedule.copy()

        # Reset tracking to match what we are loading
        self._last_saved_state = {}

        for name, doc in db_tasks.items():
            try:
                if self.replace_dots:
                    doc = self._decode_keys(doc)
                entry = self._entry_from_document(doc)

                # If the task is already in memory, preserve its run-state
                # so we don't 'reset' a task that just ran but hasn't synced yet.
                if name in self._schedule:
                    entry.last_run_at = self._schedule[name].last_run_at
                    entry.total_run_count = self._schedule[name].total_run_count

                # Track the state (after merge) to ensure precision matches memory
                self._last_saved_state[name] = (
                    entry.last_run_at,
                    entry.total_run_count,
                    entry.run_immediately
                )

                new_schedule[name] = entry
            except Exception as exc:
                logger.error(f"Failed to load schedule entry '{name}': {exc}")

        self._schedule = new_schedule
        self._heap = None  # Force Celery to re-calculate next run times

    def sync(self):
        """Periodic cleanup sync."""
        logger.debug("Syncing schedule to MongoDB...")
        for entry in self._schedule.values():
            if isinstance(entry, MongoScheduleEntry):
                entry.save()


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

    def get_tasks(self, serialize=False):
        if self._collection is None: return []
        tasks = list(self._collection.find())
        if serialize:
            return [self._sanitize_task(t) for t in tasks]
        return tasks

    def get_task(self, id, serialize=False):
        if self._collection is None: return None
        try:
            oid = ObjectId(id)
        except:
            return None
        task = self._collection.find_one({'_id': oid})
        if serialize and task:
            return self._sanitize_task(task)
        return task

    def create_interval_task(self, **data):
        if self._collection is None: raise Exception("No DB")
        self._collection.insert_one(data)
        return self._sanitize_task(data)

    def create_crontab_task(self, **data):
        if self._collection is None: raise Exception("No DB")
        self._collection.insert_one(data)
        return self._sanitize_task(data)

    def update_task(self, id, **data):
        if self._collection is None: raise Exception("No DB")

        update_payload = {'$set': data}
        unset_payload = {}

        # Ensure mutually exclusive schedule types are cleaned up
        if 'interval' in data:
            unset_payload['crontab'] = 1
        elif 'crontab' in data:
            unset_payload['interval'] = 1

        if unset_payload:
            update_payload['$unset'] = unset_payload

        self._collection.update_one({'_id': ObjectId(id)}, update_payload)

    def delete_task(self, id):
        if self._collection is None: raise Exception("No DB")
        self._collection.delete_one({'_id': ObjectId(id)})

    def count_tasks(self, enabled=None):
        if self._collection is None: return 0
        query = {}
        if enabled is not None:
            query['enabled'] = enabled
        return self._collection.count_documents(query)

    def _sanitize_task(self, task):
        if not task: return {}
        sanitized = {}
        for k, v in task.items():
            if isinstance(v, ObjectId):
                sanitized[k] = str(v)
            elif isinstance(v, datetime.datetime):
                sanitized[k] = v.isoformat()
            elif isinstance(v, dict):
                sanitized[k] = self._sanitize_task(v)
            elif isinstance(v, list):
                sanitized[k] = [self._sanitize_task(i) if isinstance(i, dict) else i for i in v]
            else:
                sanitized[k] = v
        return sanitized
