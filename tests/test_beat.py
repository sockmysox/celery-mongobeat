import datetime
import time
from unittest.mock import MagicMock, patch

import mongomock
import pytest
from celery.schedules import crontab, schedule, solar

from celery_mongobeat.beat import CustomMongoScheduler


@pytest.fixture
def mock_celery_app():
    """Creates a mock Celery app with a default configuration."""
    app = MagicMock()
    app.conf.beat_schedule = {}
    app.conf.beat_max_loop_interval = 10
    # Mock the now() method to return a fixed time
    app.now = datetime.datetime.utcnow
    return app


@pytest.fixture
def mock_mongo_client():
    """Provides a mock MongoDB client."""
    return mongomock.MongoClient()


@pytest.fixture
def scheduler(mock_celery_app, mock_mongo_client):
    """
    Provides a CustomMongoScheduler instance initialized with mock components.
    """
    with patch('celery_mongobeat.beat.MongoClient', return_value=mock_mongo_client):
        # Set default config on the mock app
        mock_celery_app.conf.mongodb_scheduler_db = 'celery_test'
        mock_celery_app.conf.mongodb_scheduler_collection = 'schedules'
        mock_celery_app.conf.mongodb_scheduler_url = 'mongodb://localhost'
        mock_celery_app.conf.mongodb_scheduler_replace_dots = False
        mock_celery_app.conf.mongodb_scheduler_client_kwargs = {}

        # Initialize the scheduler, which will use the patched MongoClient
        sched = CustomMongoScheduler(app=mock_celery_app)
        yield sched
        # Teardown: close the client
        sched.close()


def test_scheduler_initialization(scheduler, mock_mongo_client):
    """Test that the scheduler initializes and connects to the mock DB."""
    assert scheduler._client is not None
    assert scheduler._collection is not None
    assert scheduler._collection.name == 'schedules'
    assert scheduler._collection.database.name == 'celery_test'


def test_legacy_config_support(mock_celery_app, mock_mongo_client):
    """Test backwards compatibility with 'mongodb_backend_settings'."""
    mock_celery_app.conf.mongodb_backend_settings = {
        'host': 'mongodb://legacy-host',
        'database': 'legacy_db',
        'collection': 'legacy_collection',
        'ssl': True  # Example of a custom kwarg
    }

    with patch('celery_mongobeat.beat.MongoClient') as mock_client_constructor:
        sched = CustomMongoScheduler(app=mock_celery_app)

        # Assert that MongoClient was called with the legacy settings
        mock_client_constructor.assert_called_with(
            'mongodb://legacy-host',
            appname='celery-mongobeat',
            ssl=True
        )
        assert sched._collection.database.name == 'legacy_db'
        assert sched._collection.name == 'legacy_collection'


def test_load_schedule_from_db(scheduler):
    """Test that different schedule types are loaded correctly from MongoDB."""
    collection = scheduler._collection
    collection.insert_many([
        {
            'name': 'interval_task',
            'task': 'tasks.interval',
            'enabled': True,
            'interval': {'every': 10, 'period': 'seconds'}
        },
        {
            'name': 'crontab_task',
            'task': 'tasks.crontab',
            'enabled': True,
            'crontab': {'minute': '0', 'hour': '5'}
        },
        {
            'name': 'solar_task',
            'task': 'tasks.solar',
            'enabled': True,
            'solar': {'event': 'sunrise', 'lat': 40.7, 'lon': -74.0}
        },
        {
            'name': 'disabled_task',
            'task': 'tasks.disabled',
            'enabled': False,
            'interval': {'every': 5, 'period': 'minutes'}
        }
    ])

    # Access the .schedule property to trigger loading from DB
    s = scheduler.schedule
    assert 'interval_task' in s
    assert 'crontab_task' in s
    assert 'solar_task' in s
    assert 'disabled_task' not in s

    # Verify schedule types
    assert isinstance(s['interval_task'].schedule, schedule)
    assert s['interval_task'].schedule.run_every.total_seconds() == 10
    assert isinstance(s['crontab_task'].schedule, crontab)
    assert s['crontab_task'].schedule._orig_hour == '5'
    assert isinstance(s['solar_task'].schedule, solar)
    assert s['solar_task'].schedule.event == 'sunrise'


def test_load_invalid_schedule_entry(scheduler, caplog):
    """Test that an entry with no valid schedule type is skipped and logged."""
    collection = scheduler._collection
    collection.insert_one({
        'name': 'invalid_task',
        'task': 'tasks.invalid',
        'enabled': True
        # Missing interval, crontab, or solar key
    })

    s = scheduler.schedule
    assert 'invalid_task' not in s
    assert "Failed to load schedule entry 'invalid_task'" in caplog.text
    assert "missing a valid schedule type" in caplog.text


def test_sync_updates_run_tasks(scheduler):
    """Test that sync() persists last_run_at and total_run_count."""
    collection = scheduler._collection
    collection.insert_one({
        'name': 'task_to_run',
        'task': 'tasks.test',
        'enabled': True,
        'interval': {'every': 1, 'period': 'seconds'},
        'last_run_at': None,
        'total_run_count': 0
    })

    # Load the schedule
    s = scheduler.schedule
    entry = s['task_to_run']

    # Simulate a task run by updating its state
    now = datetime.datetime.utcnow()
    entry.last_run_at = now
    entry.total_run_count += 1

    # Perform sync
    scheduler.sync()

    # Verify the document in the database was updated
    updated_doc = collection.find_one({'name': 'task_to_run'})
    assert updated_doc is not None
    assert updated_doc['total_run_count'] == 1
    # Compare timestamps with a small tolerance
    assert abs((updated_doc['last_run_at'] - now).total_seconds()) < 1


def test_schedule_caching(scheduler):
    """Test that the schedule is cached and not reloaded on every tick."""
    # Patch the collection's find method to track calls
    with patch.object(scheduler._collection, 'find', wraps=scheduler._collection.find) as mock_find:
        # First access, should call find()
        s1 = scheduler.schedule
        mock_find.assert_called_once()

        # Second access within max_interval, should NOT call find() again
        s2 = scheduler.schedule
        mock_find.assert_called_once()  # Still 1 call

        # Force the cache to expire
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        # Third access, should call find() again
        s3 = scheduler.schedule
        assert mock_find.call_count == 2


def test_decode_keys_with_replace_dots(scheduler):
    """Test DocumentDB compatibility feature for decoding keys."""
    scheduler.replace_dots = True  # Enable the feature
    collection = scheduler._collection
    collection.insert_one({
        'name': 'dotted_key_task',
        'task': 'tasks.dotted',
        'enabled': True,
        'interval': {'every': 1, 'period': 'seconds'},
        'kwargs': {'config__dot__key': 'value'}
    })

    s = scheduler.schedule
    entry = s['dotted_key_task']

    # Assert that the key was correctly decoded
    assert 'config.key' in entry.kwargs
    assert entry.kwargs['config.key'] == 'value'