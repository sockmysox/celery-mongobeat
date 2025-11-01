"""
Tests for the MongoScheduler and ScheduleManager.
"""
import datetime
import time
import json
from unittest.mock import MagicMock

import pytest
from celery.schedules import crontab, schedule, solar
from bson import ObjectId
from mongomock import MongoClient

# from celery_mongobeat.beat import MongoScheduler
# from celery_mongobeat.helpers import ScheduleManager
from src.celery_mongobeat.beat import MongoScheduler
from src.celery_mongobeat.helpers import ScheduleManager

@pytest.fixture
def scheduler(celery_app, mock_mongo_client, monkeypatch):
    """
    Provides an instance of the MongoScheduler, patched to use the mock client.
    """
    # Patch MongoClient so the scheduler uses our mock client instead of creating a real one.
    # monkeypatch.setattr('celery_mongobeat.beat.MongoClient', lambda *args, **kwargs: mock_mongo_client)
    # The path must match how the module is imported in the test environment.
    monkeypatch.setattr('src.celery_mongobeat.beat.MongoClient', lambda *args, **kwargs: mock_mongo_client)
    return MongoScheduler(app=celery_app)


@pytest.fixture
def manager(mock_collection):
    """
    Provides an instance of the ScheduleManager using the mock collection.
    """
    return ScheduleManager(collection=mock_collection)


class TestMongoScheduler:
    """
    Tests for the core MongoScheduler functionality.
    """

    def test_setup_schedule_creates_index(self, scheduler, mock_collection):
        """
        Verify that the scheduler creates the required unique index on 'name'.
        """
        # The index is created during scheduler initialization via setup_schedule()
        index_info = mock_collection.index_information()
        assert 'name_1' in index_info
        assert index_info['name_1']['unique'] is True

    def test_schedule_loading_all_types(self, scheduler, manager, mock_collection, monkeypatch):
        """
        Test that the scheduler correctly loads enabled tasks from the database.
        """
        # Use the manager to create some test tasks
        manager.create_interval_task('interval-task', 'tasks.test', every=10, period='seconds')
        manager.create_crontab_task('crontab-task', 'tasks.test', minute='*/1')
        manager.create_solar_task('solar-task', 'tasks.test', event='sunrise', lat=40.7, lon=-74.0)
        manager.create_interval_task('disabled-task', 'tasks.test', every=30, period='seconds')
        manager.disable_task('disabled-task')

        assert mock_collection.count_documents({}) == 4

        # Force a reload from the database to bypass the cache
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        # Prevent default entries from being installed to isolate DB loading logic
        monkeypatch.setattr(scheduler, 'install_default_entries', lambda _: None)

        # The `schedule` property should read from the DB
        s = scheduler.schedule
        assert len(s) == 3  # Should only load the three enabled tasks
        assert 'interval-task' in s
        assert 'crontab-task' in s
        assert 'solar-task' in s
        assert 'disabled-task' not in s

        # Verify schedule types
        assert isinstance(s['interval-task'].schedule, schedule)
        assert s['interval-task'].schedule.run_every.total_seconds() == 10
        assert isinstance(s['crontab-task'].schedule, crontab)
        assert isinstance(s['solar-task'].schedule, solar)
        assert s['solar-task'].schedule.event == 'sunrise'

    def test_sync_updates_last_run_at(self, scheduler, manager):
        """
        Test that sync() persists the last_run_at and total_run_count fields.
        """
        manager.create_interval_task('sync-test-task', 'tasks.test', every=1, period='seconds')

        # Force a reload from the database to bypass the cache
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        # Simulate the beat running the task
        s = scheduler.schedule
        entry = s['sync-test-task']
        entry.last_run_at = scheduler.app.now()
        entry.total_run_count += 1

        # Sync the state back to the database
        scheduler.sync()

        # Verify the document in the DB was updated
        task_doc = manager.get_task('sync-test-task')
        assert task_doc is not None
        # mongomock may return a naive datetime, so we make it aware before comparing.
        db_last_run_at = task_doc['last_run_at'].replace(tzinfo=datetime.timezone.utc)
        # Assert that the timestamps are within a small tolerance to account for DB precision loss.
        assert abs(db_last_run_at - entry.last_run_at).total_seconds() < 1
        assert task_doc['total_run_count'] == 1

    def test_sync_disables_task_on_max_run_count(self, scheduler, manager):
        """
        Test that sync() disables a task when it reaches its max_run_count.
        """
        manager.create_interval_task(
            'limited-run-task', 'tasks.test', every=1, period='seconds', max_run_count=3
        )

        # Force a reload from the database to bypass the cache
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        # Simulate the beat running the task 3 times
        s = scheduler.schedule
        entry = s['limited-run-task']
        entry.total_run_count = 3
        entry.last_run_at = scheduler.app.now()

        # Sync the state
        scheduler.sync()

        # Verify the task is now disabled in the database
        task_doc = manager.get_task('limited-run-task')
        assert task_doc is not None
        assert task_doc['enabled'] is False
        assert task_doc['total_run_count'] == 3

    def test_load_invalid_schedule_entry(self, scheduler, manager, caplog):
        """Test that an entry with no valid schedule type is skipped and logged."""
        manager.collection.insert_one({
            'name': 'invalid_task',
            'task': 'tasks.invalid',
            'enabled': True
            # Missing interval, crontab, or solar key
        })

        # Force a reload from the database to bypass the cache
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        s = scheduler.schedule
        assert 'invalid_task' not in s
        assert "Failed to load schedule entry 'invalid_task'" in caplog.text
        assert "missing a valid schedule type" in caplog.text

    def test_schedule_caching(self, scheduler, monkeypatch):
        """Test that the schedule is not reloaded from the DB on every tick."""
        # Patch the collection's find method to track calls
        # Use a mock with a side_effect to wrap the original method
        mock_find = MagicMock(side_effect=scheduler._collection.find)
        monkeypatch.setattr(scheduler._collection, 'find', mock_find)

        # Force the cache to be considered stale for the first access in this test
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        # First access, should call find()
        _ = scheduler.schedule
        mock_find.assert_called_once()

        # Second access within max_interval, should NOT call find() again
        _ = scheduler.schedule
        mock_find.assert_called_once()  # Still 1 call

        # Force the cache to expire
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        # Third access, should call find() again
        _ = scheduler.schedule
        assert mock_find.call_count == 2

    def test_close_client(self, scheduler, monkeypatch):
        """Test that the scheduler closes the mongo client."""
        mock_close = MagicMock()
        monkeypatch.setattr(scheduler._client, 'close', mock_close)
        scheduler.close()
        mock_close.assert_called_once()

    def test_legacy_config_loading(self, celery_app, monkeypatch):
        """Test that the scheduler correctly falls back to legacy uppercase config."""
        # Clear modern config keys and set legacy ones
        celery_app.conf.mongodb_scheduler_url = None
        celery_app.conf.mongodb_scheduler_db = None
        celery_app.conf.mongodb_scheduler_collection = None
        celery_app.conf.CELERY_MONGODB_SCHEDULER_URL = 'mongodb://legacy-host:27017/legacy_db'
        celery_app.conf.CELERY_MONGODB_SCHEDULER_DB = 'legacy_db'
        celery_app.conf.CELERY_MONGODB_SCHEDULER_COLLECTION = 'legacy_schedules'

        # Patch MongoClient to return a new mongomock client, and track the call.
        # We don't need to wrap the real client. We just need to check it was called correctly
        # and provide a mock client that can be used to get a collection.
        mock_client_instance = MagicMock()
        mock_client_constructor = MagicMock(return_value=mock_client_instance)
        monkeypatch.setattr('src.celery_mongobeat.beat.MongoClient', mock_client_constructor)

        scheduler = MongoScheduler(app=celery_app)

        # Verify that MongoClient was called with the legacy URI
        mock_client_constructor.assert_called_with(
            'mongodb://legacy-host:27017/legacy_db', appname='celery-mongobeat'
        )
        # Verify the scheduler is using the correct DB and collection from legacy config
        mock_client_instance.__getitem__.assert_called_with('legacy_db')
        mock_client_instance.__getitem__.return_value.__getitem__.assert_called_with('legacy_schedules')

    def test_replace_dots_feature(self, scheduler, manager, monkeypatch):
        """Test that the replace_dots feature correctly decodes keys from the DB."""
        # Enable the feature on the scheduler instance provided by the fixture
        scheduler.replace_dots = True

        # Manually insert a document with encoded keys, as the manager doesn't encode.
        manager.collection.insert_one({
            'name': 'dotted-key-task',
            'task': 'tasks.test',
            'enabled': True,
            'interval': {'every': 1, 'period': 'seconds'},
            'kwargs': {'config__dot__key': 'value'}  # Encoded 'config.key'
        })

        monkeypatch.setattr(scheduler, 'install_default_entries', lambda _: None)
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        s = scheduler.schedule
        assert 'dotted-key-task' in s
        entry = s['dotted-key-task']

        # Verify that the key was correctly decoded back to 'config.key'
        assert 'config.key' in entry.kwargs
        assert entry.kwargs['config.key'] == 'value'

    def test_db_schedule_overwrites_static_schedule(self, scheduler, manager, monkeypatch):
        """Test that a task from the DB overwrites a static one with the same name."""
        # Define a static task in the config
        scheduler.app.conf.beat_schedule = {
            'shared-task-name': {
                'task': 'tasks.static_version',
                'schedule': 10.0,  # Runs every 10 seconds
            },
        }

        # Create a task in the DB with the same name but a different schedule
        manager.create_interval_task('shared-task-name', 'tasks.db_version', every=30, period='seconds')

        monkeypatch.setattr(scheduler, 'install_default_entries', lambda _: None)
        scheduler._last_db_load = time.monotonic() - (scheduler.max_interval + 5)

        s = scheduler.schedule
        assert len(s) == 1
        entry = s['shared-task-name']

        # Verify that the entry from the database was used
        assert entry.task == 'tasks.db_version'
        assert entry.schedule.run_every.total_seconds() == 30


class TestScheduleManager:
    """
    Tests for the ScheduleManager helper class.
    """

    def test_from_celery_app(self, celery_app, mock_mongo_client, monkeypatch):
        """
        Test that the manager can be initialized from a Celery app instance.
        """
        # monkeypatch.setattr('celery_mongobeat.helpers.MongoClient', lambda *args, **kwargs: mock_mongo_client)
        monkeypatch.setattr('src.celery_mongobeat.helpers.MongoClient', lambda *args, **kwargs: mock_mongo_client)
        manager = ScheduleManager.from_celery_app(celery_app)
        assert manager.collection.database.name == 'test_db'
        assert manager.collection.name == 'schedules'

    def test_create_and_get_task(self, manager):
        """Test creating and retrieving a task."""
        manager.create_interval_task('test-get', 'tasks.get', 1, 'seconds', args=[1], kwargs={'a': 1})
        task = manager.get_task('test-get')
        assert task is not None
        assert task['task'] == 'tasks.get'
        assert task['args'] == [1]
        assert task['kwargs'] == {'a': 1}

    def test_enable_disable_task(self, manager):
        """Test enabling and disabling a task."""
        manager.create_interval_task('test-enable', 'tasks.test', 1, 'seconds')
        manager.disable_task('test-enable')
        task = manager.get_task('test-enable')
        assert task['enabled'] is False

        manager.enable_task('test-enable')
        task = manager.get_task('test-enable')
        assert task['enabled'] is True

    def test_delete_task(self, manager):
        """Test deleting a task."""
        manager.create_interval_task('test-delete', 'tasks.test', 1, 'seconds')
        assert manager.get_task('test-delete') is not None
        manager.delete_task('test-delete')
        assert manager.get_task('test-delete') is None

    def test_get_tasks_with_filters(self, manager):
        """Test filtering tasks with get_tasks."""
        manager.create_interval_task('interval-1', 'tasks.one', 1, 'seconds', kwargs={'id': 1})
        manager.create_crontab_task('crontab-1', 'tasks.two', kwargs={'id': 2})
        manager.create_interval_task('interval-2-disabled', 'tasks.three', 1, 'seconds')
        manager.disable_task('interval-2-disabled')

        # Test count
        assert manager.count_tasks() == 3
        assert manager.count_tasks(enabled=True) == 2

        # Test get
        enabled_tasks = manager.get_tasks(enabled=True)
        assert len(enabled_tasks) == 2
        assert {'interval-1', 'crontab-1'} == {t['name'] for t in enabled_tasks}

        interval_tasks = manager.get_tasks(schedule_type='interval')
        assert len(interval_tasks) == 2

        filtered_by_kwarg = manager.get_tasks(kwargs__id=1)
        assert len(filtered_by_kwarg) == 1
        assert filtered_by_kwarg[0]['name'] == 'interval-1'

    def test_document_serialization(self, manager):
        """Test the document serialization helper methods."""
        now = datetime.datetime.now(datetime.timezone.utc)
        doc_id = ObjectId()
        original_doc = {
            '_id': doc_id,
            'name': 'serializable-task',
            'last_run_at': now,
            'total_run_count': 5
        }

        # Test document_to_serializable_dict
        serializable_dict = manager.document_to_serializable_dict(original_doc)
        assert isinstance(serializable_dict['_id'], str)
        assert serializable_dict['_id'] == str(doc_id)
        assert isinstance(serializable_dict['last_run_at'], str)
        assert serializable_dict['last_run_at'] == now.isoformat()
        assert serializable_dict['name'] == 'serializable-task'

        # Test document_to_json
        json_string = manager.document_to_json(original_doc, indent=2)
        assert isinstance(json_string, str)

        # Verify the content of the JSON string by parsing it back
        parsed_json = json.loads(json_string)
        assert parsed_json['_id'] == str(doc_id)
        assert parsed_json['last_run_at'] == now.isoformat()
        assert parsed_json['name'] == 'serializable-task'