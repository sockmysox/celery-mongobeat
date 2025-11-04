"""
Tests for the MongoScheduler and ScheduleManager.
"""
import datetime
import time
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
        task_doc = manager.get_task(name='sync-test-task')
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
        task_doc = manager.get_task(name='limited-run-task')
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

    def test_max_interval_configuration(self, celery_app, monkeypatch):
        """Test that max_interval is set correctly from config and has a default."""
        # To avoid real DB connections, we patch MongoClient for this test
        monkeypatch.setattr('src.celery_mongobeat.beat.MongoClient', lambda *args, **kwargs: MagicMock())

        # --- Test Case 1: Value is set in config ---
        celery_app.conf.beat_max_loop_interval = 60
        scheduler_with_config = MongoScheduler(app=celery_app)
        assert scheduler_with_config.max_interval == 60

        # --- Test Case 2: Value is NOT set in config ---
        # The parent Scheduler sets beat_max_loop_interval to None if not present
        celery_app.conf.beat_max_loop_interval = None
        scheduler_with_default = MongoScheduler(app=celery_app)
        # Verify it falls back to the hardcoded default of 300
        assert scheduler_with_default.max_interval == 300


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

    def test_from_celery_app_auto_discovery(self, celery_app, mock_mongo_client, monkeypatch):
        """
        Test that from_celery_app correctly discovers the current app when none is provided.
        """
        # Patch `celery.current_app` to return our test app fixture
        mock_current_app = MagicMock()
        mock_current_app._get_current_object.return_value = celery_app
        monkeypatch.setattr('src.celery_mongobeat.helpers.celery_current_app', mock_current_app)

        # Patch MongoClient to use the mock client
        monkeypatch.setattr('src.celery_mongobeat.helpers.MongoClient', lambda *args, **kwargs: mock_mongo_client)

        # Call from_celery_app without an app argument to test auto-discovery
        manager = ScheduleManager.from_celery_app()

        # Assert that the manager was configured correctly from the discovered app
        assert manager.collection.database.name == 'test_db'
        assert manager.collection.name == 'schedules'

    def test_create_and_get_task(self, manager):
        """Test creating, updating, and retrieving a task, and verify ObjectId return."""
        # 1. Test creation: A new task should return the full document.
        created_doc = manager.create_interval_task('test-get', 'tasks.get', 1, 'seconds', args=[1], kwargs={'a': 1})
        assert isinstance(created_doc, dict)
        assert isinstance(created_doc['_id'], ObjectId)
        task_id = created_doc['_id']

        # 2. Test retrieval: The retrieved task should have the correct data.
        task = manager.get_task(name='test-get')
        assert task is not None
        assert task['_id'] == task_id
        assert task['task'] == 'tasks.get'
        assert task['args'] == [1]
        assert task['kwargs'] == {'a': 1}

        # 3. Test retrieval by ID: The retrieved task should be the same.
        task_by_id = manager.get_task(id=task_id)
        assert task_by_id is not None
        assert task_by_id['name'] == 'test-get'

        # 4. Test update: Updating the same task should return a document with the same ObjectId.
        updated_doc = manager.create_interval_task('test-get', 'tasks.get.updated', 2, 'seconds')
        assert isinstance(updated_doc, dict)
        assert updated_doc['_id'] == task_id

    def test_enable_disable_task(self, manager):
        """Test enabling and disabling a task."""
        manager.create_interval_task('test-enable', 'tasks.test', 1, 'seconds')
        manager.disable_task('test-enable')
        task = manager.get_task(name='test-enable')
        assert task['enabled'] is False

        manager.enable_task('test-enable')
        task = manager.get_task(name='test-enable')
        assert task['enabled'] is True

    def test_delete_task(self, manager):
        """Test deleting a task."""
        # Create two tasks to test deletion by name and by id
        task1 = manager.create_interval_task('delete-by-name', 'tasks.test', 1, 'seconds')
        task2 = manager.create_interval_task('delete-by-id', 'tasks.test', 1, 'seconds')

        # Verify deletion by name
        assert manager.get_task(name='delete-by-name') is not None
        manager.delete_task(name='delete-by-name')
        assert manager.get_task(name='delete-by-name') is None

        # Verify deletion by id
        assert manager.get_task(id=task2['_id']) is not None
        manager.delete_task(id=task2['_id'])
        assert manager.get_task(name='test-delete') is None

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

    def test_get_task_with_serialization(self, manager):
        """Test the `serialize` flag on get_task and get_tasks."""
        now = datetime.datetime.now(datetime.timezone.utc)
        manager.create_interval_task('serialize-test', 'tasks.test', 1, 'seconds')
        manager.collection.update_one(
            {'name': 'serialize-test'},
            {'$set': {'last_run_at': now}}
        )

        # Test get_task with serialization
        task = manager.get_task(name='serialize-test', serialize=True)
        assert task is not None
        assert isinstance(task['_id'], str)
        # Parse the serialized datetime string and compare with a tolerance
        # to account for potential precision loss during DB serialization.
        # The string from isoformat on a naive datetime is naive, so we make it aware.
        serialized_dt_naive = datetime.datetime.fromisoformat(task['last_run_at'])
        serialized_dt_aware = serialized_dt_naive.replace(tzinfo=datetime.timezone.utc)
        assert abs(serialized_dt_aware - now) < datetime.timedelta(seconds=1)

        # Test get_tasks with serialization
        tasks = manager.get_tasks(serialize=True, name='serialize-test')
        assert len(tasks) == 1
        assert isinstance(tasks[0]['_id'], str)
        # Also check the timestamp from the get_tasks result
        serialized_dt_from_list_naive = datetime.datetime.fromisoformat(tasks[0]['last_run_at'])
        serialized_dt_from_list_aware = serialized_dt_from_list_naive.replace(
            tzinfo=datetime.timezone.utc
        )
        assert abs(serialized_dt_from_list_aware - now) < datetime.timedelta(seconds=1)

    def test_create_task_from_dictionary(self, manager):
        """Test creating a task by unpacking a dictionary with custom fields."""
        task_data = {
            'name': 'task-from-dict',
            'task': 'tasks.dict_task',
            'every': 30,
            'period': 'minutes',
            'description': 'A task created from a dictionary.',
            'display_name': 'My Dictionary Task'  # Custom field
        }

        # Use dictionary unpacking to pass all data
        created_doc = manager.create_interval_task(**task_data)
        assert created_doc is not None

        # Verify that the custom field was saved correctly
        assert created_doc['display_name'] == 'My Dictionary Task'
        assert created_doc['description'] == 'A task created from a dictionary.'
        assert created_doc['interval']['every'] == 30

    def test_create_task_with_custom_fields(self, manager):
        """Test that arbitrary keyword arguments are saved as custom fields."""
        manager.create_interval_task(
            'task-with-custom-fields',
            'tasks.test',
            1, 'seconds',
            owner='data-team',
            priority=100
        )
        task = manager.get_task(name='task-with-custom-fields')
        assert task is not None
        assert task['owner'] == 'data-team'
        assert task['priority'] == 100

    def test_update_task(self, manager):
        """Test updating a task using various identification methods."""
        # Create an initial task
        created_doc = manager.create_interval_task(
            'task-to-update', 'tasks.test', 1, 'seconds', description='Initial'
        )
        task_id = created_doc['_id']

        # 1. Update by name
        updated_doc_by_name = manager.update_task(name='task-to-update', description='Updated by name')
        assert updated_doc_by_name is not None
        assert updated_doc_by_name['description'] == 'Updated by name'

        # 2. Update by ID
        updated_doc_by_id = manager.update_task(id=task_id, description='Updated by ID')
        assert updated_doc_by_id is not None
        assert updated_doc_by_id['description'] == 'Updated by ID'

        # 3. Update using `name` from data payload
        updated_doc_from_data_name = manager.update_task(description='Updated from data name', name='task-to-update')
        assert updated_doc_from_data_name['description'] == 'Updated from data name'

        # 4. Update using `_id` from data payload
        updated_doc_from_data_id = manager.update_task(description='Updated from data id', _id=task_id)
        assert updated_doc_from_data_id['description'] == 'Updated from data id'

        # 5. Update using `id` (as string) from data payload
        updated_doc_from_data_id_str = manager.update_task(description='Updated from data id string', id=str(task_id))
        assert updated_doc_from_data_id_str['description'] == 'Updated from data id string'