"""
Pytest fixtures for celery-mongobeat tests.
"""
import pytest
from celery import Celery
from mongomock import MongoClient


@pytest.fixture
def celery_app():
    """
    Provides a mock Celery application instance for tests.
    """
    app = Celery('test_app')
    app.conf.update({
        'mongodb_scheduler_url': 'mongodb://localhost:27017/test_db',
        'mongodb_scheduler_db': 'test_db',
        'mongodb_scheduler_collection': 'schedules',
    })
    return app


@pytest.fixture
def mock_mongo_client():
    """
    Provides a mongomock.MongoClient instance for tests.
    This fixture ensures that tests do not interact with a real database.
    """
    return MongoClient()


@pytest.fixture
def mock_collection(mock_mongo_client):
    """
    Provides a mock MongoDB collection using mongomock.
    """
    return mock_mongo_client.test_db.schedules