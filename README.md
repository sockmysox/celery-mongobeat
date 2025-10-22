# celery-mongobeat
A reliable and persistent Celery Beat schduler backed using MongoDB.

# Description
The standard Celery Beat scheduler is notoriously single-point-of-failure and relies on local files for state management.  This project provides a robust, highly available alternative by utilizing MongoDB to store and manage the periodic task schedule.

By moving the schedule and state into a database, celery-mongobeat, allows you to run multiple Beat processes simultaneously for redundancy and easily change scheduled tasks without restarting the Beat process, making it ideal for containerized or cloud-native deploments.

It's designed to be a simple drop-in replacement making it an easy to migrate from the file-based scheduler to a durable database solution.

# Features
- High Availability: Run multiple Beat instances sumultaneously without conflicts.
- Dynamic Scheduling: Add, remove, or modify periodic tasks at runtime.
- Database-Backend State: Persists task schedule and run times in MongoDB.
- Simple COnfiguration: Easily integrates into existing Celery applications.

# Installation
You can install `celery-mongobeat` directly from PyPI using pip.
```
pip install celery-mongobeat
```

# Configuration
To use the MongoDB scheduler, you need to configure your Celery application to use the custom scheduler class and provide connection details for your MongoDB instance.

1. Celery App Settings
2. Define Periodic Tasks

# Usage
Once configured, simply start your Celery worker and the Beat process.
1. Start the Celery Worker
2. Start the MongoDB-Backend Celery Beat

