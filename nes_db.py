import os
import pymysql
import logging

# These environment variables are configured in app.yaml.
CLOUD_SQL_CONNECTION_NAME = os.environ.get('CLOUD_SQL_CONNECTION_NAME')
CLOUD_SQL_USERNAME = os.environ.get('CLOUD_SQL_USERNAME', 'root')
CLOUD_SQL_PASSWORD = os.environ.get('CLOUD_SQL_PASSWORD')
CLOUD_SQL_DATABASE_NAME = os.environ.get('CLOUD_SQL_DATABASE_NAME', 'nes_data')

def connect():
    # When deployed to App Engine, the `GAE_ENV` environment variable will be
    # set to `standard`
    if os.environ.get('GAE_ENV') == 'standard':
        # If deployed, use the local socket interface for accessing Cloud SQL
        unix_socket = '/cloudsql/{}'.format(CLOUD_SQL_CONNECTION_NAME)
        logging.debug('Trying to connect to Cloud SQL at Unix socket: %s' % unix_socket)
        db = pymysql.connect(user=CLOUD_SQL_USERNAME, password=CLOUD_SQL_PASSWORD,
                             unix_socket=unix_socket, db=CLOUD_SQL_DATABASE_NAME)
    else:
        # If running locally, use the TCP connections instead
        # Set up Cloud SQL Proxy (cloud.google.com/sql/docs/mysql/sql-proxy)
        # so that your application can use 127.0.0.1:3306 to connect to your
        # Cloud SQL instance
        host = '127.0.0.1'
        db = pymysql.connect(user=CLOUD_SQL_USERNAME, password=CLOUD_SQL_PASSWORD,
                             host=host, db=CLOUD_SQL_DATABASE_NAME)

    return db
