"""
Tests for database operations.

Assumes a PostgreSQL service running on localhost on port 5432 with
user "postgres" and password "postgres".
"""

import json
import pytest
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from unmagic import fixture

from db_fwd import execute_query, DatabaseHandler

TEST_DB_URL = 'postgresql://postgres:postgres@localhost:5432/postgres'


@fixture()
def test_db():
    engine = create_engine(TEST_DB_URL)

    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS test_data (
                    id SERIAL PRIMARY KEY,
                    data JSONB
                )
                """
            )
        )
        conn.commit()

    try:
        yield TEST_DB_URL

    finally:
        with engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_data'))
            conn.commit()

        engine.dispose()


@fixture()
def test_log_db():
    engine = create_engine(TEST_DB_URL)

    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS db_fwd_logs'))
        conn.commit()

    try:
        yield TEST_DB_URL

    finally:
        with engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS db_fwd_logs'))
            conn.commit()

        engine.dispose()


def test_execute_query_success():
    db_url = test_db()
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(
            text('INSERT INTO test_data (data) VALUES (:data)'),
            {'data': '{"test": "data"}'},
        )
        conn.commit()

    engine.dispose()

    result = execute_query(
        db_url, 'SELECT data::text FROM test_data LIMIT 1;', []
    )

    assert result == '{"test": "data"}'


def test_execute_query_with_params():
    db_url = test_db()
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(
            text('INSERT INTO test_data (data) VALUES (:data)'),
            {'data': '{"period": "2024Q1"}'},
        )
        conn.commit()

    engine.dispose()

    result = execute_query(
        db_url,
        "SELECT data::text FROM test_data WHERE data->>'period' = :param1;",
        ['2024Q1'],
    )

    assert result == '{"period": "2024Q1"}'


def test_execute_query_no_results():
    db_url = test_db()

    with pytest.raises(ValueError, match='Query returned no results'):
        execute_query(
            db_url, 'SELECT data FROM test_data WHERE id = 99999;', []
        )


def test_execute_query_multiple_fields():
    db_url = test_db()
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(
            text('INSERT INTO test_data (data) VALUES (:data)'),
            {'data': '{"test": "data"}'},
        )
        conn.commit()

    engine.dispose()

    with pytest.raises(
        ValueError, match='Query must return exactly one field'
    ):
        execute_query(db_url, 'SELECT id, data FROM test_data;', [])


def test_execute_query_multiple_rows():
    db_url = test_db()
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(
            text('INSERT INTO test_data (data) VALUES (:data)'),
            {'data': '{"test": "data1"}'},
        )
        conn.execute(
            text('INSERT INTO test_data (data) VALUES (:data)'),
            {'data': '{"test": "data2"}'},
        )
        conn.commit()

    engine.dispose()

    with pytest.raises(ValueError, match='Query returned more than one row'):
        execute_query(db_url, 'SELECT data FROM test_data;', [])


@patch('db_fwd.create_engine')
def test_execute_query_database_error(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.side_effect = SQLAlchemyError('Connection failed')

    with pytest.raises(SQLAlchemyError):
        execute_query('postgresql://localhost/test', 'SELECT data;', [])


def test_database_handler_init():
    log_db_url = test_log_db()
    handler = DatabaseHandler(log_db_url)

    assert handler.engine is not None

    engine = create_engine(log_db_url)
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name='db_fwd_logs'
                )
                """
            )
        ).scalar()
        assert exists is True

    engine.dispose()


def test_database_handler_emit():
    import logging

    log_db_url = test_log_db()
    handler = DatabaseHandler(log_db_url)

    logger = logging.getLogger('test_logger')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    logger.info('Test message')

    engine = create_engine(log_db_url)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT level, message FROM db_fwd_logs'))
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'INFO'
        assert 'Test message' in row[1]

    engine.dispose()
    logger.removeHandler(handler)


@patch('db_fwd.create_engine')
def test_database_handler_emit_error(mock_create_engine):
    import logging

    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.side_effect = [
        None,
        SQLAlchemyError('Log insert failed'),
    ]

    handler = DatabaseHandler('postgresql://localhost/logs')

    logger = logging.getLogger('test_error_logger')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    # This should not raise an exception
    logger.info('Test message')

    logger.removeHandler(handler)


def test_execute_query_sql_injection_safe():
    db_url = test_db()
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(
            text('INSERT INTO test_data (id, data) VALUES (1, :data)'),
            {'data': '{"data": "safe"}'},
        )
        conn.commit()

    engine.dispose()

    malicious_param = "'; DROP TABLE test_data; --"

    try:
        execute_query(
            db_url,
            "SELECT data FROM test_data WHERE data->>'data' = :param1;",
            [malicious_param],
        )
    except ValueError:
        pass  # Expected - no results found

    engine = create_engine(db_url)
    with engine.connect() as conn:
        exists = conn.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name='test_data'
                )
                """
            )
        ).scalar()
        assert (
            exists is True
        ), 'Table should still exist - SQL injection was prevented'

    engine.dispose()


def test_execute_query_multiple_params():
    db_url = test_db()
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(
            text('INSERT INTO test_data (data) VALUES (:data)'),
            {
                'data': '{"category": "category1", "period": "2024Q1", "status": "active"}'
            },
        )
        conn.commit()

    engine.dispose()

    result = execute_query(
        db_url,
        """
        SELECT data::text
        FROM test_data
        WHERE data->>'category' = :param1
          AND data->>'period' = :param2
          AND data->>'status' = :param3;
        """,
        ['category1', '2024Q1', 'active'],
    )

    assert json.loads(result) == {
        'category': 'category1',
        'period': '2024Q1',
        'status': 'active',
    }
