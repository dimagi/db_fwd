"""Tests for database operations."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

from db_fwd import execute_query, DatabaseLogger


@patch('db_fwd.create_engine')
def test_execute_query_success(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"test": "data"}',)

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    result = execute_query('postgresql://localhost/test', 'SELECT data;', [])

    assert result == '{"test": "data"}'
    mock_conn.execute.assert_called_once()


@patch('db_fwd.create_engine')
def test_execute_query_with_params(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"period": "2024Q1"}',)

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    result = execute_query(
        'postgresql://localhost/test',
        "SELECT data WHERE period = :param1;",
        ['2024Q1']
    )

    assert result == '{"period": "2024Q1"}'
    call_args = mock_conn.execute.call_args
    assert call_args[0][1] == {'param1': '2024Q1'}


@patch('db_fwd.create_engine')
def test_execute_query_no_results(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = None

    with pytest.raises(ValueError, match="Query returned no results"):
        execute_query('postgresql://localhost/test', 'SELECT data;', [])


@patch('db_fwd.create_engine')
def test_execute_query_multiple_fields(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('field1', 'field2')

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    with pytest.raises(ValueError, match="Query must return exactly one field"):
        execute_query('postgresql://localhost/test', 'SELECT data, extra;', [])


@patch('db_fwd.create_engine')
def test_execute_query_database_error(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.side_effect = SQLAlchemyError("Connection failed")

    with pytest.raises(SQLAlchemyError):
        execute_query('postgresql://localhost/test', 'SELECT data;', [])


@patch('db_fwd.create_engine')
def test_database_logger_init_no_url(mock_create_engine):
    logger = DatabaseLogger(None)
    assert logger.engine is None
    mock_create_engine.assert_not_called()


@patch('db_fwd.create_engine')
def test_database_logger_init_with_url(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context

    logger = DatabaseLogger('postgresql://localhost/logs')

    assert logger.engine is not None
    mock_create_engine.assert_called_once()
    mock_conn.execute.assert_called_once()  # CREATE TABLE


@patch('db_fwd.create_engine')
def test_database_logger_log(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context

    logger = DatabaseLogger('postgresql://localhost/logs')
    logger.log('INFO', 'Test message')

    # Should be called twice: once for CREATE TABLE, once for INSERT
    assert mock_conn.execute.call_count == 2


@patch('db_fwd.create_engine')
def test_database_logger_log_no_engine(mock_create_engine):
    logger = DatabaseLogger(None)
    logger.log('INFO', 'Test message')  # Should not raise


@patch('db_fwd.create_engine')
def test_database_logger_log_error(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.side_effect = [None, SQLAlchemyError("Log insert failed")]

    logger = DatabaseLogger('postgresql://localhost/logs')

    # This should not raise an exception
    logger.log('INFO', 'Test message')


@patch('db_fwd.create_engine')
def test_execute_query_sql_injection_safe(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"data": "safe"}',)

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    malicious_param = "'; DROP TABLE users; --"

    result = execute_query(
        'postgresql://localhost/test',
        "SELECT data WHERE id = :param1;",
        [malicious_param]
    )

    # Verify the parameter was passed safely, not interpolated into the query
    call_args = mock_conn.execute.call_args
    # First argument should be the text object with the query
    assert ":param1" in str(call_args[0][0])
    # Second argument should be the params dict with the malicious string safely contained
    assert call_args[0][1] == {'param1': "'; DROP TABLE users; --"}
    assert result == '{"data": "safe"}'


@patch('db_fwd.create_engine')
def test_execute_query_multiple_params(mock_create_engine):
    mock_engine = Mock()
    mock_conn = Mock()
    mock_result = Mock()
    mock_row = ('{"result": "success"}',)

    mock_create_engine.return_value = mock_engine
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context
    mock_conn.execute.return_value = mock_result
    mock_result.fetchone.return_value = mock_row

    result = execute_query(
        'postgresql://localhost/test',
        "SELECT data WHERE category = :param1 AND period = :param2 AND status = :param3;",
        ['category1', '2024Q1', 'active']
    )

    call_args = mock_conn.execute.call_args
    assert call_args[0][1] == {
        'param1': 'category1',
        'param2': '2024Q1',
        'param3': 'active'
    }
    assert result == '{"result": "success"}'
