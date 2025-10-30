"""Tests for configuration management."""

import os
import tempfile
from pathlib import Path

import pytest

from db_fwd import Config


@pytest.fixture
def sample_config_file():
    config_content = """
[db_fwd]
log_level = 'debug'
log_file = 'test.log'
log_db_url = 'postgresql://localhost/test_logs'

[queries]
db_url = 'postgresql://localhost/test_db'
api_username = 'test_user'
api_password = 'test_pass'
api_url = 'https://example.com/api/default'

[queries.test_query]
query = "SELECT json_payload FROM test_view WHERE id = '%s';"
api_url = 'https://example.com/api/test'

[queries.query_with_db]
query = "SELECT data FROM other_view;"
db_url = 'postgresql://localhost/other_db'
api_username = 'other_user'
api_password = 'other_pass'

[queries.minimal_query]
query = "SELECT result FROM minimal;"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        yield temp_path
    finally:
        Path(temp_path).unlink()


def test_config_load_success(sample_config_file):
    config = Config(sample_config_file)
    assert config.config is not None
    assert 'db_fwd' in config.config
    assert 'queries' in config.config


def test_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        Config('nonexistent.toml')


def test_get_log_level(sample_config_file):
    config = Config(sample_config_file)
    assert config.get_log_level() == 'debug'


def test_get_log_level_default():
    config_content = '[db_fwd]\n'

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = Config(temp_path)
        assert config.get_log_level() == 'info'
    finally:
        Path(temp_path).unlink()


def test_get_log_file(sample_config_file):
    config = Config(sample_config_file)
    assert config.get_log_file() == 'test.log'


def test_get_log_file_default():
    config_content = '[db_fwd]\n'

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = Config(temp_path)
        assert config.get_log_file() == 'db_fwd.log'
    finally:
        Path(temp_path).unlink()


def test_get_log_db_url(sample_config_file):
    config = Config(sample_config_file)
    assert config.get_log_db_url() == 'postgresql://localhost/test_logs'


def test_get_db_url_from_queries_section(sample_config_file):
    config = Config(sample_config_file)
    assert config.get_db_url('test_query') == 'postgresql://localhost/test_db'


def test_get_db_url_query_specific(sample_config_file):
    config = Config(sample_config_file)
    assert (
        config.get_db_url('query_with_db') == 'postgresql://localhost/other_db'
    )


def test_get_db_url_from_env():
    config_content = """
[queries]

[queries.test]
query = "SELECT 1;"
api_url = "https://example.com/api"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        os.environ['DB_FWD_DB_URL'] = 'postgresql://env/db'
        config = Config(temp_path)
        assert config.get_db_url('test') == 'postgresql://env/db'
    finally:
        Path(temp_path).unlink()
        if 'DB_FWD_DB_URL' in os.environ:
            del os.environ['DB_FWD_DB_URL']


def test_get_db_url_missing():
    config_content = """
[queries]

[queries.test]
query = "SELECT 1;"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = Config(temp_path)
        with pytest.raises(ValueError, match='Database URL not configured'):
            config.get_db_url('test')
    finally:
        Path(temp_path).unlink()


def test_get_query(sample_config_file):
    config = Config(sample_config_file)
    query = config.get_query('test_query')
    assert 'SELECT json_payload FROM test_view' in query


def test_get_query_not_found(sample_config_file):
    config = Config(sample_config_file)
    with pytest.raises(ValueError, match="Query 'nonexistent' not found"):
        config.get_query('nonexistent')


def test_get_query_no_sql(sample_config_file):
    config_content = """
[queries]

[queries.bad_query]
api_url = "https://example.com/api"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = Config(temp_path)
        with pytest.raises(ValueError, match='No query defined'):
            config.get_query('bad_query')
    finally:
        Path(temp_path).unlink()


def test_get_api_url_query_specific(sample_config_file):
    config = Config(sample_config_file)
    assert config.get_api_url('test_query') == 'https://example.com/api/test'


def test_get_api_url_from_queries_section(sample_config_file):
    config = Config(sample_config_file)
    assert (
        config.get_api_url('minimal_query')
        == 'https://example.com/api/default'
    )


def test_get_api_url_missing():
    config_content = """
[queries]

[queries.test]
query = "SELECT 1;"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = Config(temp_path)
        with pytest.raises(ValueError, match='API URL not configured'):
            config.get_api_url('test')
    finally:
        Path(temp_path).unlink()


def test_get_api_credentials_from_queries(sample_config_file):
    config = Config(sample_config_file)
    creds = config.get_api_credentials('test_query')
    assert creds == ('test_user', 'test_pass')


def test_get_api_credentials_query_specific(sample_config_file):
    config = Config(sample_config_file)
    creds = config.get_api_credentials('query_with_db')
    assert creds == ('other_user', 'other_pass')


def test_get_api_credentials_from_env():
    config_content = """
[queries]

[queries.test]
query = "SELECT 1;"
api_url = "https://example.com/api"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        os.environ['DB_FWD_API_USERNAME'] = 'env_user'
        os.environ['DB_FWD_API_PASSWORD'] = 'env_pass'
        config = Config(temp_path)
        creds = config.get_api_credentials('test')
        assert creds == ('env_user', 'env_pass')
    finally:
        Path(temp_path).unlink()
        if 'DB_FWD_API_USERNAME' in os.environ:
            del os.environ['DB_FWD_API_USERNAME']
        if 'DB_FWD_API_PASSWORD' in os.environ:
            del os.environ['DB_FWD_API_PASSWORD']


def test_get_api_credentials_none():
    config_content = """
[queries]

[queries.test]
query = "SELECT 1;"
api_url = "https://example.com/api"
"""

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.toml', delete=False
    ) as f:
        f.write(config_content)
        temp_path = f.name

    try:
        config = Config(temp_path)
        creds = config.get_api_credentials('test')
        assert creds is None
    finally:
        Path(temp_path).unlink()
