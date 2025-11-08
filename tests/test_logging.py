"""Tests for logging functionality."""

import logging
import re
import tempfile
from pathlib import Path

import pytest

from db_fwd import set_up_logging


def test_setup_logging_info_level():
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / 'test.log'

        set_up_logging('info', str(log_file), None)

        logging.info('Test info message')

        # Close all handlers to ensure messages are written
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)

        with open(log_file, 'r') as f:
            content = f.read()

        assert 'Test info message' in content
        assert 'INFO' in content


def test_setup_logging_debug_level():
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / 'test.log'

        set_up_logging('debug', str(log_file), None)

        logging.debug('Test debug message')
        logging.info('Test info message')

        # Close all handlers to ensure messages are written
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)

        with open(log_file, 'r') as f:
            content = f.read()

        assert 'Test debug message' in content
        assert 'DEBUG' in content
        assert 'Test info message' in content


def test_setup_logging_none_level():
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.log', delete=False
    ) as f:
        log_file = f.name

    try:
        set_up_logging('none', log_file, None)

        logging.critical('Test critical message')
        logging.error('Test error message')
        logging.info('Test info message')

        with open(log_file, 'r') as f:
            content = f.read()

        assert 'Test info message' not in content
        assert 'Test error message' not in content
    finally:
        Path(log_file).unlink()
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_creates_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / 'new_log.log'
        assert not log_file.exists()

        set_up_logging('info', str(log_file), None)

        logging.info('Test message')

        assert log_file.exists()

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


def test_setup_logging_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / 'test.log'

        set_up_logging('info', str(log_file), None)

        logging.info('Test message')

        # Close all handlers to ensure messages are written
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)

        with open(log_file, 'r') as f:
            content = f.read()

        assert ' - INFO - Test message' in content
        assert re.search(r'\d{4}-\d{2}-\d{2}', content)


def test_setup_logging_invalid_level():
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / 'test.log'

        with pytest.raises(ValueError, match="Invalid log level 'invalid'"):
            set_up_logging('invalid', str(log_file), None)
