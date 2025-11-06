"""Tests for API operations."""

import logging
import pytest
from unittest.mock import Mock, patch
import requests

from db_fwd import forward_to_api


@patch('db_fwd.requests.post')
def test_forward_to_api_success(mock_post):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = '{"success": true}'
    mock_post.return_value = mock_response

    payload = {'test': 'data'}
    forward_to_api('https://example.com/api', payload, ('user', 'pass'))

    mock_post.assert_called_once_with(
        'https://example.com/api',
        json=payload,
        auth=('user', 'pass'),
        headers={'Content-Type': 'application/json'},
    )
    mock_response.raise_for_status.assert_called_once()


@patch('db_fwd.requests.post')
def test_forward_to_api_no_auth(mock_post):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = '{"success": true}'
    mock_post.return_value = mock_response

    payload = {'test': 'data'}
    forward_to_api('https://example.com/api', payload, None)

    mock_post.assert_called_once_with(
        'https://example.com/api',
        json=payload,
        auth=None,
        headers={'Content-Type': 'application/json'},
    )


@patch('db_fwd.requests.post')
def test_forward_to_api_http_error(mock_post):
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = 'Internal Server Error'
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        '500 Server Error'
    )
    mock_post.return_value = mock_response

    payload = {'test': 'data'}

    with pytest.raises(requests.exceptions.HTTPError):
        forward_to_api('https://example.com/api', payload, ('user', 'pass'))


@patch('db_fwd.requests.post')
def test_forward_to_api_connection_error(mock_post):
    mock_post.side_effect = requests.exceptions.ConnectionError(
        'Connection refused'
    )

    payload = {'test': 'data'}

    with pytest.raises(requests.exceptions.ConnectionError):
        forward_to_api('https://example.com/api', payload, ('user', 'pass'))


@patch('db_fwd.requests.post')
def test_forward_to_api_timeout(mock_post):
    mock_post.side_effect = requests.exceptions.Timeout('Request timed out')

    payload = {'test': 'data'}

    with pytest.raises(requests.exceptions.Timeout):
        forward_to_api('https://example.com/api', payload, ('user', 'pass'))


@patch('db_fwd.requests.post')
def test_forward_to_api_json_payload(mock_post):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = '{"success": true}'
    mock_post.return_value = mock_response

    payload = '{"period": "2024Q1", "data": [1, 2, 3]}'

    forward_to_api('https://example.com/api', payload, ('user', 'pass'))

    call_kwargs = mock_post.call_args[1]
    assert call_kwargs['json'] == payload


@patch('db_fwd.requests.post')
def test_forward_to_api_logging(mock_post, caplog):
    mock_response = Mock()
    mock_response.status_code = 201
    mock_response.text = '{"id": 123}'
    mock_post.return_value = mock_response

    payload = {'test': 'data'}

    with caplog.at_level(logging.DEBUG):
        forward_to_api('https://example.com/api', payload, ('user', 'pass'))

    debug_records = [r for r in caplog.records if r.levelname == 'DEBUG']
    assert len(debug_records) >= 2  # Request and response should be logged
