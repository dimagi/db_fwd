#!/usr/bin/env python3
# /// script
# dependencies = [
#     'requests',
#     'sqlalchemy',
#     'psycopg2-binary',
# ]
# ///

import argparse
import logging
import os
import sys
import tomllib
from pathlib import Path
from typing import Any, Optional

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

type UsernameType = Optional[str]
type PasswordType = Optional[str]
type CredentialsType = tuple[UsernameType, PasswordType]


class Config:
    """Configuration manager for db_fwd."""

    def __init__(self, config_filename ="db_fwd.toml"):
        self.config_file = config_filename
        self.config = {}
        self.load_config()

    def load_config(self):
        config_path = Path(self.config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_file}")

        with open(config_path, "rb") as f:
            self.config = tomllib.load(f)

    def get_log_level(self):
        return self.config.get("db_fwd", {}).get("log_level", "info")

    def get_log_file(self):
        return self.config.get("db_fwd", {}).get("log_file", "db_fwd.log")

    def get_log_db_url(self) -> Optional[str]:
        return self.config.get("db_fwd", {}).get("log_db_url")

    def get_db_url(self, query_name=None):
        # Check query-specific db_url first
        if query_name and "queries" in self.config:
            query_config = self.config["queries"].get(query_name, {})
            if "db_url" in query_config:
                return query_config["db_url"]

        # Check queries section db_url
        if "queries" in self.config and "db_url" in self.config["queries"]:
            return self.config["queries"]["db_url"]

        # Fall back to environment variable
        db_url = os.environ.get("DB_FWD_DB_URL")
        if not db_url:
            raise ValueError("Database URL not configured")
        return db_url

    def get_query(self, query_name):
        if "queries" not in self.config or query_name not in self.config["queries"]:
            raise ValueError(f"Query '{query_name}' not found in configuration")

        query_config = self.config["queries"][query_name]
        if "query" not in query_config:
            raise ValueError(f"No query defined for '{query_name}'")

        return query_config["query"]

    def get_api_url(self, query_name):
        # Check query-specific api_url
        if "queries" in self.config and query_name in self.config["queries"]:
            query_config = self.config["queries"][query_name]
            if "api_url" in query_config:
                return query_config["api_url"]

        # Check queries section api_url
        if "queries" in self.config and "api_url" in self.config["queries"]:
            return self.config["queries"]["api_url"]

        raise ValueError(f"API URL not configured for query '{query_name}'")

    def get_api_credentials(self, query_name: Optional[str] = None) -> CredentialsType:
        username = password = None

        # Check query-specific credentials
        if query_name and "queries" in self.config and query_name in self.config["queries"]:
            query_config = self.config["queries"][query_name]
            username = query_config.get("api_username")
            password = query_config.get("api_password")

        # Fall back to queries section credentials
        if not username and "queries" in self.config:
            username = self.config["queries"].get("api_username")
            password = self.config["queries"].get("api_password")

        # Fall back to environment variables
        if not username:
            username = os.environ.get("DB_FWD_API_USERNAME")
        if not password:
            password = os.environ.get("DB_FWD_API_PASSWORD")

        return username, password


class DatabaseLogger:
    """Logger that writes to a database table."""

    def __init__(self, db_url: Optional[str]):
        self.db_url = db_url
        self.engine = None
        if db_url:
            self.engine = create_engine(db_url)
            self._ensure_table()

    def _ensure_table(self):
        if not self.engine:
            return

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS db_fwd_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR(10),
            message TEXT
        )
        """

        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

    def log(self, level_name, message):
        if not self.engine:
            return

        insert_sql = """
        INSERT INTO db_fwd_logs (level, message)
        VALUES (:level, :message)
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(insert_sql), {"level": level_name, "message": message})
                conn.commit()
        except SQLAlchemyError as e:
            # Don't fail the main operation if logging fails
            logging.error(f"Failed to log to database: {e}")


def set_up_logging(log_level_name, log_filename):
    level_map = {
        "none": logging.CRITICAL + 1,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }

    level = level_map.get(log_level_name.lower(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_filename)],
        force=True
    )


def execute_query(db_url, query, params):
    """Execute SQL query and return the result using parameterized queries.

    Query parameters should use named placeholders like :param1, :param2, etc.
    or positional placeholders that SQLAlchemy supports.
    """
    engine = create_engine(db_url)

    # Assumes parameters are named :param1, :param2, etc. in the query
    if params:
        param_dict = {f"param{i+1}": param for i, param in enumerate(params)}
    else:
        param_dict = {}

    logging.info(f"Executing query: {query}")
    logging.debug(f"Query parameters: {param_dict}")

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), param_dict)
            row = result.fetchone()

            if not row:
                raise ValueError("Query returned no results")

            if len(row) != 1:
                raise ValueError("Query must return exactly one field")

            return row[0]
    except SQLAlchemyError as e:
        logging.error(f"Database error: {e}")
        raise


def forward_to_api(
    api_url: str,
    payload: Any,
    username: UsernameType,
    password: PasswordType,
    db_logger: DatabaseLogger,
) -> None:

    auth = None
    if username and password:
        auth = (username, password)

    logging.info(f"Forwarding to API: {api_url}")
    db_logger.log("DEBUG", f"API Request - URL: {api_url}, Payload: {payload}")

    try:
        response = requests.post(
            api_url,
            json=payload,
            auth=auth,
            headers={"Content-Type": "application/json"}
        )

        logging.info(f"API Response - Status: {response.status_code}")
        db_logger.log("DEBUG", f"API Response - Status: {response.status_code}, Body: {response.text}")

        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        db_logger.log("ERROR", f"API request failed: {e}")
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Forwards a SQL query result to a web API endpoint."
    )
    parser.add_argument(
        "--log-level",
        choices=["none", "info", "debug"],
        help="Logging level (overrides config file)"
    )
    parser.add_argument(
        "--log-file",
        help="Log file path (overrides config file)"
    )
    parser.add_argument(
        "--config-file",
        default="db_fwd.toml",
        help="Configuration file path (default: db_fwd.toml)"
    )
    parser.add_argument(
        "query_name",
        help="Name of the query to execute"
    )
    parser.add_argument(
        "query_params",
        nargs="*",
        help="Parameters for the query"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    try:
        config = Config(args.config_file)

        log_level = args.log_level or config.get_log_level()
        log_file = args.log_file or config.get_log_file()
        set_up_logging(log_level, log_file)

        log_db_url = config.get_log_db_url()
        db_logger = DatabaseLogger(log_db_url)

        logging.info(f"Starting db_fwd for query: {args.query_name}")
        db_logger.log("INFO", f"Starting db_fwd for query: {args.query_name}")

        db_url = config.get_db_url(args.query_name)
        query = config.get_query(args.query_name)
        api_url = config.get_api_url(args.query_name)
        username, password = config.get_api_credentials(args.query_name)

        result = execute_query(db_url, query, args.query_params)
        logging.info(f"Query result: {result}")
        db_logger.log("DEBUG", f"Query result: {result}")

        forward_to_api(api_url, result, username, password, db_logger)

        logging.info("Completed successfully")
        db_logger.log("INFO", "Completed successfully")

    except Exception as e:
        logging.error(f"Error: {e}")
        if 'db_logger' in locals():
            db_logger.log("ERROR", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
