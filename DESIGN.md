db_fwd
======

**db_fwd** is a command line tool to call a SQL query that returns
a JSON result, and to send that result to a DHIS2 API endpoint. The
query result, the API request, and the API response are all logged.

Read [README.md](README.md) for details.


Tech stack
----------

* Language: Python 3.13
* Virtual environment: uv
* Database library: SQLAlchemy 2.0
* HTTP library: Requests
* Testing library: pytest


Tests
-----

Test modules are located in the `tests/` directory. They use functional
pytest style.
