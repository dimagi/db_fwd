# Documentation for Coding Agents

## Context

See @README.md for purpose, usage and configuration.

See @DESIGN.md for design context.


## Comands

The following commands assume that the virtual environment is active.

* Running tests: `pytest tests/`
* Type checking: `mypy db_fwd.py tests/`
* Linting: `ruff check db_fwd.py tests/`
* Formatting: `ruff format <path/to/file.py>`


## Coding style

### Avoid using comments, docstrings, and type hints.

Don't use comments to indicate _what_ the code does; that should be
obvious from the code itself. Use comments to explain _why_ the code
does what it does, and only when it might not be clear.

Use docstrings to give the purpose of a module or class. Avoid
docstrings on methods or functions where their purpose is clear from the
name. Use reStructuredText format in docstrings.

Use docstrings for doctests for functions and methods where a doctest
can demonstrate usage in a simple way. Doctests can augment but should
not replace unit tests. Run doctests from an appropriate test module.

Don't use docstrings for test functions if the function's name explains
what it is testing. 

Use type hints when it would be useful to know a parameter's class, or
where a parameter's type, or a function- or method's return value, is
not obvious from its name. If you do use a type hint in a function or
method definition, then include type hints for all its parameters and
its return value.

Use type aliases (e.g. `type UsernameType = Optional[str]`) where it
would clarify the usage of a type.
