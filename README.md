# Cronken
[![GitHub license](https://img.shields.io/github/license/Populi/cronken.svg)](https://github.com/Populi/cronken/blob/master/LICENSE)
[![PyPI version](https://img.shields.io/pypi/v/cronken)](https://pypi.org/project/cronken/)

![Cronken logo](cronken-logo.png)

## What is Cronken?
Cronken is a Redis-backed distributed cron system which allows running cronjobs across a set of servers without
risking double-running them on multiple boxes at once.  To facilitate this, it uses Redis locks to ensure that
only one server at a time runs any particular job.  It also uses Redis for all of its input and output,
reading its job list from Redis, storing results to Redis, and subscribing to a Redis pub/sub channel to allow for
centralized control of all cronken instances.

## Usage
A barebones example using a local Redis server can be found in [example_server.py](example_server.py), and a
slightly more sophisticated example that uses aiohttp to expose a web interface can be found in
[example_web.py](example_web.py).

## Data Format
A full description of the backend Redis data format can be found in [cronken_specification.md](cronken_specification.md).

