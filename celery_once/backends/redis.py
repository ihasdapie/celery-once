# -*- coding: utf-8 -*-

"""Definition of the redis locking backend."""

from __future__ import absolute_import


try:
    from urlparse import urlparse, parse_qsl
except:
    # Python 3!
    from urllib.parse import urlparse, parse_qsl

from celery_once.tasks import AlreadyQueued


def parse_url(url):
    """
    Parse the argument url and return a redis connection.
    Three patterns of url are supported:

        * redis://host:port[/db][?options]
        * redis+socket:///path/to/redis.sock[?options]
        * rediss://host:port[/db][?options]

    A ValueError is raised if the URL is not recognized.
    """
    parsed = urlparse(url)
    kwargs = parse_qsl(parsed.query)

    # TCP redis connection
    if parsed.scheme in ["redis", "rediss"]:
        details = {"host": parsed.hostname}
        if parsed.port:
            details["port"] = parsed.port
        if parsed.password:
            details["password"] = parsed.password
        db = parsed.path.lstrip("/")
        if db and db.isdigit():
            details["db"] = db
        if parsed.scheme == "rediss":
            details["ssl"] = True

    # Unix socket redis connection
    elif parsed.scheme == "redis+socket":
        details = {"unix_socket_path": parsed.path}
    else:
        raise ValueError("Unsupported protocol %s" % (parsed.scheme))

    # Add kwargs to the details and convert them to the appropriate type, if needed
    details.update(kwargs)
    if "socket_timeout" in details:
        details["socket_timeout"] = float(details["socket_timeout"])
    if "db" in details:
        details["db"] = int(details["db"])

    return details


redis = None

try:
    from redis.lock import Lock

except ImportError:
    raise ImportError(
        "You need to install the redis library in order to use Redis"
        " backend (pip install redis)"
    )


def get_redis(settings):
    global redis
    if not redis:
        try:
            from redis import StrictRedis
        except ImportError:
            raise ImportError(
                "You need to install the redis library in order to use Redis"
                " backend (pip install redis)"
            )
        redis = StrictRedis(**parse_url(settings["url"]))
    return redis


class Redis(object):
    """Redis locking backend."""

    def __init__(self, settings):
        self._redis = get_redis(settings)
        self.blocking_timeout = settings.get("blocking_timeout", 1)
        self.blocking = settings.get("blocking", False)

    @property
    def redis(self):
        # Used to allow easy mocking when testing.
        return self._redis

    def raise_or_lock(self, key, timeout):
        """
        Checks if the task is locked and raises an exception, else locks
        the task. By default, the tasks and the key expire after 60 minutes.
        (meaning it will not be executed and the lock will clear).
        """
        acquired = Lock(
            self.redis,
            key,
            timeout=timeout,
            blocking=self.blocking,
            blocking_timeout=self.blocking_timeout,
        ).acquire()

        if not acquired:
            # Time remaining in milliseconds
            # https://redis.io/commands/pttl
            ttl = self.redis.pttl(key)
            raise AlreadyQueued(ttl / 1000.0)

    def clear_lock(self, key):
        """Remove the lock from redis."""
        return self.redis.delete(key)

    def sema_inc(self, key, timeout):
        k = f"{key}:sema"
        self.raise_or_lock(k, timeout)
        self.sema_set(key, self.sema_get(key, timeout) + 1, timeout)
        self.clear_lock(k)

    def sema_dec(self, key, timeout):
        k = f"{key}:sema"
        self.raise_or_lock(k, timeout)
        self.sema_set(key, self.sema_get(key, timeout) - 1, timeout)
        self.clear_lock(k)

    def sema_get(self, key, timeout):
        k0 = f"{key}:0"
        k1 = f"{key}:1"

        e0 = self.redis.exists(k0)
        e1 = self.redis.exists(k1)

        x = sum(map(lambda x: x[1] << x[0], enumerate([e1, e0])))
        return x

    def sema_set(self, key, n, timeout):
        """
        An atomic counter up to 3 implemented as two locks
        """
        k0 = f"{key}:0"
        k1 = f"{key}:1"
        # 0, 0: 0
        # 0, 1: 1
        # 1, 0: 2
        # 1, 1: 3

        if n not in [0, 1, 2, 3]:
            raise ValueError("n must be 0, 1, 2 or 3")

        if n == 0:
            self.clear_lock(k0)
            self.clear_lock(k1)

        elif n == 1:
            self.raise_or_lock(k0, timeout)
            self.clear_lock(k1)

        elif n == 2:
            self.clear_lock(k0)
            self.raise_or_lock(k1, timeout)

        elif n == 3:
            self.raise_or_lock(k0, timeout)
            self.raise_or_lock(k1, timeout)
