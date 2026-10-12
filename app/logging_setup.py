"""Structured JSON logging: one line per event, always tagged with the instance id."""
import json
import logging
import sys
import time

from . import config


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
                  + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "instance": config.INSTANCE_ID,
            "event": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            data.update(extra)
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, default=str)


def setup() -> logging.Logger:
    log = logging.getLogger("cart")
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    log.addHandler(h)
    if config.LOG_FILE:
        fh = logging.FileHandler(config.LOG_FILE)
        fh.setFormatter(JsonFormatter())
        log.addHandler(fh)
    log.propagate = False
    return log


def event(log: logging.Logger, name: str, level: int = logging.INFO, **fields) -> None:
    log.log(level, name, extra={"fields": fields})
