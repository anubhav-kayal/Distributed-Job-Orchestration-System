from datetime import datetime

from cronsim import CronSim, CronSimError


class InvalidCron(ValueError):
    pass


def next_fire_after(cron_expr: str, after: datetime) -> datetime:
    """The first time strictly after `after` that `cron_expr` (evaluated in UTC) fires."""
    try:
        return next(CronSim(cron_expr, after))
    except CronSimError as exc:
        raise InvalidCron(f"invalid cron expression {cron_expr!r}: {exc}") from None
