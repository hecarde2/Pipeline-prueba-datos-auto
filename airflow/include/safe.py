"""Conversiones seguras: cualquier valor raro produce un ValueError claro (nunca un crash opaco)."""
import math
from datetime import datetime, timezone


def to_float(v, field, required=False):
    if v is None or v == "":
        if required:
            raise ValueError(f"{field}: requerido")
        return None
    if isinstance(v, bool):
        raise ValueError(f"{field}: booleano no es numérico")
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise ValueError(f"{field}: no numérico ({str(v)[:30]!r})") from None
    if math.isnan(x) or math.isinf(x):
        raise ValueError(f"{field}: NaN/Inf")
    return x


def to_int(v, field, required=False):
    x = to_float(v, field, required)
    return None if x is None else int(x)


def to_text(v, max_len=500):
    if v is None:
        return None
    return str(v).replace("\x00", "")[:max_len]


def epoch_ms_to_dt(v, field):
    x = to_float(v, field, required=True)
    try:
        return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise ValueError(f"{field}: epoch fuera de rango ({v})") from None
