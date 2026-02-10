from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PersonalizationParams:
    am: int
    h: int
    d: int
    k: int
    L: int = 4
    hours_set: frozenset[int] = field(init=False)
    allowed_2024_pickup_dates: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        hours = frozenset(((self.h + i) % 24) for i in range(int(self.L)))
        object.__setattr__(self, "hours_set", hours)

        d0 = int(self.d)
        d1 = min(d0 + 1, 28)
        d2 = min(d0 + 2, 28)
        doms = sorted(set([d0, d1, d2]))

        dates = tuple(f"2024-{m:02d}-{day:02d}" for m in range(1, 13) for day in doms)
        object.__setattr__(self, "allowed_2024_pickup_dates", dates)


def params_from_am(am: int) -> PersonalizationParams:
    a = int(am)
    h = a % 24
    d = (a % 25) + 1
    k = (a % 11) + 10
    return PersonalizationParams(am=a, h=h, d=d, k=k)


def hour_window_set(h: int, L: int = 4) -> frozenset[int]:
    h0 = int(h) % 24
    return frozenset(((h0 + i) % 24) for i in range(int(L)))


def hour_in_window(hour: int, *, h: int, L: int = 4) -> bool:
    hh = int(hour)
    if hh < 0 or hh > 23:
        return False
    return hh in hour_window_set(h, L)


def filter_2015(trips_2015_df, params: PersonalizationParams):
    from pyspark.sql import functions as F

    hours = sorted(params.hours_set)
    return trips_2015_df.where(F.hour("pickup_datetime").isin(hours))


def _ensure_pickup_date_str(df):
    from pyspark.sql import functions as F

    if "pickup_date" in df.columns:
        return df
    return df.withColumn("pickup_date", F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd"))


def filter_2024(trips_2024_df, params: PersonalizationParams):
    from pyspark.sql import functions as F

    df = _ensure_pickup_date_str(trips_2024_df)
    hours = sorted(params.hours_set)
    dates = list(params.allowed_2024_pickup_dates)
    return df.where(F.col("pickup_date").isin(dates) & F.hour("pickup_datetime").isin(hours))

