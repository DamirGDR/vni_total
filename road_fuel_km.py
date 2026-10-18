"""Monthly road kilometres from «График работ», added into fuel distance."""

from __future__ import annotations

import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)

GROUP_KEYS = ["Месяц", "Worker id", "Worker username", "Worker nickname"]
ROAD_KM_COLUMNS = GROUP_KEYS + ["road_km"]
_NEEDED = {
    "Date",
    "Worker id",
    "Worker username",
    "Worker nickname",
    "Road start km",
    "Road finish km",
}
_BLANK_KM = {"", "nan", "none", "nat", "<na>"}


def _empty_road_km() -> pd.DataFrame:
    return pd.DataFrame(columns=ROAD_KM_COLUMNS)


def _parse_km(series: pd.Series) -> pd.Series:
    text = series.map(lambda value: "" if value is None else str(value))
    text = text.str.strip().str.replace(",", ".", regex=False)
    text = text.mask(text.str.lower().isin(_BLANK_KM), pd.NA)
    return pd.to_numeric(text, errors="coerce")


def monthly_road_km(df: pd.DataFrame | None) -> pd.DataFrame:
    """Sum max(0, Road finish km − Road start km) by month and worker.

    A pair counts only when both sides parse to finite numbers.
    A negative delta is logged and contributes 0.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return _empty_road_km()
    if not _NEEDED.issubset(df.columns):
        return _empty_road_km()

    work = pd.DataFrame(
        {
            "Date": pd.to_datetime(df["Date"], errors="coerce"),
            "Worker id": pd.to_numeric(df["Worker id"], errors="coerce"),
            "Worker username": df["Worker username"],
            "Worker nickname": df["Worker nickname"],
            "start": _parse_km(df["Road start km"]),
            "finish": _parse_km(df["Road finish km"]),
        }
    )
    counted = work["start"].notna() & work["finish"].notna()
    work = work.loc[counted].copy()
    if work.empty:
        return _empty_road_km()
    finite = work["start"].map(math.isfinite) & work["finish"].map(math.isfinite)
    work = work.loc[finite].dropna(subset=["Date", "Worker id"]).copy()
    if work.empty:
        return _empty_road_km()

    delta = work["finish"] - work["start"]
    negative = delta < 0
    for date, worker_id in work.loc[negative, ["Date", "Worker id"]].itertuples(
        index=False
    ):
        logged_date = pd.Timestamp(date).date().isoformat()
        logged_worker = int(worker_id)
        logger.warning(
            "Negative road km delta; using 0 (Date=%s, Worker id=%s)",
            logged_date,
            logged_worker,
        )
    work["road_km"] = delta.clip(lower=0)
    work["Месяц"] = work["Date"].dt.to_period("M").dt.to_timestamp()
    work["Worker id"] = work["Worker id"].astype(int)
    grouped = work.groupby(GROUP_KEYS, as_index=False, dropna=False)["road_km"].sum()
    return grouped[ROAD_KM_COLUMNS]


def add_road_km_to_distance(
    odometer: pd.DataFrame | None,
    road: pd.DataFrame | None,
) -> pd.DataFrame:
    """Add monthly road km onto odometer ``distance``. Road-only workers stay."""
    keys = GROUP_KEYS
    distance_columns = keys + ["distance"]
    if road is None or road.empty or "road_km" not in road.columns:
        if odometer is None or odometer.empty:
            return pd.DataFrame(columns=distance_columns)
        out = odometer.copy()
        out["distance"] = pd.to_numeric(out["distance"], errors="coerce").fillna(0)
        return out

    road_part = road[keys + ["road_km"]]
    if odometer is None or odometer.empty:
        out = road_part.rename(columns={"road_km": "distance"})
        out["distance"] = pd.to_numeric(out["distance"], errors="coerce").fillna(0)
        return out[distance_columns]

    merged = odometer.merge(road_part, how="outer", on=keys)
    merged["distance"] = pd.to_numeric(merged["distance"], errors="coerce").fillna(0)
    merged["distance"] = merged["distance"] + pd.to_numeric(
        merged["road_km"], errors="coerce"
    ).fillna(0)
    return merged.drop(columns=["road_km"])
