"""Road start/finish kilometres added into monthly fuel distance."""

import logging

import pandas as pd
import pytest
from road_fuel_km import add_road_km_to_distance, monthly_road_km


def _row(**overrides):
    row = {
        "Date": "2026-03-15",
        "Worker id": 7,
        "Worker username": "ivan",
        "Worker nickname": "Иван",
        "Road start km": "0",
        "Road finish km": "10",
    }
    row.update(overrides)
    return row


def test_road_only_km_summed():
    df = pd.DataFrame(
        [
            _row(**{"Road start km": "100", "Road finish km": "120"}),
            _row(Date="2026-03-16", **{"Road start km": "0", "Road finish km": "15.5"}),
        ]
    )
    result = monthly_road_km(df)
    assert len(result) == 1
    assert result.iloc[0]["road_km"] == pytest.approx(35.5)


def test_negative_delta_counts_as_zero(caplog):
    df = pd.DataFrame([_row(**{"Road start km": "40", "Road finish km": "10"})])
    with caplog.at_level(logging.WARNING):
        result = monthly_road_km(df)
    total = 0.0 if result.empty else float(result["road_km"].sum())
    assert total == 0
    assert any(record.levelno == logging.WARNING for record in caplog.records)
    warning = " ".join(record.getMessage() for record in caplog.records)
    assert "2026-03-15" in warning
    assert "7" in warning


def test_missing_one_side_does_not_contribute():
    df = pd.DataFrame(
        [
            _row(**{"Road start km": "10", "Road finish km": "15"}),
            _row(Date="2026-03-16", **{"Road start km": "10", "Road finish km": ""}),
            _row(Date="2026-03-17", **{"Road start km": None, "Road finish km": "20"}),
            _row(Date="2026-03-18", **{"Road start km": "abc", "Road finish km": "30"}),
        ]
    )
    result = monthly_road_km(df)
    assert float(result["road_km"].sum()) == pytest.approx(5)


def test_comma_decimal_parses():
    df = pd.DataFrame([_row(**{"Road start km": "10,5", "Road finish km": "12,25"})])
    result = monthly_road_km(df)
    assert result.iloc[0]["road_km"] == pytest.approx(1.75)


def test_empty_or_missing_columns_returns_empty():
    assert monthly_road_km(None).empty
    assert monthly_road_km(pd.DataFrame()).empty
    assert monthly_road_km(pd.DataFrame({"Date": ["2026-03-01"]})).empty
    df = pd.DataFrame(
        {
            "Date": ["2026-03-01"],
            "Worker id": [1],
            "Worker username": ["a"],
            "Worker nickname": ["b"],
        }
    )
    assert monthly_road_km(df).empty


def test_groupby_keys_month_from_date():
    df = pd.DataFrame(
        [
            _row(Date="2026-01-15", **{"Road start km": "0", "Road finish km": "3"}),
            _row(Date="2026-01-31", **{"Road start km": "0", "Road finish km": "2"}),
            _row(
                Date="2026-02-01",
                **{
                    "Worker id": 8,
                    "Worker username": "anna",
                    "Worker nickname": "Анна",
                    "Road start km": "1",
                    "Road finish km": "4",
                },
            ),
        ]
    )
    result = monthly_road_km(df)
    assert list(result.columns) == [
        "Месяц",
        "Worker id",
        "Worker username",
        "Worker nickname",
        "road_km",
    ]
    january = result[result["Worker id"] == 7].iloc[0]
    assert january["Месяц"] == pd.Timestamp("2026-01-01")
    assert january["Worker username"] == "ivan"
    assert january["Worker nickname"] == "Иван"
    assert january["road_km"] == pytest.approx(5)
    february = result[result["Worker id"] == 8].iloc[0]
    assert february["Месяц"] == pd.Timestamp("2026-02-01")
    assert february["road_km"] == pytest.approx(3)


def test_empty_road_leaves_odometer_unchanged():
    odometer = pd.DataFrame(
        [
            {
                "Месяц": pd.Timestamp("2026-03-01"),
                "Worker id": 7,
                "Worker username": "ivan",
                "Worker nickname": "Иван",
                "distance": 42.5,
            }
        ]
    )
    result = add_road_km_to_distance(odometer, monthly_road_km(None))
    assert len(result) == 1
    assert result.iloc[0]["distance"] == pytest.approx(42.5)


def test_road_and_odometer_distances_are_summed():
    odometer = pd.DataFrame(
        [
            {
                "Месяц": pd.Timestamp("2026-03-01"),
                "Worker id": 7,
                "Worker username": "ivan",
                "Worker nickname": "Иван",
                "distance": 100.0,
            }
        ]
    )
    road = monthly_road_km(
        pd.DataFrame(
            [
                _row(**{"Road start km": "0", "Road finish km": "12"}),
                _row(
                    Date="2026-03-02",
                    **{
                        "Worker id": 9,
                        "Worker username": "only",
                        "Worker nickname": "road",
                        "Road start km": "1",
                        "Road finish km": "6",
                    },
                ),
            ]
        )
    )
    result = add_road_km_to_distance(odometer, road)
    ivan = result[result["Worker id"] == 7].iloc[0]
    assert ivan["distance"] == pytest.approx(112)
    road_only = result[result["Worker id"] == 9].iloc[0]
    assert road_only["distance"] == pytest.approx(5)
