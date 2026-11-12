from entrypoint import normalize_sheet_date


def test_serial_becomes_iso_date():
    assert normalize_sheet_date("46334") == "2026-11-08"
    assert normalize_sheet_date("46334.0") == "2026-11-08"
    assert normalize_sheet_date(46334) == "2026-11-08"


def test_iso_date_unchanged():
    assert normalize_sheet_date("2026-11-08") == "2026-11-08"


def test_empty_stays_empty():
    assert normalize_sheet_date("") == ""
    assert normalize_sheet_date(None) is None
