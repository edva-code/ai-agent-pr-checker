import importlib.util
from pathlib import Path

path = Path(__file__).parents[1] / "transform/models/marts/capture_monthly_volume.py"
spec = importlib.util.spec_from_file_location("capture_monthly_volume", path)
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


def test_flagged_months_stay_out_of_baseline_and_sources_reset():
    rows = [
        ("gharchive", 1, 100),
        ("gharchive", 2, 100),
        ("gharchive", 3, 100),
        ("gharchive", 4, 50),  # drop
        ("gharchive", 5, 60),  # still low: baseline must not adapt to month 4
        ("gharchive", 6, 100),  # recovered
        ("opendigger", 6, 1000),  # new source, no baseline yet
        ("opendigger", 7, 900),
    ]
    out = capture.flag_months(rows)
    assert [r["volume_low"] for r in out] == [False, False, False, True, True, False, False, False]
    assert out[4]["baseline"] == 100
    assert out[6]["baseline"] is None
    assert out[7]["baseline"] == 1000
