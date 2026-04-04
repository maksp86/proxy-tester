from app.models import CandidateProxy, SpeedTestResult, UrlTestResult
from app.pipeline import _collect_speed_stage_rows, _collect_url_stage_rows


def test_collect_url_stage_rows_splits_ok_and_dead() -> None:
    candidates = {
        "ok": CandidateProxy(proxy_hash="ok", raw_link="vmess://ok", scheme="vmess"),
        "bad": CandidateProxy(proxy_hash="bad", raw_link="vmess://bad", scheme="vmess"),
    }

    url_results = [
        UrlTestResult(proxy_hash="ok", success=True, latency_ms=11.5, exit_ip="1.1.1.1", country="US", city="NY"),
        UrlTestResult(proxy_hash="bad", success=False, reason="timeout"),
    ]

    ok_rows, dead_rows = _collect_url_stage_rows(url_results, candidates)

    assert len(ok_rows) == 1
    assert ok_rows[0]["proxy_hash"] == "ok"
    assert dead_rows == [("bad", "timeout")]


def test_collect_speed_stage_rows_keeps_only_fast_and_successful() -> None:
    top_for_speed = [
        {"proxy_hash": "good", "raw_link": "vmess://good", "latency_ms": 10.0},
        {"proxy_hash": "slow", "raw_link": "vmess://slow", "latency_ms": 20.0},
        {"proxy_hash": "fail", "raw_link": "vmess://fail", "latency_ms": 30.0},
    ]

    speed_by_hash = {
        "good": SpeedTestResult(proxy_hash="good", success=True, mbps=15.0),
        "slow": SpeedTestResult(proxy_hash="slow", success=True, mbps=0.5),
        "fail": SpeedTestResult(proxy_hash="fail", success=False, reason="speed_test_failed"),
    }

    final_rows, dead_rows = _collect_speed_stage_rows(
        top_for_speed=top_for_speed,
        speed_by_hash=speed_by_hash,
        speed_min_mb_s=1.0,
        target_final_count=25,
    )

    assert [row["proxy_hash"] for row in final_rows] == ["good"]
    assert ("slow", "below_speed_threshold") in dead_rows
    assert ("fail", "speed_test_failed") in dead_rows


def test_collect_speed_stage_rows_empty_if_no_speed_success() -> None:
    top_for_speed = [{"proxy_hash": "a", "raw_link": "vmess://a", "latency_ms": 1.0}]
    speed_by_hash = {"a": SpeedTestResult(proxy_hash="a", success=False, reason="nope")}

    final_rows, dead_rows = _collect_speed_stage_rows(
        top_for_speed=top_for_speed,
        speed_by_hash=speed_by_hash,
        speed_min_mb_s=1.0,
        target_final_count=25,
    )

    assert final_rows == []
    assert dead_rows == [("a", "nope")]
