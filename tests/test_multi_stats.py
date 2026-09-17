"""다중 스트림 통계와 depth 연속성 관측을 검증한다."""

import unittest
from typing import Any

from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.storage_routes import StorageRoute


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


def event(
    stream_type: str,
    *,
    symbol: str = "BTCUSDT",
    connection_id: str = "connection-1",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {"symbol": symbol, **(data or {})}
    return {
        "meta": {
            "stream_type": stream_type,
            "connection_group": "spot_depth" if stream_type == "depth" else "spot_market",
            "connection_id": connection_id,
            "received_at_ms": 1_100,
        },
        "data": payload,
    }


class MultiStreamStatsTests(unittest.TestCase):
    def test_reports_symbol_stream_matrix_for_last_completed_second(self) -> None:
        clock = FakeClock()
        stats = MultiStreamStatsCollector(("BTCUSDT", "ETHUSDT"), clock=clock)
        stats.record_received(event("aggTrade"))
        stats.record_received(event("aggTrade"))
        stats.record_received(event("bookTicker", symbol="ETHUSDT"))
        clock.value = 1.0

        snapshot = stats.snapshot()

        self.assertEqual(snapshot.per_symbol_stream_rate["BTCUSDT"]["aggTrade"], 2)
        self.assertEqual(snapshot.per_symbol_stream_rate["ETHUSDT"]["bookTicker"], 1)
        self.assertEqual(snapshot.per_symbol_stream_rate["BTCUSDT"]["markPrice"], 0)
        self.assertEqual(snapshot.total_rate, 3)
        self.assertEqual(snapshot.cumulative_by_stream["aggTrade"], 2)

    def test_separates_collection_acceptance_persistence_and_buffer(self) -> None:
        stats = MultiStreamStatsCollector(("BTCUSDT",))
        trade = event("aggTrade")
        control = {"meta": {"stream_type": "control"}, "data": {"id": 1}}

        stats.record_received(trade)
        stats.record_received(control)
        stats.record_forwarded(trade)
        stats.record_forwarded(control)
        stats.record_buffer_changed(StorageRoute.AGG_TRADE, 1, 240, 2, 400)
        stats.record_batch_persisted(StorageRoute.AGG_TRADE, 1, 0.008)
        stats.record_storage_state(StorageRoute.AGG_TRADE, "연결됨")
        snapshot = stats.snapshot()

        self.assertEqual(snapshot.control_or_unclassified, 1)
        self.assertEqual(snapshot.forwarded_messages, 2)
        trade_stats = snapshot.collections[StorageRoute.AGG_TRADE]
        self.assertEqual(trade_stats.accepted, 1)
        self.assertEqual(trade_stats.persisted, 1)
        self.assertEqual(trade_stats.pending, 1)
        self.assertEqual(trade_stats.pending_bytes, 240)
        self.assertEqual(trade_stats.last_batch_duration_ms, 8)
        self.assertEqual(snapshot.collections[StorageRoute.CONTROL].accepted, 1)

    def test_depth_observer_reports_gap_without_rejecting_overlap_or_duplicate(self) -> None:
        stats = MultiStreamStatsCollector(("BTCUSDT",))
        stats.record_received(
            event("depth", data={"first_update_id": 10, "final_update_id": 12})
        )
        stats.record_received(
            event("depth", data={"first_update_id": 11, "final_update_id": 13})
        )
        stats.record_received(
            event("depth", data={"first_update_id": 15, "final_update_id": 16})
        )
        stats.record_received(
            event(
                "depth",
                connection_id="connection-2",
                data={"first_update_id": 30, "final_update_id": 31},
            )
        )

        snapshot = stats.snapshot()

        self.assertEqual(snapshot.depth_gap_count, 1)
        self.assertEqual(snapshot.depth_connection_resets, 1)
        assert snapshot.last_depth_gap is not None
        self.assertEqual(snapshot.last_depth_gap.previous_final_update_id, 13)
        self.assertEqual(snapshot.last_depth_gap.next_first_update_id, 15)

    def test_connection_status_and_event_time_difference_are_visible(self) -> None:
        clock = FakeClock()
        stats = MultiStreamStatsCollector(("BTCUSDT",), clock=clock)
        stats.record_connection_status("spot_market", "connection", "연결됨")
        stats.record_connection_status("spot_market", "subscription", "30개 구독 확인")
        stats.record_connection_status("spot_market", "data", "수신 중")
        stats.record_received(event("aggTrade", data={"event_time": 1_000}))
        clock.value = 2.5

        snapshot = stats.snapshot()

        self.assertEqual(snapshot.connections["spot_market"].connection, "연결됨")
        self.assertEqual(snapshot.connections["spot_market"].last_data_age_seconds, 2.5)
        self.assertEqual(snapshot.latest_latency_ms["aggTrade"], 100)


if __name__ == "__main__":
    unittest.main()
