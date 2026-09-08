"""1초 수집 통계와 누적 전달 통계를 검증한다."""

import unittest

from src.monitoring.stats import StatsCollector


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class StatsCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.stats = StatsCollector(
            ("BTCUSDT", "ETHUSDT"),
            output_connected=True,
            clock=self.clock,
        )

    def test_reports_last_completed_second_with_matching_total(self) -> None:
        self.clock.value = 0.2
        self.stats.record_received({"event_type": "aggTrade", "symbol": "BTCUSDT"})
        self.stats.record_received({"event_type": "aggTrade", "symbol": "BTCUSDT"})
        self.clock.value = 0.9
        self.stats.record_received({"event_type": "aggTrade", "symbol": "ETHUSDT"})
        self.stats.record_received({"result": None, "id": 1})

        self.clock.value = 1.0
        snapshot = self.stats.snapshot()

        self.assertEqual(
            dict(snapshot.per_symbol_rate),
            {"BTCUSDT": 2, "ETHUSDT": 1},
        )
        self.assertEqual(snapshot.total_rate, 3)
        self.assertEqual(snapshot.cumulative_trades, 3)
        self.assertEqual(snapshot.control_or_unclassified, 1)

    def test_empty_completed_interval_is_zero_without_losing_cumulative(self) -> None:
        self.clock.value = 0.4
        self.stats.record_received({"event_type": "aggTrade", "symbol": "BTCUSDT"})
        self.clock.value = 2.0

        snapshot = self.stats.snapshot()

        self.assertEqual(snapshot.total_rate, 0)
        self.assertEqual(snapshot.cumulative_trades, 1)
        self.assertEqual(
            dict(snapshot.per_symbol_rate),
            {"BTCUSDT": 0, "ETHUSDT": 0},
        )

    def test_unknown_trade_is_control_and_duplicates_are_counted(self) -> None:
        event = {"event_type": "aggTrade", "symbol": "BTCUSDT", "a": 1}
        self.stats.record_received(event)
        self.stats.record_received(event)
        self.stats.record_received({"event_type": "aggTrade", "symbol": "UNKNOWN"})

        snapshot = self.stats.snapshot()

        self.assertEqual(snapshot.cumulative_trades, 2)
        self.assertEqual(snapshot.control_or_unclassified, 1)

    def test_forwarded_count_and_status_are_explicit(self) -> None:
        self.stats.record_forwarded()
        self.stats.set_connection_state("연결됨")
        self.stats.set_subscription_state("구독 확인")
        self.stats.record_error("sample error")

        snapshot = self.stats.snapshot()

        self.assertEqual(snapshot.forwarded_messages, 1)
        self.assertTrue(snapshot.output_connected)
        self.assertEqual(snapshot.connection_state, "연결됨")
        self.assertEqual(snapshot.subscription_state, "구독 확인")
        self.assertEqual(snapshot.recent_error, "sample error")

    def test_separates_queue_acceptance_from_confirmed_persistence(self) -> None:
        stats = StatsCollector(
            ("BTCUSDT",),
            output_connected=True,
            storage_name="MongoDB",
        )
        for _ in range(3):
            stats.record_forwarded()
        stats.record_persisted(2, 0.0125)
        stats.set_storage_state("적재 중")

        snapshot = stats.snapshot()

        self.assertEqual(snapshot.storage_name, "MongoDB")
        self.assertEqual(snapshot.storage_state, "적재 중")
        self.assertEqual(snapshot.forwarded_messages, 3)
        self.assertEqual(snapshot.persisted_messages, 2)
        self.assertEqual(snapshot.pending_messages, 1)
        self.assertEqual(snapshot.last_batch_size, 2)
        self.assertEqual(snapshot.last_batch_duration_ms, 12.5)


if __name__ == "__main__":
    unittest.main()
