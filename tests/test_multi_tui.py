"""다중 스트림 TUI에 수신·연결·저장 상태가 함께 표시되는지 확인한다."""

import io
import unittest

from rich.console import Console

from src.monitoring.multi_stats import MultiStreamStatsCollector
from src.monitoring.multi_tui import MultiStreamTuiRenderer


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class MultiStreamTuiTests(unittest.TestCase):
    def test_renders_matrix_connections_and_collection_metrics(self) -> None:
        clock = FakeClock()
        stats = MultiStreamStatsCollector(("BTCUSDT", "ETHUSDT"), clock=clock)
        trade = {
            "meta": {
                "stream_type": "aggTrade",
                "connection_group": "spot_market",
                "connection_id": "c1",
                "received_at_ms": 1100,
            },
            "data": {"symbol": "BTCUSDT", "event_time": 1000},
        }
        stats.record_received(trade)
        stats.record_forwarded(trade)
        stats.record_batch_persisted("agg_trades", 1, 0.008)
        stats.record_buffer_changed("agg_trades", 0, 0, 0, 0)
        stats.record_connection_status("spot_market", "connection", "연결됨")
        stats.record_connection_status("spot_market", "subscription", "30개 구독 확인")
        stats.record_connection_status("spot_market", "data", "수신 중")
        stats.record_storage_state("agg_trades", "연결됨")
        clock.value = 1.0
        stream = io.StringIO()
        console = Console(
            file=stream,
            force_terminal=False,
            color_system=None,
            width=180,
        )

        console.print(MultiStreamTuiRenderer.render(stats.snapshot()))
        rendered = stream.getvalue()

        self.assertIn("BTCUSDT", rendered)
        self.assertIn("ETHUSDT", rendered)
        self.assertIn("aggTrade", rendered)
        self.assertIn("markPrice", rendered)
        self.assertIn("spot_market", rendered)
        self.assertIn("30개 구독 확인", rendered)
        self.assertIn("agg_trades", rendered)
        self.assertIn("적재 확인", rendered)
        self.assertIn("100ms", rendered)


if __name__ == "__main__":
    unittest.main()
