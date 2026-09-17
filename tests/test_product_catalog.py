"""Spot과 USDⓈ-M 상품 확인 계약을 검증한다."""

import unittest

from src.collector.product_catalog import ProductCatalogError, validate_products


class ProductCatalogTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _fetcher(url: str, timeout: float) -> dict[str, object]:
        if "fapi" in url:
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                    },
                ]
            }
        return {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING"},
                {"symbol": "ETHUSDT", "status": "TRADING"},
            ]
        }

    async def test_accepts_symbols_available_in_both_markets(self) -> None:
        result = await validate_products(
            ("BTCUSDT", "ETHUSDT"),
            fetcher=self._fetcher,
        )

        self.assertEqual(result.spot_symbols, ("BTCUSDT", "ETHUSDT"))
        self.assertEqual(result.usdm_perpetual_symbols, ("BTCUSDT", "ETHUSDT"))

    async def test_reports_market_where_a_symbol_is_not_eligible(self) -> None:
        with self.assertRaisesRegex(ProductCatalogError, "USDⓈ-M.*ETHUSDT"):
            await validate_products(
                ("BTCUSDT", "ETHUSDT"),
                fetcher=lambda url, timeout: (
                    {"symbols": [{"symbol": "BTCUSDT", "status": "TRADING"}]}
                    if "fapi" in url
                    else self._fetcher(url, timeout)
                ),
            )


if __name__ == "__main__":
    unittest.main()
