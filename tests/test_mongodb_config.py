"""로컬 MongoDB 설정의 기본값과 검증 규칙을 확인한다."""

import unittest

from src.config import (
    MULTI_COLLECTION_ENV_BY_ROUTE,
    ConfigurationError,
    load_mongo_config,
    load_multi_mongo_collection_config,
)
from src.storage_routes import StorageRoute


def multi_collection_environment() -> dict[str, str]:
    return {
        environment_name: f"deployed_{route.value}"
        for route, environment_name in MULTI_COLLECTION_ENV_BY_ROUTE.items()
    }


class MongoConfigTests(unittest.TestCase):
    def test_uses_local_defaults_without_environment_values(self) -> None:
        config = load_mongo_config(environ={})

        self.assertEqual(config.uri, "mongodb://localhost:27017")
        self.assertEqual(config.database, "studygroup")
        self.assertEqual(config.collection, "binance_events")
        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertIsNone(config.auth_source)

    def test_builds_encoded_uri_from_complete_credentials(self) -> None:
        config = load_mongo_config(
            environ={
                "DATALAKE_HOST": "127.0.0.1",
                "DATALAKE_PORT": "27018",
                "DATALAKE_USER": "study user",
                "DATALAKE_PASSWORD": "p@ss/word",
                "DATALAKE_DB_NAME": "market_data",
                "DATALAKE_COLLECTION_NAME": "agg_trades",
            }
        )

        self.assertEqual(
            config.uri,
            "mongodb://study+user:p%40ss%2Fword@127.0.0.1:27018/"
            "?authSource=market_data",
        )
        self.assertEqual(config.auth_source, "market_data")
        self.assertEqual(config.database, "market_data")
        self.assertEqual(config.collection, "agg_trades")

    def test_uses_explicit_encoded_auth_source(self) -> None:
        config = load_mongo_config(
            environ={
                "DATALAKE_USER": "study",
                "DATALAKE_PASSWORD": "secret",
                "DATALAKE_DB_NAME": "market_data",
                "DATALAKE_AUTH_SOURCE": "auth db",
            }
        )

        self.assertEqual(config.auth_source, "auth db")
        self.assertEqual(
            config.uri,
            "mongodb://study:secret@localhost:27017/?authSource=auth+db",
        )

    def test_rejects_non_numeric_or_out_of_range_port(self) -> None:
        for port in ("mongo", "0", "65536"):
            with self.subTest(port=port), self.assertRaises(ConfigurationError):
                load_mongo_config(environ={"DATALAKE_PORT": port})

    def test_rejects_partial_credentials(self) -> None:
        with self.assertRaises(ConfigurationError):
            load_mongo_config(environ={"DATALAKE_USER": "study"})

    def test_rejects_auth_source_without_credentials(self) -> None:
        with self.assertRaises(ConfigurationError):
            load_mongo_config(environ={"DATALAKE_AUTH_SOURCE": "admin"})


class MultiMongoCollectionConfigTests(unittest.TestCase):
    def test_loads_six_custom_collection_names_by_storage_route(self) -> None:
        config = load_multi_mongo_collection_config(
            environ=multi_collection_environment()
        )

        self.assertEqual(
            config.collection_name_by_route[StorageRoute.AGG_TRADE],
            "deployed_agg_trade",
        )
        self.assertEqual(
            config.collection_name_by_route[StorageRoute.CONTROL],
            "deployed_control",
        )
        self.assertEqual(len(config.collection_name_by_route), 6)

    def test_rejects_missing_or_blank_collection_name(self) -> None:
        environment = multi_collection_environment()
        missing_name = MULTI_COLLECTION_ENV_BY_ROUTE[StorageRoute.MARK_PRICE]
        environment.pop(missing_name)
        with self.assertRaisesRegex(ConfigurationError, missing_name):
            load_multi_mongo_collection_config(environ=environment)

        environment = multi_collection_environment()
        blank_name = MULTI_COLLECTION_ENV_BY_ROUTE[StorageRoute.CONTROL]
        environment[blank_name] = "   "
        with self.assertRaisesRegex(ConfigurationError, blank_name):
            load_multi_mongo_collection_config(environ=environment)

    def test_rejects_duplicate_physical_collection_names(self) -> None:
        environment = multi_collection_environment()
        environment[
            MULTI_COLLECTION_ENV_BY_ROUTE[StorageRoute.BOOK_TICKER]
        ] = environment[MULTI_COLLECTION_ENV_BY_ROUTE[StorageRoute.AGG_TRADE]]

        with self.assertRaisesRegex(ConfigurationError, "서로 달라야"):
            load_multi_mongo_collection_config(environ=environment)


if __name__ == "__main__":
    unittest.main()
