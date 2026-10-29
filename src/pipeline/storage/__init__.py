"""Storage package for database repositories and persistence sinks."""

from pipeline.storage.repository import PostgresSink, PostgresTradeRepository

__all__ = ["PostgresTradeRepository", "PostgresSink"]
