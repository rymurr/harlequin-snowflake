from __future__ import annotations

from typing import Any, Sequence

from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_snowflake.cli_options import SNOWFLAKEADAPTER_OPTIONS
from snowflake.connector import connect


class SnowflakeCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        names = self.cur.column_names
        types = self.cur.column_types
        return list(zip(names, types))

    def set_limit(self, limit: int) -> SnowflakeCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e


class SnowflakeConnection(HarlequinConnection):
    def __init__(
        self, conn_str: Sequence[str], *args: Any, **kwargs: Any
    ) -> None:
        try:
            ## TODO set up config better
            self.conn =connect(user=kwargs['username'],
                                       password=os.environ['SNOWSQL_PWD'],
                                       account=kwargs['account'],
                                       database=kwargs['database'].upper(),
                                       host=kwargs['hostname'],
                                       role=kwargs['role'],
                                       warehouse=kwargs['warehouse']
                                       )

        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to snowflake."
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.cursor().execute(query)  # type: ignore
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur is not None:
                return SnowflakeCursor(cur)
            else:
                return None

    def get_catalog(self) -> Catalog:
        ## TODO double check this actually works
        databases = self.conn.cursor().execute("SHOW DATABASES")
        db_items: list[CatalogItem] = []
        for db in databases:
            schemas = self.conn.cursor().execute(f"SHOW SCHEMAS IN DATABASE {db}")
            schema_items: list[CatalogItem] = []
            for schema in schemas:
                ## do again for views
                relations = self.conn.cursor().execute(f'show tables in schema "{db}"."{schema}"')
                rel_items: list[CatalogItem] = []
                for rel, rel_type in relations:
                    ## todo columns... informations chema right?
                    cols = self.conn.list_columns_in_relation(rel)
                    col_items = [
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"."{col}"',
                            query_name=f'"{col}"',
                            label=col,
                            type_label=col_type,
                        )
                        for col, col_type in cols
                    ]
                    rel_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"',
                            query_name=f'"{db}"."{schema}"."{rel}"',
                            label=rel,
                            type_label=rel_type,
                            children=col_items,
                        )
                    )
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{schema}"',
                        query_name=f'"{db}"."{schema}"',
                        label=schema,
                        type_label="s",
                        children=rel_items,
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="db",
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)

    def get_completions(self) -> list[HarlequinCompletion]:
        ## TODO list of snowflake functions that I should include
        extra_keywords = []
        return [
            HarlequinCompletion(
                label=item, type_label="kw", value=item, priority=1000, context=None
            )
            for item in extra_keywords
        ]


class SnowflakeAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = SNOWFLAKEADAPTER_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> SnowflakeConnection:
        conn = SnowflakeConnection(self.conn_str, self.options)
        return conn
