from __future__ import annotations
import os
from collections import defaultdict
from typing import Any, Sequence

import snowflake.connector.cursor
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
from snowflake.connector.constants import FIELD_TYPES


class SnowflakeCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur: snowflake.connector.cursor.SnowflakeCursor = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return list((i.name, FIELD_TYPES[i.type_code].name) for i in self.cur.description)


    def set_limit(self, limit: int) -> SnowflakeCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetch_arrow_all()
            else:
                ## No easy way to fetch with a limit
                return self.cur.fetch_arrow_all().slice(0, self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e

from functools import lru_cache
from typing import List, Callable
from dataclasses import dataclass, field
@dataclass
class LazyCatalogItem(CatalogItem):
    child_fetcher: Callable[[], List[CatalogItem]] = field(default_factory=lambda: list())
    children: List[CatalogItem] = field(default_factory=list())

    @property
    def children(self) -> List[CatalogItem]:
        return self.fetch()

    @children.setter
    def children(self, val):
        self.children = val

    @lru_cache()
    def fetch(self):
        return self.child_fetcher()

def get_schemas(db: str, cur: snowflake.connector.cursor.SnowflakeCursor) -> List[CatalogItem]:
    schs = cur.execute(f'show terse schemas in database "{db}"')
    cols = [i.name for i in schs.description]
    name = cols.index("name")
    return [LazyCatalogItem(
                        qualified_identifier=f'"{db}"."{schema[name]}"',
                        query_name=f'"{db}"."{schema[name]}"',
                        label=schema[name],
                        type_label="s",
                        child_fetcher=lambda: []) for schema in schs]
class SnowflakeConnection(HarlequinConnection):
    def __init__(
        self, account: str, username: str, password: str, role: str, warehouse: str, database: str, schema: str, hostname: str
    ) -> None:
        self.init_message = "Hello from Snowflake"
        try:
            print(account, username, password, role, warehouse, database, schema, hostname)
            self.conn =connect(user=username,
                                       password=password if password else os.environ['SNOWSQL_PWD'],
                                       account=account,
                                       #database=database,
                                       #host=hostname,
                                       #role=role,
                                       #warehouse=warehouse,
                               #schema=schema,
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
        cur = self.conn.cursor()
        databases = cur.execute("SHOW TERSE DATABASES")
        db_cols = [i.name for i in databases.description]
        db_items = []
        for db in databases.fetchall():
            if db[db_cols.index("kind")] == "APPLICATION":
                continue
            db_name = db[db_cols.index("name")]

            db_items.append(
                LazyCatalogItem(
                    qualified_identifier=f'"{db_name}"',
                    query_name=f'"{db_name}"',
                    label=db_name,
                    type_label="db",
                    child_fetcher=lambda : get_schemas(db_name, cur)
                )
            )
            # try:
            #     schemas = cur.execute(f'select table_catalog, table_schema, table_name, column_name, data_type from "{db_name}".information_schema.columns')
            # except:
            #     continue
            # schema_items = dict()
            # rel_items = defaultdict(dict)
            # for row in schemas:
            #     dbn, schema, table, col, dtype = row
            #     if table not in rel_items[schema]:
            #         rel_items[schema][table] = CatalogItem(
            #                 qualified_identifier=f'"{dbn}"."{schema}"."{table}"',
            #                 query_name=f'"{dbn}"."{schema}"."{table}"',
            #                 label=table,
            #                 type_label='t',
            #                 children=[],
            #             )
            #
            #     rel_items[schema][table].children.append(CatalogItem(
            #                 qualified_identifier=f'"{dbn}"."{schema}"."{table}"."{col}"',
            #                 query_name=f'"{col}"',
            #                 label=col,
            #                 type_label=dtype,
            #             ))
            #     if schema not in schema_items:
            #         schema_items[schema] = CatalogItem(
            #                 qualified_identifier=f'"{dbn}"."{schema}"',
            #                 query_name=f'"{dbn}"."{schema}"',
            #                 label=schema,
            #                 type_label="s",
            #                 children=[],
            #             )
            #
            # for schema, catitem in schema_items.items():
            #     rels = rel_items[schema]
            #     catitem.children.extend(rels.values())
            #     db_items[-1].children.append(catitem)

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

    def __init__(self, conn_str, account: str, username: str, password: str = None, role: str = None, database: str = None, schema: str = None, warehouse: str = None, hostname: str = None) -> None:
        self.account = account
        self.username = username
        self.password = password
        self.role = role
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.hostname = hostname

    def connect(self) -> SnowflakeConnection:
        conn = SnowflakeConnection(account=self.account, username=self.username, password=self.password, role=self.role, schema=self.schema, database=self.database, hostname=self.hostname, warehouse=self.warehouse)
        return conn
