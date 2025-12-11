import os
from contextlib import contextmanager
from includes.reports import *

try:
    import psycopg2
    from psycopg2 import sql
except Exception as e:
    psycopg2 = None
    sql = None


class PostgresOps(object):
    """
    Create and manage tables in Postgres using the same schemas defined for BigQuery.
    - dataset maps to postgres schema
    - table names are created inside that schema
    """

    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.port = int(os.getenv("DB_PORT", "5432"))
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USERNAME")
        self.password = os.getenv("DB_PASSWORD")

        if psycopg2 is None:
            raise RuntimeError("psycopg2 is required for Postgres operations but was not imported.")

    @contextmanager
    def _conn(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _map_bq_type_to_pg(bq_type: str) -> str:
        mapping = {
            "STRING": "TEXT",
            "INT64": "BIGINT",
            "FLOAT64": "DOUBLE PRECISION",
            "TIMESTAMP": "TIMESTAMP",
            "BOOLEAN": "BOOLEAN",
        }
        return mapping.get(bq_type.upper(), "TEXT")

    def _ensure_schema(self, schema_name: str):
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(schema_name)
                    )
                )

    def create_table(self, tableName: str, tableConfig: TableConfig):
        """
        Create schema (if needed) and table with given columns.
        Optionally, create an index on partitionColumn if provided.
        """
        schema_name = tableConfig.getDataset()
        schema = tableConfig.getSchema()
        indexColumns = tableConfig.getIndexColumns()
        loadMethod = tableConfig.getLoadMethod()
        mergeColumns = tableConfig.getMergeColumns()

        table_name = tableName

        self._ensure_schema(schema_name)
        
        columns_sql = [sql.SQL("{} {}").format(sql.Identifier('id'), sql.SQL("BIGSERIAL"))]
        for col_name, bq_type in schema.items():
            if col_name == 'id':
                continue
            pg_type = self._map_bq_type_to_pg(bq_type)
            columns_sql.append(sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(pg_type)))

        create_table_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(columns_sql) if columns_sql else sql.SQL("id BIGSERIAL PRIMARY KEY")
        )

        with self._conn() as conn:
            with conn.cursor() as cur:
                print(f"Creating PG table {schema_name}.{table_name}")
                cur.execute(create_table_stmt)

                for indexColumn in indexColumns:
                    # Create index on the index column(s) single column or composite column
                    if isinstance(indexColumn, list):
                        # Composite index - multiple columns
                        index_cols = [sql.Identifier(col) for col in indexColumn]
                        index_name = f"{table_name}_{'_'.join(indexColumn)}_idx"
                        index_columns_sql = sql.SQL(", ").join(index_cols)
                    else:
                        # Single column index
                        index_name = f"{table_name}_{indexColumn}_idx"
                        index_columns_sql = sql.Identifier(indexColumn)
                    print(f"Creating index {index_name} on {schema_name}.{table_name} with columns {index_columns_sql}")
                    cur.execute(
                        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                            sql.Identifier(index_name),
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            index_columns_sql,
                        )
                    )

                # Create unique index on mergeColumns if loadMethod is 'merge'
                if loadMethod == 'merge' and mergeColumns:
                    merge_cols_sql = sql.SQL(", ").join([sql.Identifier(col) for col in mergeColumns])
                    unique_index_name = f"{table_name}__unique_idx"
                    print(f"Creating unique index {unique_index_name} on {schema_name}.{table_name} with columns {merge_cols_sql}")
                    cur.execute(
                        sql.SQL("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({})").format(
                            sql.Identifier(unique_index_name),
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            merge_cols_sql,
                        )
                    )

    def fix_table_schema(self, dataset: str, tableName: str, schema: dict):
        """
        Add any missing columns to match the provided schema.
        """
        schema_name = dataset
        table_name = tableName
        with self._conn() as conn:
            with conn.cursor() as cur:
                # fetch existing columns
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    """,
                    (schema_name, table_name),
                )
                existing_cols = {row[0] for row in cur.fetchall()}

                # Ensure id column exists
                if 'id' not in existing_cols:
                    print(f"Adding missing column id BIGSERIAL to {schema_name}.{table_name}")
                    cur.execute(
                        sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier('id'),
                            sql.SQL("BIGSERIAL"),
                        )
                    )

                for col_name, bq_type in schema.items():
                    if col_name not in existing_cols:
                        pg_type = self._map_bq_type_to_pg(bq_type)
                        print(f"Adding missing column {col_name} {pg_type} to {schema_name}.{table_name}")
                        cur.execute(
                            sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
                                sql.Identifier(schema_name),
                                sql.Identifier(table_name),
                                sql.Identifier(col_name),
                                sql.SQL(pg_type),
                            )
                        )

    def delete_table(self, table_name: str, dataset: str):
        schema_name = dataset
        with self._conn() as conn:
            with conn.cursor() as cur:
                print(f"Dropping PG table {schema_name}.{table_name} if exists")
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                    )
                )


