from sqllineage._compat import StrEnum


class Dialect(StrEnum):
    ANSI = "ansi"
    ATHENA = "athena"
    BIGQUERY = "bigquery"
    CLICKHOUSE = "clickhouse"
    DATABRICKS = "databricks"
    DB2 = "db2"
    DORIS = "doris"
    DUCKDB = "duckdb"
    EXASOL = "exasol"
    FLINK = "flink"
    GREENPLUM = "greenplum"
    HIVE = "hive"
    IMPALA = "impala"
    MARIADB = "mariadb"
    MATERIALIZE = "materialize"
    MYSQL = "mysql"
    NON_VALIDATING = "non-validating"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    SOQL = "soql"
    SPARKSQL = "sparksql"
    SQLITE = "sqlite"
    STARROCKS = "starrocks"
    TERADATA = "teradata"
    TRINO = "trino"
    TSQL = "tsql"
    VERTICA = "vertica"


class NodeTag:
    READ = "read"
    WRITE = "write"
    CTE = "cte"
    DROP = "drop"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    SELFLOOP = "selfloop"


class EdgeTag:
    INDEX = "index"


class EdgeType:
    LINEAGE = "lineage"
    RENAME = "rename"
    HAS_COLUMN = "has_column"
    HAS_ALIAS = "has_alias"


class EdgeDirection:
    IN = "in"
    OUT = "out"


class LineageLevel:
    TABLE = "table"
    COLUMN = "column"
