"""
Export sqllineage lineage result as an `OpenLineage <https://openlineage.io>`_ RunEvent.

The produced event is a plain, JSON-serializable dictionary, so no OpenLineage client
library is required. It can be posted to any OpenLineage-compatible backend
(e.g. Marquez) or emitted with the official ``openlineage-python`` client.
"""

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqllineage import VERSION
from sqllineage.core.models import Column, Path, Table

if TYPE_CHECKING:  # pragma: no cover
    from sqllineage.runner import LineageRunner

PRODUCER = f"https://github.com/reata/sqllineage/tree/v{VERSION}"
RUN_EVENT_SCHEMA_URL = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
)
SQL_JOB_FACET_SCHEMA_URL = (
    "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json#/$defs/SQLJobFacet"
)
COLUMN_LINEAGE_FACET_SCHEMA_URL = (
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
    "#/$defs/ColumnLineageDatasetFacet"
)
DEFAULT_EVENT_TYPE = "COMPLETE"


class OpenLineageConverter:
    """
    Convert the table and column lineage of a :class:`sqllineage.runner.LineageRunner`
    into an OpenLineage RunEvent dictionary.
    """

    def __init__(
        self,
        runner: "LineageRunner",
        job_namespace: str,
        job_name: str,
        dataset_namespace: str,
        run_id: str | uuid.UUID | None = None,
        event_time: str | datetime | None = None,
    ):
        """
        :param runner: the :class:`sqllineage.runner.LineageRunner` to convert
        :param job_namespace: OpenLineage namespace of the job
        :param job_name: OpenLineage name of the job
        :param dataset_namespace: OpenLineage namespace used for every dataset
        :param run_id: run id, should be a UUID per OpenLineage spec. A random UUID is
               generated when not supplied.
        :param event_time: event time, as a :class:`datetime.datetime` or an ISO-8601
               string. Current UTC time is used when not supplied.
        """
        self._runner = runner
        self._job_namespace = job_namespace
        self._job_name = job_name
        self._dataset_namespace = dataset_namespace
        self._run_id = self._normalize_run_id(run_id)
        self._event_time = self._normalize_event_time(event_time)

    @staticmethod
    def _normalize_run_id(run_id: str | uuid.UUID | None) -> str:
        if run_id is None:
            return str(uuid.uuid4())
        return str(run_id)

    @staticmethod
    def _normalize_event_time(event_time: str | datetime | None) -> str:
        if event_time is None:
            event_time = datetime.now(timezone.utc)
        if isinstance(event_time, datetime):
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)
            return event_time.isoformat()
        try:
            datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(
                f"event_time must be an ISO-8601 datetime string, got: {event_time!r}"
            ) from e
        return event_time

    @staticmethod
    def dataset_name(dataset: Table | Path) -> str:
        """
        OpenLineage dataset name of a sqllineage Table or Path, consistent with
        sqllineage's own naming, e.g. ``schema.table``. Unqualified tables use
        ``<default>`` as schema unless ``DEFAULT_SCHEMA`` is configured.
        """
        return str(dataset)

    @staticmethod
    def _is_valid_field(column: Column) -> bool:
        # unexpanded wildcard can't be represented as an OpenLineage field,
        # and ambiguous/unknown columns have no single resolvable parent dataset
        return column.raw_name != "*" and isinstance(column.parent, (Table, Path))

    def _dataset(self, dataset: Table | Path) -> dict[str, Any]:
        return {
            "namespace": self._dataset_namespace,
            "name": self.dataset_name(dataset),
            "facets": {},
        }

    def _column_lineage(self) -> dict[str, dict[str, set[tuple[str, str, str]]]]:
        """
        mapping of target dataset name -> target column -> set of
        (source namespace, source dataset name, source column)
        """
        targets = {self.dataset_name(t) for t in self._runner.target_tables}
        lineage: dict[str, dict[str, set[tuple[str, str, str]]]] = {}
        for path in self._runner.get_column_lineage():
            src_col, tgt_col = path[0], path[-1]
            if not (self._is_valid_field(src_col) and self._is_valid_field(tgt_col)):
                continue
            tgt_name = self.dataset_name(tgt_col.parent)
            if tgt_name not in targets:
                continue
            lineage.setdefault(tgt_name, {}).setdefault(tgt_col.raw_name, set()).add(
                (
                    self._dataset_namespace,
                    self.dataset_name(src_col.parent),
                    src_col.raw_name,
                )
            )
        return lineage

    def to_dict(self) -> dict[str, Any]:
        """
        :return: a JSON-serializable OpenLineage RunEvent dictionary
        """
        inputs = sorted(
            (self._dataset(t) for t in self._runner.source_tables),
            key=lambda d: (d["namespace"], d["name"]),
        )
        outputs = sorted(
            (self._dataset(t) for t in self._runner.target_tables),
            key=lambda d: (d["namespace"], d["name"]),
        )
        column_lineage = self._column_lineage()
        for output in outputs:
            fields = column_lineage.get(output["name"])
            if not fields:
                continue
            output["facets"]["columnLineage"] = {
                "_producer": PRODUCER,
                "_schemaURL": COLUMN_LINEAGE_FACET_SCHEMA_URL,
                "fields": {
                    field: {
                        "inputFields": [
                            {"namespace": ns, "name": name, "field": col}
                            for ns, name, col in sorted(input_fields)
                        ]
                    }
                    for field, input_fields in sorted(fields.items())
                },
            }
        return {
            "eventType": DEFAULT_EVENT_TYPE,
            "eventTime": self._event_time,
            "producer": PRODUCER,
            "schemaURL": RUN_EVENT_SCHEMA_URL,
            "run": {"runId": self._run_id, "facets": {}},
            "job": {
                "namespace": self._job_namespace,
                "name": self._job_name,
                "facets": {
                    "sql": {
                        "_producer": PRODUCER,
                        "_schemaURL": SQL_JOB_FACET_SCHEMA_URL,
                        "query": self._runner._sql,
                    }
                },
            },
            "inputs": inputs,
            "outputs": outputs,
        }

    def convert(self) -> dict[str, Any]:
        """
        alias of :meth:`to_dict`
        """
        return self.to_dict()
