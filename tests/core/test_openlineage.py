import json
import uuid
from datetime import datetime, timezone

import pytest

from sqllineage.cli import main
from sqllineage.openlineage import OpenLineageConverter
from sqllineage.runner import LineageRunner

RUN_ID = "0190e5c4-6a8d-7c3e-9f7a-1b2c3d4e5f60"
EVENT_TIME = "2024-01-01T00:00:00+00:00"
DATASET_NS = "postgres://db.example.com:5432"


def _event(sql: str, **kwargs):
    params = dict(
        job_namespace="etl",
        job_name="daily_job",
        dataset_namespace=DATASET_NS,
        run_id=RUN_ID,
        event_time=EVENT_TIME,
    )
    params.update(kwargs)
    return LineageRunner(sql).to_openlineage(**params)


def _names(datasets):
    return [d["name"] for d in datasets]


def test_simple_insert():
    sql = "INSERT INTO db1.tgt SELECT id, name FROM db2.src"
    event = _event(sql)
    for key in (
        "eventType",
        "eventTime",
        "producer",
        "schemaURL",
        "run",
        "job",
        "inputs",
        "outputs",
    ):
        assert key in event
    assert event["eventType"] == "COMPLETE"
    assert event["job"]["namespace"] == "etl"
    assert event["job"]["name"] == "daily_job"
    assert event["job"]["facets"]["sql"]["query"] == sql
    assert _names(event["inputs"]) == ["db2.src"]
    assert _names(event["outputs"]) == ["db1.tgt"]
    for dataset in event["inputs"] + event["outputs"]:
        assert dataset["namespace"] == DATASET_NS


def test_column_lineage():
    event = _event(
        "INSERT INTO db1.tgt SELECT s.id, s.name AS full_name, t.amount + 1 AS amt "
        "FROM db2.src s JOIN db2.txn t ON s.id = t.id"
    )
    facet = event["outputs"][0]["facets"]["columnLineage"]
    assert facet["_producer"] == event["producer"]
    assert "ColumnLineageDatasetFacet" in facet["_schemaURL"]
    assert facet["fields"] == {
        "amt": {
            "inputFields": [
                {"namespace": DATASET_NS, "name": "db2.txn", "field": "amount"}
            ]
        },
        "full_name": {
            "inputFields": [
                {"namespace": DATASET_NS, "name": "db2.src", "field": "name"}
            ]
        },
        "id": {
            "inputFields": [{"namespace": DATASET_NS, "name": "db2.src", "field": "id"}]
        },
    }


def test_column_lineage_multiple_input_fields_sorted():
    event = _event(
        "INSERT INTO db1.tgt SELECT concat(b.y, a.x) AS c FROM db2.b b JOIN db2.a a ON a.id = b.id"
    )
    fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
    assert fields["c"]["inputFields"] == [
        {"namespace": DATASET_NS, "name": "db2.a", "field": "x"},
        {"namespace": DATASET_NS, "name": "db2.b", "field": "y"},
    ]


def test_unrepresentable_columns_excluded():
    # `col` is ambiguous between two tables, `*` can't be expanded without metadata
    event = _event(
        "INSERT INTO db1.tgt SELECT a.x, col, b.* FROM db2.a a JOIN db2.b b ON a.id = b.id"
    )
    fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
    assert list(fields) == ["x"]
    # no representable column lineage at all: no facet
    event = _event("INSERT INTO db1.tgt SELECT * FROM db2.src")
    assert "columnLineage" not in event["outputs"][0]["facets"]


def test_multiple_input_tables():
    event = _event(
        "INSERT INTO db1.tgt SELECT a.id, b.v, c.w FROM db2.zeta a "
        "JOIN db2.alpha b ON a.id = b.id JOIN db3.mid c ON a.id = c.id"
    )
    assert _names(event["inputs"]) == ["db2.alpha", "db2.zeta", "db3.mid"]
    assert _names(event["outputs"]) == ["db1.tgt"]


def test_multiple_statements():
    event = _event(
        "INSERT INTO db1.mid SELECT x FROM db2.src;"
        "INSERT INTO db1.tgt SELECT x AS y FROM db1.mid;"
    )
    assert _names(event["inputs"]) == ["db2.src"]
    assert _names(event["outputs"]) == ["db1.tgt"]
    fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
    assert fields == {
        "y": {
            "inputFields": [{"namespace": DATASET_NS, "name": "db2.src", "field": "x"}]
        }
    }


def test_fixed_run_id_and_event_time():
    event = _event("INSERT INTO db1.tgt SELECT id FROM db2.src")
    assert event["run"]["runId"] == RUN_ID
    assert event["eventTime"] == EVENT_TIME
    event = _event(
        "INSERT INTO db1.tgt SELECT id FROM db2.src",
        run_id=uuid.UUID(RUN_ID),
        event_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    assert event["run"]["runId"] == RUN_ID
    assert event["eventTime"] == EVENT_TIME


def test_default_run_id_and_event_time():
    event = _event(
        "INSERT INTO db1.tgt SELECT id FROM db2.src", run_id=None, event_time=None
    )
    uuid.UUID(event["run"]["runId"])
    assert datetime.fromisoformat(event["eventTime"]).tzinfo is not None


def test_invalid_event_time():
    with pytest.raises(ValueError):
        _event("INSERT INTO db1.tgt SELECT id FROM db2.src", event_time="not a time")


def test_deterministic_ordering():
    sql = (
        "INSERT INTO db1.tgt SELECT c.z, a.y, b.x, concat(c.k, a.k, b.k) AS k "
        "FROM db2.c c JOIN db2.a a ON c.id = a.id JOIN db2.b b ON a.id = b.id"
    )
    event = _event(sql)
    assert _names(event["inputs"]) == ["db2.a", "db2.b", "db2.c"]
    fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
    assert list(fields) == ["k", "x", "y", "z"]
    assert [f["name"] for f in fields["k"]["inputFields"]] == [
        "db2.a",
        "db2.b",
        "db2.c",
    ]
    dumps = {json.dumps(_event(sql)) for _ in range(3)}
    assert len(dumps) == 1


def test_json_serialization():
    event = _event("INSERT INTO db1.tgt SELECT id FROM db2.src")
    assert json.loads(json.dumps(event)) == event


def test_converter_directly():
    runner = LineageRunner("INSERT INTO db1.tgt SELECT id FROM db2.src")
    event = OpenLineageConverter(
        runner, "etl", "job", DATASET_NS, run_id=RUN_ID, event_time=EVENT_TIME
    ).to_dict()
    assert event == runner.to_openlineage(
        "etl", "job", DATASET_NS, run_id=RUN_ID, event_time=EVENT_TIME
    )


def test_cli_openlineage(capsys):
    main(
        [
            "-e",
            "INSERT INTO db1.tgt SELECT id FROM db2.src",
            "--openlineage",
            "--job-namespace",
            "etl",
            "--job-name",
            "daily_job",
            "--dataset-namespace",
            DATASET_NS,
            "--run-id",
            RUN_ID,
            "--event-time",
            EVENT_TIME,
        ]
    )
    out = capsys.readouterr().out
    event = json.loads(out)
    assert "Source Tables" not in out
    assert event["run"]["runId"] == RUN_ID
    assert event["eventTime"] == EVENT_TIME
    assert event["job"]["namespace"] == "etl"
    assert event["job"]["name"] == "daily_job"
    assert _names(event["inputs"]) == ["db2.src"]
    assert _names(event["outputs"]) == ["db1.tgt"]
    assert event["inputs"][0]["namespace"] == DATASET_NS


def test_cli_openlineage_defaults_and_file(capsys, tmp_path):
    sql_file = tmp_path / "my_job.sql"
    sql_file.write_text("INSERT INTO db1.tgt SELECT id FROM db2.src")
    main(["-f", str(sql_file), "--openlineage", "-l", "column"])
    event = json.loads(capsys.readouterr().out)
    assert event["job"]["name"] == "my_job.sql"
    assert event["job"]["namespace"] == "sqllineage"
    assert event["outputs"][0]["namespace"] == "default"


def test_cli_without_openlineage_unchanged(capsys):
    main(["-e", "INSERT INTO db1.tgt SELECT id FROM db2.src"])
    out = capsys.readouterr().out
    assert "Source Tables" in out
    main(["-e", "INSERT INTO db1.tgt SELECT id FROM db2.src", "-l", "column"])
    assert capsys.readouterr().out.strip() == "db1.tgt.id <- db2.src.id"


def test_cli_openlineage_invalid_event_time():
    with pytest.raises(SystemExit) as e:
        main(["-e", "select 1", "--openlineage", "--event-time", "bad"])
    assert e.value.code == 2
