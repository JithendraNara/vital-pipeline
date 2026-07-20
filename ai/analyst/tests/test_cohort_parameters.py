from __future__ import annotations

import duckdb
import pytest

import app as analyst_app


def _capture_queries(monkeypatch):
    calls: list[tuple[str, list[object]]] = []

    def fake_run_query(sql: str, parameters=None):
        calls.append((sql, list(parameters or [])))
        if "COUNT(DISTINCT person_id)" in sql:
            return [{"cohort_size": 1}]
        return [{"person_id": 1, "year_of_birth": 1980, "gender_concept_id": 8507}]

    monkeypatch.setattr(analyst_app, "run_query", fake_run_query)
    return calls


def test_quote_and_comment_payloads_are_bound_values(client, monkeypatch) -> None:
    calls = _capture_queries(monkeypatch)
    payloads = ["RX'); DROP TABLE patients; --", "IN' /* keep as data */"]

    response = client.post(
        "/cohort",
        json={
            "filters": {
                "drug_classes": [payloads[0]],
                "state": [payloads[1]],
            }
        },
    )

    assert response.status_code == 200
    assert len(calls) == 2
    for sql, parameters in calls:
        assert "?" in sql
        assert all(payload not in sql for payload in payloads)
        assert parameters == payloads
    assert all(payload not in response.json()["sql"] for payload in payloads)


def test_quote_and_comment_payloads_execute_as_data_in_duckdb(
    client, monkeypatch, tmp_path
) -> None:
    database = tmp_path / "synthetic.duckdb"
    con = duckdb.connect(str(database))
    con.execute("CREATE SCHEMA main_omop")
    con.execute("CREATE SCHEMA main_marts")
    con.execute(
        "CREATE TABLE main_omop.omcdm_person "
        "(person_id BIGINT, person_source_value VARCHAR, year_of_birth INTEGER, "
        "gender_concept_id BIGINT)"
    )
    con.execute(
        "CREATE TABLE main_omop.omcdm_drug_exposure "
        "(person_id BIGINT, drug_code_type VARCHAR)"
    )
    con.execute(
        "CREATE TABLE main_marts.mart_member_roster (member_id VARCHAR, state VARCHAR)"
    )
    con.execute("INSERT INTO main_omop.omcdm_person VALUES (1, 'synthetic-1', 1980, 8507)")
    con.close()
    monkeypatch.setattr(analyst_app, "DUCKDB_PATH", str(database))
    payloads = ["RX'); DROP TABLE patients; --", "IN' /* keep as data */"]

    response = client.post(
        "/cohort",
        json={"filters": {"drug_classes": [payloads[0]], "state": [payloads[1]]}},
    )

    assert response.status_code == 200
    assert response.json()["cohort_size"] == 0
    assert response.json()["sample"] == []
    assert all(payload not in response.json()["sql"] for payload in payloads)


def test_all_supported_filters_execute_in_order_against_duckdb(
    client, monkeypatch, tmp_path
) -> None:
    database = tmp_path / "full-filter-synthetic.duckdb"
    con = duckdb.connect(str(database))
    con.execute("CREATE SCHEMA main_omop")
    con.execute("CREATE SCHEMA main_marts")
    con.execute(
        "CREATE TABLE main_omop.omcdm_person "
        "(person_id BIGINT, person_source_value VARCHAR, year_of_birth INTEGER, "
        "gender_concept_id BIGINT)"
    )
    con.execute(
        "CREATE TABLE main_omop.omcdm_condition_occurrence "
        "(person_id BIGINT, ccs_category VARCHAR)"
    )
    con.execute(
        "CREATE TABLE main_omop.omcdm_visit_occurrence (person_id BIGINT)"
    )
    con.execute(
        "CREATE TABLE main_omop.omcdm_drug_exposure "
        "(person_id BIGINT, drug_code_type VARCHAR)"
    )
    con.execute(
        "CREATE TABLE main_omop.omcdm_measurement "
        "(person_id BIGINT, measurement_category VARCHAR)"
    )
    con.execute(
        "CREATE TABLE main_marts.mart_medication_adherence "
        "(person_id BIGINT, adherence_category VARCHAR, pdc_score DOUBLE)"
    )
    con.execute(
        "CREATE TABLE main_marts.mart_member_roster (member_id VARCHAR, state VARCHAR)"
    )
    con.execute("INSERT INTO main_omop.omcdm_person VALUES (1, 'synthetic-1', 1980, 8507)")
    con.execute("INSERT INTO main_omop.omcdm_condition_occurrence VALUES (1, 'circulatory')")
    con.execute("INSERT INTO main_omop.omcdm_visit_occurrence VALUES (1)")
    con.execute("INSERT INTO main_omop.omcdm_drug_exposure VALUES (1, 'RXNORM')")
    con.execute("INSERT INTO main_omop.omcdm_measurement VALUES (1, 'vitals_bp')")
    con.execute(
        "INSERT INTO main_marts.mart_medication_adherence "
        "VALUES (1, 'adherent', 0.9)"
    )
    con.execute("INSERT INTO main_marts.mart_member_roster VALUES ('synthetic-1', 'IN')")
    con.close()
    monkeypatch.setattr(analyst_app, "DUCKDB_PATH", str(database))

    response = client.post(
        "/cohort",
        json={
            "filters": {
                "min_age": 21,
                "max_age": 90,
                "gender_concept_id": 8507,
                "ccs_categories": ["circulatory"],
                "drug_classes": ["RXNORM"],
                "measurement_categories": ["vitals_bp"],
                "adherence_categories": ["adherent"],
                "min_pdc": 0.8,
                "min_visits": 1,
                "min_drugs": 1,
                "min_measurements": 1,
                "state": ["IN"],
            }
        },
    )

    assert response.status_code == 200
    assert response.json()["cohort_size"] == 1
    assert response.json()["sample"] == [
        {"person_id": 1, "year_of_birth": 1980, "gender_concept_id": 8507}
    ]


def test_every_supported_cohort_value_is_parameterized(client, monkeypatch) -> None:
    calls = _capture_queries(monkeypatch)
    filters = {
        "min_age": 21,
        "max_age": 89,
        "gender_concept_id": 8507,
        "ccs_categories": ["circulatory"],
        "drug_classes": ["RXNORM"],
        "measurement_categories": ["vitals_bp"],
        "adherence_categories": ["adherent"],
        "min_pdc": 0.8,
        "min_visits": 2,
        "min_drugs": 3,
        "min_measurements": 4,
        "state": ["IN"],
    }

    response = client.post("/cohort", json={"filters": filters})

    assert response.status_code == 200
    expected_parameters = [
        21,
        89,
        8507,
        "circulatory",
        "RXNORM",
        "vitals_bp",
        "adherent",
        0.8,
        2,
        3,
        4,
        "IN",
    ]
    assert len(calls) == 2
    assert calls[0][1] == expected_parameters
    assert calls[1][1] == expected_parameters


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("min_age", -1),
        ("min_age", 131),
        ("min_age", 1.5),
        ("max_age", -1),
        ("max_age", 131),
        ("max_age", 1.5),
        ("gender_concept_id", -1),
        ("gender_concept_id", 2_147_483_648),
        ("gender_concept_id", 10**100),
        ("gender_concept_id", 1.5),
        ("min_pdc", -0.01),
        ("min_pdc", 1.01),
        ("min_pdc", "not-a-number"),
        ("min_visits", -1),
        ("min_visits", 1_000_001),
        ("min_visits", 1.5),
        ("min_drugs", -1),
        ("min_drugs", 1_000_001),
        ("min_drugs", 1.5),
        ("min_measurements", -1),
        ("min_measurements", 1_000_001),
        ("min_measurements", 1.5),
    ],
)
def test_invalid_numeric_filters_return_422_not_500(client, field: str, value) -> None:
    response = client.post("/cohort", json={"filters": {field: value}})

    assert response.status_code == 422


@pytest.mark.parametrize(
    ("field", "values"),
    [
        ("min_age", [0, 130]),
        ("max_age", [0, 130]),
        ("gender_concept_id", [0, 2_147_483_647]),
        ("min_pdc", [0, 1]),
        ("min_visits", [0, 1_000_000]),
        ("min_drugs", [0, 1_000_000]),
        ("min_measurements", [0, 1_000_000]),
    ],
)
def test_numeric_boundary_values_are_accepted(
    client, monkeypatch, field: str, values: list[int]
) -> None:
    _capture_queries(monkeypatch)

    for value in values:
        response = client.post("/cohort", json={"filters": {field: value}})
        assert response.status_code == 200


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ccs_categories", ["not_a_ccs_category"]),
        ("measurement_categories", ["not_a_measurement_category"]),
        ("adherence_categories", ["not_an_adherence_category"]),
    ],
)
def test_known_category_lists_reject_unknown_values(client, field: str, value: list[str]) -> None:
    response = client.post("/cohort", json={"filters": {field: value}})

    assert response.status_code == 422


def test_bounded_lists_accept_limit_and_reject_over_limit(client, monkeypatch) -> None:
    calls = _capture_queries(monkeypatch)

    accepted = client.post(
        "/cohort",
        json={"filters": {"state": [f"state-{index}" for index in range(20)]}},
    )
    rejected = client.post(
        "/cohort",
        json={"filters": {"state": [f"state-{index}" for index in range(21)]}},
    )

    assert accepted.status_code == 200
    assert len(calls) == 2
    assert rejected.status_code == 422


def test_bounded_free_text_list_items_reject_oversized_values(client) -> None:
    response = client.post(
        "/cohort",
        json={"filters": {"drug_classes": ["x" * 65]}},
    )

    assert response.status_code == 422


def test_unknown_filter_keys_are_rejected(client) -> None:
    response = client.post(
        "/cohort",
        json={"filters": {"raw_sql": "SELECT * FROM patient"}},
    )

    assert response.status_code == 422


@pytest.mark.parametrize("schema", ["", "main.omop", "main; DROP SCHEMA", "two words"])
def test_invalid_schema_identifiers_fail_during_configuration(schema: str) -> None:
    with pytest.raises(ValueError, match="simple SQL identifier"):
        analyst_app._schema_name(schema)
