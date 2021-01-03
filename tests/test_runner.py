from sqllineage.runner import LineageRunner


def test_runner_dummy():
    assert str(
        LineageRunner(
            """insert into tab2 select col1, col2, col3, col4, col5, col6 from tab1;
insert overwrite table tab3 select * from tab2""",
            verbose=True,
        )
    )
