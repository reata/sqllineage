from sqllineage.runner import LineageRunner, main


def test_dummy():
    assert str(LineageRunner("select * from tab", verbose=True))
    assert main()
