import fastjsonl


def test_count_newlines():
    data = b"line1\nline2\nline3\naaa\nsomething\n"

    assert fastjsonl.count_newlines.__name__ == "count_newlines"
    assert fastjsonl.count_newlines(data) == 5
