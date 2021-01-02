import pytest

from microcouch.multipart import MultipartParser

TEST = b'--a\r\nContent-Type: application/json\r\n\r\n{}\r\n--a--'


def run_parser(input):
    parser = MultipartParser(b'a')
    for letter in input:
        # make sure there are no off-by-one bugs by feeding it a single letter
        # at the time.
        parser.feed(bytes([letter]))
    parser.check_done()
    return parser


def test_parser_normally():
    parser = run_parser(TEST)
    headers = {'Content-Type': 'application/json'}
    assert parser.results == [(headers, b'{}\r\n')]


def test_parser_extra_data():
    with pytest.raises(ValueError):
        run_parser(TEST + b'\r\n')


def test_parsing_not_done():
    with pytest.raises(ValueError):
        run_parser(TEST[:-1])
