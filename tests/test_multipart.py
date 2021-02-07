import pytest

from chairdb.multipart import MultipartParser

TEST = b'--a\r\nContent-Type: application/json\r\n\r\n{}\r\n--a--'


def run_parser(input):
    parser = MultipartParser('multipart/mixed; boundary="a"')
    for letter in input:
        # make sure there are no off-by-one bugs by feeding it a single letter
        # at the time.
        parser.feed(bytes([letter]))
    parser.check_done()
    return parser


def test_parser_normally():
    parser = run_parser(TEST)
    headers = {'Content-Type': 'application/json'}
    assert parser.results == [{
        'headers': headers,
        'body': b'{}',
        'done': True
    }]


def test_parsing_not_done():
    with pytest.raises(ValueError):
        run_parser(TEST[:-1])
