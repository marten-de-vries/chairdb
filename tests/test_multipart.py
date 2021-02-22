import pytest

from chairdb.multipart import MultipartParser

TEST = b'--a\r\nContent-Type: application/json\r\n\r\n{}\r\n--a--'


def run_parser(input):
    parser = MultipartParser('multipart/mixed; boundary="a"')
    for letter in input:
        # make sure there are no off-by-one bugs by feeding it a single letter
        # at the time.
        yield from parser.feed(bytes([letter]))
    assert parser.state == parser.DONE


def test_parser_normally():
    assert list(run_parser(TEST)) == [
        ('start',),
        ('header', 'Content-Type', 'application/json'),
        ('headers_done',),
        ('chunk', b'{'),
        ('chunk', b'}'),
        ('body_done',),
    ]


def test_parsing_not_done():
    with pytest.raises(AssertionError):
        list(run_parser(TEST[:-1]))
