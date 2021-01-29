from chairdb import Document


def test_document():
    doc = Document('test', 1, ('a',), body={'x': 123})
    assert not doc.is_deleted
    assert doc['x'] == 123
