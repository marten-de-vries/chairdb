import ast
import copy
import astor


BASE_AST = ast.parse("""
def map(doc):
    if "test":
        yield "map_value"
""")


class PlaceholderReplacer(ast.NodeTransformer):
    def __init__(self, replacements):
        self.replacements = replacements

    def visit_Constant(self, node):
        return self.replacements[node.value]


def mango_index(fields, partial_filter_selector):
    assert len(fields) > 0

    base_ast = copy.deepcopy(BASE_AST)

    test = selector(partial_filter_selector)
    map_value = ast.Tuple([extractor(field) for field in fields], ast.Load())

    replacer = PlaceholderReplacer({'test': test, 'map_value': map_value})
    return ast.fix_missing_locations(replacer.visit(base_ast))


def selector(selector):
    # TODO: support full query language, with e.g. nested objects instead of
    # 'just' the a.b.c syntax
    for field, condition in selector.items():
        if isinstance(condition, str):
            condition = {'$eq', condition}
        assert len(condition) == 1

        base = extractor(field)
        for key, value in condition.items():
            comparator = {
                '$eq': ast.Eq,
                '$ne': ast.NotEq,
                '$lt': ast.Lt,
                '$gt': ast.Gt,
                '$lte': ast.LtE,
                '$gte': ast.GtE,
            }[key]()
            return ast.Compare(base, [comparator], [ast.Constant(value)])
    return ast.Constant(True)


def extractor(field):
    result = ast.Name('doc', ctx=ast.Load())
    for i, part in enumerate(field.split('.')):
        index = ast.Index(ast.Constant(part))
        result = ast.Subscript(result, index, ast.Load())
    return result


def map(doc):
    if doc['status'] != 'archived':
        yield (doc['type'],)


myast = mango_index(fields=["type", 'test.abc'], partial_filter_selector={
  "status": {
    "$ne": "archived"
  }
})

print(astor.to_source(myast))
globals = {}
exec(compile(myast, '', 'exec'), globals)
map_ = globals['map']

print(next(map_({'type': 1, 'status': 'active', 'test': {'abc': 2}})))
