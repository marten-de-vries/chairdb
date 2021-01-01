from starlette.responses import JSONResponse

import json


class JSONResp(JSONResponse):
    def render(self, content):
        return super().render(content) + b'\n'


def parse_query_arg(request, name, default=None):
    try:
        json_or_string = request.query_params[name]
    except KeyError:
        return default
    try:
        return json.loads(json_or_string)
    except ValueError:
        return json_or_string


def as_json(item):
    return json.dumps(item, separators=(",", ":"), cls=SetEncoder)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super().default(obj)
