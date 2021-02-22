import anyio
import re

MULTIPART_REGEX = 'multipart/(?:mixed|related); boundary="?([^"$]+)"?$'


class MultipartStreamParser:
    """Makes it easy to parse a httpx stream multipart response in async
    fashion. It takes the response as constructor argument, and when (async)
    iterated over it gives you Part-s.

    Also compatible with a Starlette request.

    """
    def __init__(self, stream):
        try:
            self.input = stream.aiter_bytes()
        except AttributeError:
            self.input = stream.stream()
        self.parser = MultipartParser(stream.headers['Content-Type'])
        self.cache = []
        self.parsing_paused_event = None

    async def __aiter__(self):
        while self.parser.state != self.parser.DONE:
            await self.continue_parsing()
            for part in self.cache:
                yield part
            self.cache.clear()

    async def continue_parsing(self):
        if self.parsing_paused_event:
            # join waiting for the current parse results
            return await self.parsing_paused_event.wait()
        self.parsing_paused_event = anyio.create_event()

        chunk = await self.input.__anext__()
        for args in self.parser.feed(chunk):
            self._process_parsed(*args)

        self.parsing_paused_event.set()
        self.parsing_paused_event = None

    def _process_parsed(self, type, *args):  # noqa: C901
        if type == 'start':
            self.part = Part(self)
        elif type == 'header':
            key, value = args
            self.part.headers[key] = value
        elif type == 'headers_done':
            self.cache.append(self.part)
        elif type == 'chunk':
            self.part.cache.append(args[0])
        elif type == 'body_done':
            self.part.done = True
        else:  # pragma: no cover
            raise ValueError(f"Unknown type: {type}")


class Part:
    """Mimics the parts of httpx's stream response we use."""

    def __init__(self, parser):
        self.parser = parser
        self.headers = {}
        self.done = False
        self.cache = []

    async def aread(self):
        return b''.join([chunk async for chunk in self.aiter_bytes()])

    async def aiter_bytes(self):
        while True:
            for chunk in self.cache:
                yield chunk
            self.cache.clear()
            if self.done:
                break
            await self.parser.continue_parsing()


class MultipartParser:
    """A multipart/mixed and multipart/related push parser compatible with
    CouchDB. No specification was consulted while writing this, so any
    deviations from the appropriate standards are most likely bugs.

    ``feed()`` can be used to push data. It's a generator function that yields
    the following tokens (example values are given for the argument slots):

    - ('start',)
    - ('header', 'Content-Type', 'application/json')
    - ('headers_done')
    - ('chunk', b'{"hello": "world!"}')
    - ('body_done',)

    Don't forget to check the ``parser.state == parser.DONE`` invariant holds
    after you pushed in the final data using ``feed()``!

    """
    def __init__(self, content_type):
        match = re.match(MULTIPART_REGEX, content_type)
        assert match

        self.boundary = b'\r\n--' + match[1].encode('UTF-8')

        self.state = self.START
        self.cache = bytearray()

    def feed(self, chunk):
        self.cache.extend(chunk)
        yield from self.state()

    def change_state(self, new_state):
        self.state = new_state
        yield from self.state()

    def START(self):
        # at start there's no \r\n in the boundary
        if self._data_before(self.boundary[2:]) == b'':
            yield from self.change_state(self.READ_BOUNDARY_END)

    def READ_BOUNDARY_END(self):
        if self.cache[:2] == b'--':
            yield from self.change_state(self.DONE)
        elif self._data_before(b'\r\n') == b'':
            yield 'start',
            yield from self.change_state(self.READ_HEADERS)

    def _data_before(self, separator):
        try:
            before, self.cache = self.cache.split(separator, maxsplit=1)
        except ValueError:
            before = None
        return before

    def READ_HEADERS(self):
        line = self._data_before(b'\r\n')
        if line:
            key, value = line.decode('UTF-8').split(': ', maxsplit=1)
            yield 'header', key, value
            yield from self.state()
        elif line == b'':
            yield 'headers_done',
            yield from self.change_state(self.READ_DOC)

    def READ_DOC(self):
        done = True
        data = self._data_before(self.boundary)
        if data is None:
            done = False
            # move input that cannot contain the boundary out of the way.
            data = self.cache[:-len(self.boundary)]
            del self.cache[:-len(self.boundary)]
        if data:
            yield 'chunk', bytes(data)
        if done:
            yield 'body_done',
            yield from self.change_state(self.READ_BOUNDARY_END)

    def DONE(self):
        self.cache.clear()
        yield from []
