import contextlib
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

    async def continue_parsing(self):
        self.parser.feed(await self.input.__anext__())

    async def __aiter__(self):
        keep_parsing = True
        while keep_parsing:
            # read more input
            try:
                await self.continue_parsing()
            except StopAsyncIteration:
                keep_parsing = False

            # process input
            for part in self.parser.results:
                yield Part(part, self)
            self.parser.results.clear()

        # make sure the input wasn't incomplete
        self.parser.check_done()


class Part:
    """Mimics the parts of httpx's stream response we use."""

    def __init__(self, info, parser):
        self.info = info
        self.parser = parser

    @property
    def headers(self):
        return self.info['headers']

    async def aread(self):
        while not self.info['done']:
            await self.parser.continue_parsing()
        return self.info['body']

    async def aiter_bytes(self):
        while True:
            yield bytes(self.info['body'])
            self.info['body'].clear()
            if self.info['done']:
                break
            await self.parser.continue_parsing()


class MultipartParser:
    """A multipart/mixed and multipart/related push parser compatible with
    CouchDB. No specification was consulted while writing this, so any
    deviations from the appropriate standards are most likely bugs.

    """
    class OutOfData(Exception):
        pass

    def __init__(self, content_type):
        match = re.match(MULTIPART_REGEX, content_type)
        assert match

        self.boundary = b'--' + match[1].encode('UTF-8')

        self.state = self.START
        self.cache = bytearray()
        self.results = []

    def feed(self, chunk):
        self.cache.extend(chunk)
        with contextlib.suppress(self.OutOfData):
            self.state()
            # missing data - wait until next call

    def check_done(self):
        if self.state != self.DONE:
            raise ValueError("Incomplete data!")

    def change_state(self, new_state):
        self.state = new_state
        self.state()

    def START(self):
        assert self._data_before(self.boundary) == b''
        self.change_state(self.READ_BOUNDARY_END)

    def READ_BOUNDARY_END(self):
        if self.cache[:2] == b'--':
            self.change_state(self.DONE)
        else:
            assert self._data_before(b'\r\n') == b''
            self.current_headers = {}
            self.change_state(self.READ_HEADERS)

    def _data_before(self, separator):
        try:
            before, self.cache = self.cache.split(separator, maxsplit=1)
        except ValueError:
            raise self.OutOfData()
        return before

    def READ_HEADERS(self):
        while True:
            line = self._data_before(b'\r\n')
            if line:
                key, value = line.decode('UTF-8').split(': ', maxsplit=1)
                self.current_headers[key] = value
            else:
                self.cur_result = {
                    'headers': self.current_headers,
                    'body': bytearray(),
                    'done': False
                }
                self.results.append(self.cur_result)
                self.change_state(self.READ_DOC)

    def READ_DOC(self):
        try:
            data = self._data_before(self.boundary)
        except self.OutOfData:
            # move input that cannot contain the boundary out of the way.
            self.cur_result['body'].extend(self.cache[:-len(self.boundary)])
            del self.cache[:-len(self.boundary)]
            # ... but re-raise, as we still need to find the boundary.
            raise
        self.cur_result['body'].extend(data)
        self.cur_result['done'] = True
        self.change_state(self.READ_BOUNDARY_END)

    def DONE(self):
        self.cache.clear()
