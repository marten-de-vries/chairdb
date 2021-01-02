import contextlib


class MultipartParser:
    class OutOfData(Exception):
        pass

    def __init__(self, boundary):
        self.boundary = b'--' + boundary

        self.state = self.START
        self.cache = bytearray()
        self.results = []

        self.current_headers = None
        self.current_data = None

    def feed(self, chunk):
        self.cache.extend(chunk)
        with contextlib.suppress(self.OutOfData):
            self.state()
            # missing data - wait until next call

    def check_done(self):
        if self.state != self.DONE:
            raise ValueError("Incomplete data!")
        self.state()

    def change_state(self, new_state):
        self.state = new_state
        self.state()

    def START(self):
        assert self._data_before(self.boundary) == b''
        self.change_state(self.READ_BOUNDARY_END)

    def READ_BOUNDARY_END(self):
        if self.cache[:2] == b'--':
            self.cache = self.cache[2:]
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
                self.current_data = bytearray()
                self.change_state(self.READ_DOC)

    def READ_DOC(self):
        try:
            data = self._data_before(self.boundary)
        except self.OutOfData:
            # move input that cannot contain the boundary out of the way.
            self.current_data.extend(self.cache[:-len(self.boundary)])
            del self.cache[:-len(self.boundary)]
            # ... but re-raise, as we still need to find the boundary.
            raise
        self.current_data.extend(data)
        self.results.append((self.current_headers, self.current_data))
        self.current_headers = None
        self.current_data = None
        self.change_state(self.READ_BOUNDARY_END)

    def DONE(self):
        if self.cache:
            raise ValueError("Unexpected data!")
