import asyncio

import aiohttp
import async_timeout
from aiohttp import ClientTimeout
from aiohttp.client_exceptions import ServerFingerprintMismatch, ClientError

from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, SSLError
from elasticsearch.connection import Connection
from elasticsearch.compat import urlencode


class AIOHttpConnection(Connection):
    def __init__(self, host='localhost', port=9200, http_auth=None,
            use_ssl=False, verify_certs=False, ca_certs=None, client_cert=None,
            client_key=None, loop=None, use_dns_cache=True, headers=None, **kwargs):
        super().__init__(host=host, port=port, **kwargs)

        self.loop = asyncio.get_event_loop() if loop is None else loop

        if http_auth is not None:
            if isinstance(http_auth, str):
                http_auth = tuple(http_auth.split(':', 1))

            if isinstance(http_auth, (tuple, list)):
                http_auth = aiohttp.BasicAuth(*http_auth)

        headers = headers or {}
        headers.setdefault('content-type', 'application/json')

        self.session = aiohttp.ClientSession(
            auth=http_auth,
            timeout=ClientTimeout(total=self.timeout),
            connector=aiohttp.TCPConnector(
                loop=self.loop,
                ssl=verify_certs,
                use_dns_cache=use_dns_cache,
            ),
            headers=headers
        )

        self.base_url = 'http%s://%s:%d%s' % (
            's' if use_ssl else '',
            host, port, self.url_prefix
        )

    @asyncio.coroutine
    def close(self):
        yield from self.session.close()

    async def perform_request(self, method, url, params=None, body=None,
                              timeout=None, ignore=(), headers=None):
        url_path = url
        if params:
            url_path = '%s?%s' % (url, urlencode(params or {}))
        url = self.base_url + url_path

        start = self.loop.time()
        response = None
        try:
            async with async_timeout.timeout(
                    timeout or self.timeout, loop=self.loop
            ):
                response = await self.session.request(
                    method, url, data=body, headers=headers
                )
                raw_data = await response.text()
            duration = self.loop.time() - start

        except asyncio.CancelledError:
            raise

        except asyncio.TimeoutError as e:
            self.log_request_fail(method, url, body, self.loop.time() - start,
                                  exception=e)
            raise ConnectionTimeout('TIMEOUT', str(e), e)

        except ServerFingerprintMismatch as e:
            self.log_request_fail(method, url, body, self.loop.time() - start,
                                  exception=e)
            raise SSLError('N/A', str(e), e)

        except ClientError as e:
            self.log_request_fail(method, url, body, self.loop.time() - start,
                                  exception=e)
            raise ConnectionError('N/A', str(e), e)

        finally:
            if response is not None:
                await response.release()

        # raise errors based on http status codes, let the client handle those if needed
        if not (200 <= response.status < 300) and response.status not in ignore:
            self.log_request_fail(method, url, body, duration, response.status, raw_data)
            self._raise_error(response.status, raw_data)

        self.log_request_success(method, url, url_path, body, response.status, raw_data, duration)

        return response.status, response.headers, raw_data
