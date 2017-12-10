from __future__ import unicode_literals

import threading
import inspect
import traceback
import socket
from concurrent.futures import Future, TimeoutError, ThreadPoolExecutor, wait, FIRST_EXCEPTION
try:
    from functools import singledispatch
except ImportError:
    from singledispatch import singledispatch
from base64 import b64encode
from hashlib import sha1
import os
import socket
import ssl
import inspect
import copy
import platform

from io import StringIO
from six import string_types, binary_type
from ws4py import WS_KEY, WS_VERSION
from ws4py.exc import HandshakeError
from ws4py.websocket import WebSocket
from ws4py.compat import urlsplit
# from ws4py.client.threadedclient import WebSocketBaseClient
from ws4py.websocket import Heartbeat
try:
    from OpenSSL.SSL import Error as pyOpenSSLError
except ImportError:
    class pyOpenSSLError(Exception):
        pass
try:
    BrokenPipeError
except NameError:
    from beltway.util import BrokenPipeError

try:
    ConnectionRefusedError
except NameError:
    from beltway.util import ConnectionRefusedError


from beltway.wamp.const import CloseStatusCode
from beltway.wamp.exception import ApplicationError, SerializationError, ProtocolError, TransportLost
from beltway.wamp.uri import Pattern
from beltway.util import IdGenerator, methdispatch
from beltway.wamp import message, role, types
from beltway import __version__
from beltway.wamp.request import Publication, Handler, Subscription, SubscribeRequest, \
    RegisterRequest, Registration, Endpoint, UnregisterRequest, PublishRequest, UnsubscribeRequest, CallRequest, \
    InvocationRequest

from beltway.autolog import log


def is_method_or_function(f):
    return inspect.ismethod(f) or inspect.isfunction(f)


def parseSubprotocolIdentifier(subprotocol):
    try:
        s = subprotocol.split(u'.')
        if s[0] != u'wamp':
            raise Exception(u'WAMP WebSocket subprotocol identifier must start with "wamp", not "{}"'.format(s[0]))
        version = int(s[1])
        serializerId = u'.'.join(s[2:])
        return version, serializerId
    except:
        return None, None


class WebSocketBaseClient(WebSocket):

    STRICT_PROTOCOL_NEGOTIATION = True

    def __init__(self, url, protocols=None, extensions=None,
                 heartbeat_freq=None, ssl_options=None, headers=None, timeout=None):
        """
        A websocket client that implements :rfc:`6455` and provides a simple
        interface to communicate with a websocket server.

        This class works on its own but will block if not run in
        its own thread.

        When an instance of this class is created, a :py:mod:`socket`
        is created. If the connection is a TCP socket,
        the nagle's algorithm is disabled.

        The address of the server will be extracted from the given
        websocket url.

        The websocket key is randomly generated, reset the
        `key` attribute if you want to provide yours.

        For instance to create a TCP client:

        .. code-block:: python

           >>> from ws4py.client.threadedclient import WebSocketBaseClient
           >>> ws = WebSocketBaseClient('ws://localhost/ws')


        Here is an example for a TCP client over SSL:

        .. code-block:: python

           >>> from ws4py.client.threadedclient import WebSocketBaseClient
           >>> ws = WebSocketBaseClient('wss://localhost/ws')


        Finally an example of a Unix-domain connection:

        .. code-block:: python

           >>> from ws4py.client.threadedclient import WebSocketBaseClient
           >>> ws = WebSocketBaseClient('ws+unix:///tmp/my.sock')

        Note that in this case, the initial Upgrade request
        will be sent to ``/``. You may need to change this
        by setting the resource explicitely before connecting:

        .. code-block:: python

           >>> from ws4py.client.threadedclient import WebSocketBaseClient
           >>> ws = WebSocketBaseClient('ws+unix:///tmp/my.sock')
           >>> ws.resource = '/ws'
           >>> ws.connect()

        You may provide extra headers by passing a list of tuples
        which must be unicode objects.

        """
        self.url = url
        self.host = None
        self.scheme = None
        self.port = None
        self.unix_socket_path = None
        self.resource = None
        self.ssl_options = ssl_options or {}
        self.extra_headers = headers or []

        self.close_lock = threading.RLock()

        self._parse_url()

        if self.unix_socket_path:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
        else:
            # Let's handle IPv4 and IPv6 addresses
            # Simplified from CherryPy's code
            try:
                family, socktype, proto, canonname, sa = socket.getaddrinfo(self.host, self.port,
                                                                            socket.AF_UNSPEC,
                                                                            socket.SOCK_STREAM,
                                                                            0, socket.AI_PASSIVE)[0]
            except socket.gaierror:
                family = socket.AF_INET
                if self.host.startswith('::'):
                    family = socket.AF_INET6

                socktype = socket.SOCK_STREAM
                proto = 0
                canonname = ""
                sa = (self.host, self.port, 0, 0)

            sock = socket.socket(family, socktype, proto)

            if timeout is not None:
                # Note, this probably isn't right since it gets overridden in the listener thread ...
                sock.settimeout(timeout)

            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'AF_INET6') and family == socket.AF_INET6 and \
              self.host.startswith('::'):
                try:
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
                except (AttributeError, socket.error):
                    pass

        WebSocket.__init__(self, sock, protocols=protocols,
                           extensions=extensions,
                           heartbeat_freq=heartbeat_freq)

        self.stream.always_mask = True
        self.stream.expect_masking = False
        self.key = b64encode(os.urandom(16))

    # Adpated from: https://github.com/liris/websocket-client/blob/master/websocket.py#L105
    def _parse_url(self):
        """
        Parses a URL which must have one of the following forms:

        - ws://host[:port][path]
        - wss://host[:port][path]
        - ws+unix:///path/to/my.socket

        In the first two cases, the ``host`` and ``port``
        attributes will be set to the parsed values. If no port
        is explicitely provided, it will be either 80 or 443
        based on the scheme. Also, the ``resource`` attribute is
        set to the path segment of the URL (alongside any querystring).

        In addition, if the scheme is ``ws+unix``, the
        ``unix_socket_path`` attribute is set to the path to
        the Unix socket while the ``resource`` attribute is
        set to ``/``.
        """
        # Python 2.6.1 and below don't parse ws or wss urls properly. netloc is empty.
        # See: https://github.com/Lawouach/WebSocket-for-Python/issues/59
        scheme, url = self.url.split(":", 1)

        parsed = urlsplit(url, scheme="http")
        if parsed.hostname:
            self.host = parsed.hostname
        elif '+unix' in scheme:
            self.host = 'localhost'
        else:
            raise ValueError("Invalid hostname from: %s", self.url)

        if parsed.port:
            self.port = parsed.port

        if scheme == "ws":
            if not self.port:
                self.port = 80
        elif scheme == "wss":
            if not self.port:
                self.port = 443
        elif scheme in ('ws+unix', 'wss+unix'):
            pass
        else:
            raise ValueError("Invalid scheme: %s" % scheme)

        if parsed.path:
            resource = parsed.path
        else:
            resource = "/"

        if '+unix' in scheme:
            self.unix_socket_path = resource
            resource = '/'

        if parsed.query:
            resource += "?" + parsed.query

        self.scheme = scheme
        self.resource = resource

    @property
    def bind_addr(self):
        """
        Returns the Unix socket path if or a tuple
        ``(host, port)`` depending on the initial
        URL's scheme.
        """
        return self.unix_socket_path or (self.host, self.port)

    def close(self, code=1000, reason=''):
        """
        Initiate the closing handshake with the server.
        """
        with self.close_lock:
            log.debug("Closing WAMP connection.", code=code, reason=reason)
            if not self.client_terminated:
                self.client_terminated = True
                try:
                    self._write(self.stream.close(code=code, reason=reason).single(mask=True))
                except (BrokenPipeError, OSError):
                    pass

    def connect(self):
        """
        Connects this websocket and starts the upgrade handshake
        with the remote endpoint.
        """
        if self.scheme == "wss":
            # default port is now 443; upgrade self.sender to send ssl
            self.sock = ssl.wrap_socket(self.sock, **self.ssl_options)
            self._is_secure = True
        else:
            self._is_secure = False

        try:
            self.sock.connect(self.bind_addr)
        except ConnectionRefusedError:
            raise ConnectionRefusedError("Unable to connect to WAMP router {}".format(self.bind_addr))

        self._write(self.handshake_request)

        response = b''
        doubleCLRF = b'\r\n\r\n'
        while True:
            binary_data = self.sock.recv(128)
            if not binary_data:
                break
            response += binary_data
            if doubleCLRF in response:
                break

        if not response:
            self.close_connection()
            raise HandshakeError("Invalid response")

        headers, _, body = response.partition(doubleCLRF)
        response_line, _, headers = headers.partition(b'\r\n')

        try:
            self.process_response_line(response_line)
            self.protocols, self.extensions = self.process_handshake_header(headers)

            for subprotocol in self.protocols:
                version, serializerId = parseSubprotocolIdentifier(subprotocol)
                if version == 2 and serializerId in self._serializers.keys():
                    self._serializer = self._serializers[serializerId]
                    break

            if self.STRICT_PROTOCOL_NEGOTIATION:
                if not self._serializer:
                    # raise ConnectionDeny(ConnectionDeny.BAD_REQUEST, u'This server only speaks WebSocket subprotocols {}'.format(u', '.join(self.protocols)))
                    pass
            else:
                # assume wamp.2.json
                self._serializer = self._serializers[u'json']

        except HandshakeError:
            self.close_connection()
            raise

        self.handshake_ok()
        if body:
            self.process(body)

    @property
    def handshake_headers(self):
        """
        List of headers appropriate for the upgrade
        handshake.
        """
        headers = [
            ('Host', '%s:%s' % (self.host, self.port)),
            ('Connection', 'Upgrade'),
            ('Upgrade', 'websocket'),
            ('User-Agent', 'Beltway/{}; Python/{}'.format(__version__, platform.python_version())),
            ('Sec-WebSocket-Key', self.key.decode('utf-8')),
            ('Sec-WebSocket-Version', str(max(WS_VERSION)))
            ]

        if self.protocols:
            headers.append(('Sec-WebSocket-Protocol', ','.join(self.protocols)))

        if self.extra_headers:
            headers.extend(self.extra_headers)

        if not any(x for x in headers if x[0].lower() == 'origin'):

            scheme, url = self.url.split(":", 1)
            parsed = urlsplit(url, scheme="http")
            if parsed.hostname:
                self.host = parsed.hostname
            else:
                self.host = 'localhost'
            origin = scheme + '://' + parsed.hostname
            if parsed.port:
                origin = origin + ':' + str(parsed.port)
            headers.append(('Origin', origin))

        return headers

    @property
    def handshake_request(self):
        """
        Prepare the request to be sent for the upgrade handshake.
        """
        headers = self.handshake_headers
        request = [("GET %s HTTP/1.1" % self.resource).encode('utf-8')]
        for header, value in headers:
            request.append(("%s: %s" % (header, value)).encode('utf-8'))
        request.append(b'\r\n')

        return b'\r\n'.join(request)

    def process_response_line(self, response_line):
        """
        Ensure that we received a HTTP `101` status code in
        response to our request and if not raises :exc:`HandshakeError`.
        """
        protocol, code, status = response_line.split(b' ', 2)
        if code != b'101':
            raise HandshakeError("Invalid response status: %s %s" % (code, status))

    def process_handshake_header(self, headers):
        """
        Read the upgrade handshake's response headers and
        validate them against :rfc:`6455`.

        Returns:
            Tuple[List[str], List[str]]
        """
        protocols = []
        extensions = []

        headers = headers.strip()

        for header_line in headers.split(b'\r\n'):
            header, value = header_line.split(b':', 1)
            header = header.strip().lower()
            value = value.strip().lower()

            if header == b'upgrade' and value != b'websocket':
                raise HandshakeError("Invalid Upgrade header: %s" % value)

            elif header == b'connection' and value != b'upgrade':
                raise HandshakeError("Invalid Connection header: %s" % value)

            elif header == b'sec-websocket-accept':
                match = b64encode(sha1(self.key + WS_KEY).digest())
                if value != match.lower():
                    raise HandshakeError("Invalid challenge response: %s" % value)

            elif header == b'sec-websocket-protocol':
                protocols = value.decode('utf-8').split(',')

            elif header == b'sec-websocket-extensions':
                extensions = value.decode('utf-8').split(',')

        return protocols, extensions

    def handshake_ok(self):
        self.opened()


def or_set(self):
    self._set()
    self.changed()

def or_clear(self):
    self._clear()
    self.changed()

def orify(e, changed_callback):
    e._set = e.set
    e._clear = e.clear
    e.changed = changed_callback
    e.set = lambda: or_set(e)
    e.clear = lambda: or_clear(e)

def OrEvent(*events):
    or_event = threading.Event()
    def changed():
        bools = [e.is_set() for e in events]
        if any(bools):
            or_event.set()
        else:
            or_event.clear()
    for e in events:
        orify(e, changed)
    changed()
    return or_event


def any_event(*events, **kwargs):
    """Returns when the any of the events is set"""

    timeout=kwargs.pop("timeout", 0)
    event = OrEvent(*events)
    return event.wait(timeout)


class WampWebSocketClient(WebSocketBaseClient):
    """

    :type _session_id: int
    :type _call_reqs: Dict[int, CallRequest]
    :type _publish_reqs: Dict[int, PublishRequest]
    :type _register_reqs: Dict[int, RegisterRequest]
    :type _subscriptions: Dict[int, list[Subscription]]
    :type _subscribe_reqs: Dict[int, SubscribeRequest]
    :type _invocations: Dict[int, InvocationRequest]
    """
    debug = False

    def __init__(self, url, config=None, timeout=5.0, max_threads=None, serializers=None):
        if serializers is None:
            serializers = []

            # try MsgPack WAMP serializer
            try:
                from beltway.wamp.serializer import MsgPackSerializer
                serializers.append(MsgPackSerializer(batched=False))  # batched not supported
            except ImportError:
                pass

            # try JSON WAMP serializer
            try:
                from beltway.wamp.serializer import JsonSerializer
                serializers.append(JsonSerializer(batched=False))  # batched not supported
            except ImportError:
                pass

            if not serializers:
                raise Exception(u'Could not import any WAMP serializer')

        self._serializers = {}
        for ser in serializers:
            self._serializers[ser.SERIALIZER_ID] = ser

        protocols = [u'wamp.2.{}'.format(ser.SERIALIZER_ID) for ser in serializers]

        super(WampWebSocketClient, self).__init__(url=url, protocols=protocols, timeout=timeout)

        self.config = config or types.ComponentConfig(realm='realm1')

        self.timeout = timeout

        if max_threads is None:
            # the python2.7 compat ThreadPoolExecutor fix
            # If max_workers is None or not given, it will default to the number
            # of processors on the machine, multiplied by 5, assuming that ThreadPoolExecutor
            # is often used to overlap I/O instead of CPU work and the number of
            # workers should be higher than the number of workers for ProcessPoolExecutor.

            try:
                import multiprocessing
                max_threads = multiprocessing.cpu_count() * 5
            except (ImportError, NotImplementedError):
                # going with 4 threads otherwise it likely won't work
                max_threads = 4

        self.send_message_lock = threading.RLock()
        self.listener_thread = threading.Thread(target=self.run, name='wamp-ws-listener')
        self.executor = ThreadPoolExecutor(max_workers=max_threads)

        self._session_id = None
        self._goodbye_sent = False
        self._transport_is_closing = False
        self._joined_realm = None

        # outstanding requests
        self._publish_reqs = {}
        self._subscribe_reqs = {}
        self._unsubscribe_reqs = {}
        self._call_reqs = {}
        self._register_reqs = {}
        self._unregister_reqs = {}

        # subscriptions in place
        self._subscriptions = {}

        # registrations in place
        self._registrations = {}

        # incoming invocations
        self._invocations = {}

        self._serializer = None
        self._request_id_gen = IdGenerator()

        self.joined_event = threading.Event()

        self.aborted_event = threading.Event()

        # mapping of exception classes to WAMP error URIs
        self._ecls_to_uri_pat = {}

        # mapping of WAMP error URIs to exception classes
        self._uri_to_ecls = {
            ApplicationError.INVALID_PAYLOAD: SerializationError
        }

    def run(self):
        """
        Performs the operation of reading from the underlying
        connection in order to feed the stream of bytes.

        We start with a small size of two bytes to be read
        from the connection so that we can quickly parse an
        incoming frame header. Then the stream indicates
        whatever size must be read from the connection since
        it knows the frame payload length.

        Note that we perform some automatic opererations:

        * On a closing message, we respond with a closing
          message and finally close the connection
        * We respond to pings with pong messages.
        * Whenever an error is raised by the stream parsing,
          we initiate the closing of the connection with the
          appropiate error code.

        This method is blocking and should likely be run
        in a thread.
        """
        self.sock.setblocking(True)
        # self.sock.settimeout(0.5)

        with Heartbeat(self, frequency=self.heartbeat_freq):
            s = self.stream
            try:
                self.opened()
                while not self.terminated:
                    if self.once() is False:
                        break
            finally:
                self.terminate()

    def once(self):
        """
        Performs the operation of reading from the underlying
        connection in order to feed the stream of bytes.

        We start with a small size of two bytes to be read
        from the connection so that we can quickly parse an
        incoming frame header. Then the stream indicates
        whatever size must be read from the connection since
        it knows the frame payload length.

        It returns `False` if an error occurred at the
        socket level or during the bytes processing. Otherwise,
        it returns `True`.
        """
        if self.terminated:
            log.debug("WebSocket is already terminated")
            return False

        try:
            b = self.sock.recv(self.reading_buffer_size)
            # This will only make sense with secure sockets.
            if self._is_secure:
                b += self._get_from_pending()
        except socket.timeout:
            # Just return True in this case, so we can loop again.
            pass
        except (socket.error, OSError, pyOpenSSLError) as e:
            self.unhandled_error(e)
            return False
        else:
            if not self.process(b):
                return False

        return True

    def run_forever(self):
        """
        Simply blocks the thread until the
        websocket has terminated.
        """
        while not self.terminated:
            self.listener_thread.join(timeout=0.1)

    def handshake_ok(self):
        """
        Called when the upgrade handshake has completed
        successfully.

        Starts the client's thread.
        """
        super(WampWebSocketClient, self).handshake_ok()
        self.listener_thread.start()
        self.on_connect()

    def join(self, realm, authmethods=None, authid=None):
        """
        Implements :func:`autobahn.wamp.interfaces.ISession.join`
        """
        log.debug("Joining realm.", realm=realm)
        if self._session_id:
            raise ProtocolError("already joined")

        self._goodbye_sent = False

        msg = message.Hello(realm, role.DEFAULT_CLIENT_ROLES, authmethods, authid)
        self._joined_realm = realm
        self.send_message(msg)

        any_event(self.joined_event, self.aborted_event, timeout=self.timeout)

        if self.aborted_event.is_set():
            self._bailout(CloseStatusCode.INTERNAL_ERROR, reason=self._close_details.reason)
            raise ApplicationError(self._close_details.reason, self._close_details.message)
        elif not self.joined_event.is_set():
            msg = "Timeout waiting for JOIN message."
            self._bailout(CloseStatusCode.INTERNAL_ERROR, reason=msg)
            raise TimeoutError(msg)

    def closed(self, code, reason=None):
        # TODO: Add a reconnect here if the close was not deliberately triggered.
        log.debug("WAMP client closed.".format(code, reason), code=code, reason=reason)

    def send_message(self, msg):
        # log.debug("Sending message: {!r}".format(msg))
        payload, is_binary = self._serializer.serialize(msg)
        with self.send_message_lock:
            self.send(payload, binary=is_binary)

    def on_connect(self):
        self.join(realm=self.config.realm)

    def on_join(self, details):
        pass

    def on_challenge(self, challenge):
        pass

    def on_leave(self, details):
        pass

    def _bailout(self, code, reason=None):
        """
        Parameters:
            code: CloseStatusCode
        """

        log.debug("Failing WAMP-over-WebSocket transport.".format(code, reason), code=code, reason=reason)
        self.close(code.value, reason)

    def received_message(self, payload):
        """
        Callback from :func:`autobahn.websocket.interfaces.IWebSocketChannel.onMessage`
        """

        # TODO: This should spin off into its own thread to handle the messages
        # Even better would be a thread pool.

        data = None
        if payload.is_binary:
            data = payload.data
        else:  # text payload
            data = str(payload)
        try:
            for msg in self._serializer.unserialize(data):
                if self.debug:
                    log.debug("Parsed message: {}".format(msg))
                if not self._session_id:
                     if msg.__class__ not in message.OUT_OF_SESSION_RECV_MESSAGES:
                        raise ProtocolError("Received {0} message, and session is not yet established".format(msg.__class__))
                else:
                    if msg.__class__ not in message.IN_SESSION_RECV_MESSAGES:
                        if msg.__class__ in message.OUT_OF_SESSION_RECV_MESSAGES:
                            raise ProtocolError("Received {0} message, but session is already established.".format(msg.__class__))
                        else:
                            raise ProtocolError("Unsupported message {0}".format(msg.__class__))

                self.executor.submit(self.handle_message, msg)

        except ProtocolError as e:
            reason = "WAMP Protocol Error ({0})".format(e)
            log.error(reason, exc_info=True)
            self._bailout(CloseStatusCode.PROTOCOL_ERROR, reason=reason)

        except Exception as e:
            reason = "WAMP Internal Error ({0})".format(e)
            log.error(reason, exc_info=True)
            self._bailout(CloseStatusCode.INTERNAL_ERROR, reason=reason)

    def register(self, endpoint, procedure=None, options=None):
        """
        Implements :func:`autobahn.wamp.interfaces.ICallee.register`
        """
        assert (callable(endpoint) and procedure is not None) or hasattr(endpoint, '__class__')
        assert procedure is None or isinstance(procedure, string_types)
        assert options is None or isinstance(options, types.RegisterOptions)

        if self.terminated:
            raise TransportLost()

        def _register_fn(obj, fn, procedure, options, resolve_future=True):
            request_id = self._request_id_gen.next()
            on_reply = Future()
            endpoint_obj = Endpoint(fn, obj, options.details_arg if options else None)
            self._register_reqs[request_id] = RegisterRequest(request_id, on_reply, procedure, endpoint_obj)

            if options:
                msg = message.Register(request_id, procedure, **options.message_attr())
            else:
                msg = message.Register(request_id, procedure)

            self.send_message(msg)
            return on_reply.result(timeout=self.timeout) if resolve_future else on_reply

        if callable(endpoint):

            # register a single callable
            return _register_fn(None, endpoint, procedure, options)

        else:

            # register all methods on an object decorated with "wamp.register"
            on_replies = []
            for k in inspect.getmembers(endpoint.__class__, is_method_or_function):
                proc = k[1]
                if "_wampuris" in proc.__dict__:
                    for pat in proc.__dict__["_wampuris"]:
                        if pat.is_endpoint():
                            uri = pat.uri()
                            proc_options = proc.__dict__["_wampoptions"].get(uri, options)
                            on_replies.append(_register_fn(endpoint, proc, uri, proc_options))
            return on_replies

    def _unregister(self, registration):
        """
        Called from :meth:`autobahn.wamp.protocol.Registration.unregister`
        """
        assert (isinstance(registration, Registration))
        assert registration.active
        assert (registration.id in self._registrations)

        if self.terminated:
            raise TransportLost()

        request_id = self._request_id_gen.next()

        on_reply = Future()
        self._unregister_reqs[request_id] = UnregisterRequest(request_id, on_reply, registration.id)

        msg = message.Unregister(request_id, registration.id)

        self.send_message(msg)
        return on_reply.result(timeout=self.timeout)

    def publish(self, topic, *args, **kwargs):
        """
        Implements :func:`autobahn.wamp.interfaces.IPublisher.publish`
        """
        assert isinstance(topic, string_types)
        if self.terminated:
            raise TransportLost()

        request_id = self._request_id_gen.next()

        if 'options' in kwargs and isinstance(kwargs['options'], types.PublishOptions):
            options = kwargs.pop('options')
            msg = message.Publish(request_id, topic, args=args, kwargs=kwargs, **options.message_attr())
        else:
            options = None
            msg = message.Publish(request_id, topic, args=args, kwargs=kwargs)

        on_reply = None
        if options and options.acknowledge:
            # only acknowledged publications expect a reply ..
            on_reply = Future()
            self._publish_reqs[request_id] = PublishRequest(request_id, on_reply)

        try:
            # Notes:
            #
            # * this might raise autobahn.wamp.exception.SerializationError
            #   when the user payload cannot be serialized
            # * we have to setup a PublishRequest() in _publish_reqs _before_
            #   calling transpor.send(), because a mock- or side-by-side transport
            #   will immediately lead on an incoming WAMP message in onMessage()
            #
            self.send_message(msg)
        except Exception as e:
            if request_id in self._publish_reqs:
                del self._publish_reqs[request_id]
            raise e

        if on_reply:
            return on_reply.result(timeout=self.timeout)

    def subscribe(self, handler, topic=None, options=None):
        """

        :param handler: Callable.
        :param topic:
        :param options:
        :type options: types.SubscribeOptions
        :return:
        """

        assert (callable(handler) and topic is not None) or hasattr(handler, '__class__')
        assert topic is None or isinstance(topic, string_types)
        assert options is None or isinstance(options, types.SubscribeOptions)

        if self.terminated:
            raise TransportLost()

        def _subscribe_fn(obj, fn, topic, options):
            request_id = self._request_id_gen.next()
            on_reply = Future()
            handler_obj = Handler(fn, obj, options.details_arg if options else None)
            self._subscribe_reqs[request_id] = SubscribeRequest(request_id, topic, on_reply, handler_obj)

            if options:
                msg = message.Subscribe(request_id, topic, **options.message_attr())
            else:
                msg = message.Subscribe(request_id, topic)

            self.send_message(msg)
            return on_reply.result(timeout=self.timeout)

        if callable(handler):
            # subscribe a single handler
            return _subscribe_fn(None, handler, topic, options)

        else:

            # subscribe all methods on an object decorated with "wamp.subscribe"
            on_replies = []
            for k in inspect.getmembers(handler.__class__, is_method_or_function):
                proc = k[1]
                if "_wampuris" in proc.__dict__:
                    for pat in proc.__dict__["_wampuris"]:
                        if pat.is_handler():
                            uri = pat.uri()
                            # use the options provided at @subscribe, fall back on global register, finally exact or wildcard
                            subopts = proc.__dict__["_wampoptions"].get(uri, options or pat.subscribe_options())
                            # XXX XXX XXX XXX
                            # FIXME: We are doing this synchronously.  Really this should be a thread pool..
                            # XXX XXX XXX XXX
                            on_replies.append(_subscribe_fn(handler, proc, uri, subopts))
            return on_replies

    def _unsubscribe(self, subscription):
        """
        Called from :meth:`autobahn.wamp.protocol.Subscription.unsubscribe`
        """
        assert (isinstance(subscription, Subscription))
        assert subscription.active
        assert (subscription.id in self._subscriptions)
        assert (subscription in self._subscriptions[subscription.id])

        if self.terminated:
            raise TransportLost()

        # remove handler subscription and mark as inactive
        self._subscriptions[subscription.id].remove(subscription)
        subscription.active = False

        # number of handler subscriptions left ..
        scount = len(self._subscriptions[subscription.id])

        if scount == 0:
            # if the last handler was removed, unsubscribe from broker ..
            request_id = self._request_id_gen.next()

            on_reply = Future()
            self._unsubscribe_reqs[request_id] = UnsubscribeRequest(request_id, on_reply, subscription.id)
            msg = message.Unsubscribe(request_id, subscription.id)

            self.send_message(msg)
            return on_reply.result(timeout=self.timeout)

        else:
            # there are still handlers active on the subscription!
            raise NotImplementedError("# there are still handlers active on the subscription! This is not being handled.")
            #return txaio.create_future_success(scount)
            # we probably need to join on the queues here?

    def call(self, procedure, *args, **kwargs):
        """
        Implements :func:`autobahn.wamp.interfaces.ICaller.call`
        """
        assert isinstance(procedure, string_types)

        if self.terminated:
            raise TransportLost()

        request_id = self._request_id_gen.next()

        if 'options' in kwargs and isinstance(kwargs['options'], types.CallOptions):
            options = kwargs.pop('options')
            msg = message.Call(request_id, procedure, args=args, kwargs=kwargs, **options.message_attr())
        else:
            options = None
            msg = message.Call(request_id, procedure, args=args, kwargs=kwargs)

        on_reply = Future()
        self._call_reqs[request_id] = CallRequest(request_id, procedure, on_reply, options)
        try:
            self.send_message(msg)
        except:
            if request_id in self._call_reqs:
                del self._call_reqs[request_id]
            raise

        return on_reply.result(timeout=self.timeout)

    @methdispatch
    def handle_message(self, msg):
        """
        The generic "base" function.

        Parameters:
            msg: message.Message
        """
        raise NotImplementedError("Unhandled message {}".format(msg.__class__))

    @handle_message.register(message.Welcome)
    def _(self, msg):
        self._session_id = msg.session
        details = types.SessionDetails(self._joined_realm, self._session_id, msg.authid, msg.authrole, msg.authmethod)
        self.joined_event.set()
        try:
            self.on_join(details)
        except:
            log.exception("Error in on_join() call.")

    @handle_message.register(message.Abort)
    def _(self, msg):
        """
        Parameters:
            msg: message.Abort
        """

        # fire callback and close the transport
        details = types.CloseDetails(msg.reason, msg.message)
        self._close_details = details
        self.aborted_event.set()
        try:
            self.on_leave(details)
        except:
            log.exception("Error in on_leave() call.")

    @handle_message.register(message.Challenge)
    def _handlemsg_challenge(self, msg):
        """
        Parameters:
            msg: message.Challenge
        """
        challenge = types.Challenge(msg.method, msg.extra)
        log.debug("Challenge received", method=challenge.method)

        try:
            signature = self.on_challenge(challenge)
        except Exception as ex:
            reply = message.Abort("wamp.error.cannot_authenticate", "{0}".format(ex))
            self.send_message(reply)

            details = types.CloseDetails(reply.reason, reply.message)
            try:
                self.on_leave(details)
            except Exception as ex:
                log.warning("Error while firing onLeave: {}".format(ex))
        finally:
            msg = message.Authenticate(signature)
            self.send_message(msg)

    # @handle_message.register(message.Interrupt)
    # def _(self, msg):
    #     # if msg.request not in self._invocations:
    #     #     raise ProtocolError("INTERRUPT received for non-pending invocation {0}".format(msg.request))
    #     # else:
    #     #     # noinspection PyBroadException
    #     #     try:
    #     #         self._invocations[msg.request].cancel()
    #     #     except Exception as e:
    #     #         # XXX can .cancel() return a Deferred/Future?
    #     #         try:
    #     #             self.onUserError(e, "While cancelling call.")
    #     #         except:
    #     #             pass
    #     #     finally:
    #     #         del self._invocations[msg.request]

    @handle_message.register(message.Registered)
    def _(self, msg):
        """
        Parameters:
            msg: message.Registered
        """

        if msg.request in self._register_reqs:

            # get and pop outstanding register request
            request = self._register_reqs.pop(msg.request)

            on_reply = request.on_reply

            # create new registration if not yet tracked
            if msg.registration not in self._registrations:
                registration = Registration(self, msg.registration, request.procedure, request.endpoint)
                self._registrations[msg.registration] = registration
                on_reply.set_result(registration)
            else:
                raise ProtocolError(
                    "REGISTERED received for already existing registration ID {0}".format(msg.registration))

        else:
            raise ProtocolError("REGISTERED received for non-pending request ID {0}".format(msg.request))

    @handle_message.register(message.Unregistered)
    def _(self, msg):
        """
        Parameters:
            msg: message.Unregistered
        """

        if msg.request in self._unregister_reqs:

            # get and pop outstanding subscribe request
            request = self._unregister_reqs.pop(msg.request)

            # if the registration still exists, mark as inactive and remove ..
            if request.registration_id in self._registrations:
                self._registrations[request.registration_id].active = False
                del self._registrations[request.registration_id]

        else:
            raise ProtocolError("UNREGISTERED received for non-pending request ID {0}".format(msg.request))

    @handle_message.register(message.Error)
    def _(self, msg):
        """
        Parameters:
            msg: message.Error
        """

        # remove outstanding request and get the reply deferred/future
        on_reply = None

        # ERROR reply to CALL
        if msg.request_type == message.Call.MESSAGE_TYPE and msg.request in self._call_reqs:
            on_reply = self._call_reqs.pop(msg.request).on_reply

        # ERROR reply to PUBLISH
        elif msg.request_type == message.Publish.MESSAGE_TYPE and msg.request in self._publish_reqs:
            on_reply = self._publish_reqs.pop(msg.request).on_reply

        # ERROR reply to SUBSCRIBE
        elif msg.request_type == message.Subscribe.MESSAGE_TYPE and msg.request in self._subscribe_reqs:
            on_reply = self._subscribe_reqs.pop(msg.request).on_reply

        # ERROR reply to UNSUBSCRIBE
        elif msg.request_type == message.Unsubscribe.MESSAGE_TYPE and msg.request in self._unsubscribe_reqs:
            on_reply = self._unsubscribe_reqs.pop(msg.request).on_reply

        # ERROR reply to REGISTER
        elif msg.request_type == message.Register.MESSAGE_TYPE and msg.request in self._register_reqs:
            on_reply = self._register_reqs.pop(msg.request).on_reply

        # ERROR reply to UNREGISTER
        elif msg.request_type == message.Unregister.MESSAGE_TYPE and msg.request in self._unregister_reqs:
            on_reply = self._unregister_reqs.pop(msg.request).on_reply

        if on_reply:
            x = self._exception_from_message(msg)
            on_reply.set_exception(x)
        else:
            raise ProtocolError(
                "WampClient.onMessage(): ERROR received for non-pending request_type {0} and request ID {1}".format(
                    msg.request_type, msg.request))

    def _invoke_fn(self, endpoint_uri, fn, *args, **kwargs):
        """
        Execute the specified function.

        This is an override point for clients that wish to provide extra error handling etc.

        :param procedure: The registred URI for the callable.
        :param fn: The callable to execute.
        :param args: Any args to pass to the function.
        :param kwargs: Any kwargs to pass to the function.
        :return:
        """
        assert callable(fn)
        return fn(*args, **kwargs)

    @handle_message.register(message.Invocation)
    def _(self, msg):
        """
        Parameters:
            msg: message.Invocation
        """

        if msg.request in self._invocations:

            raise ProtocolError("INVOCATION received for request ID {0} already invoked".format(msg.request))

        else:

            self._invocations[msg.request] = InvocationRequest(msg.request, on_reply=None)

            if msg.registration not in self._registrations:

                raise ProtocolError("INVOCATION received for non-registered registration ID {0}".format(msg.registration))

            else:
                registration = self._registrations[msg.registration]
                endpoint = registration.endpoint

                if endpoint.obj is not None:
                    invoke_args = (endpoint.obj,)
                else:
                    invoke_args = tuple()

                if msg.args:
                    invoke_args = invoke_args + tuple(msg.args)

                invoke_kwargs = msg.kwargs if msg.kwargs else dict()

                if endpoint.details_arg:

                    if msg.receive_progress:
                        raise NotImplementedError("Progress not supported.")
                        # def progress(*args, **kwargs):
                        #     progress_msg = message.Yield(msg.request, args=args, kwargs=kwargs, progress=True)
                        #     self._transport.send(progress_msg)
                    else:
                        progress = None

                    invoke_kwargs[endpoint.details_arg] = types.CallDetails(progress, caller=msg.caller, caller_authid=msg.caller_authid, caller_authrole=msg.caller_authrole, procedure=msg.procedure, enc_algo=msg.enc_algo)

                try:
                    res = self._invoke_fn(registration.procedure, endpoint.fn, *invoke_args, **invoke_kwargs)
                except Exception as err:
                    try:
                        errmsg = 'Failure while invoking procedure {0} registered under "{1}".'.format(endpoint.fn, registration.procedure)
                        log.exception(errmsg)

                        tb = StringIO()
                        traceback.print_exc(file=tb)
                        formatted_tb = tb.getvalue().splitlines()

                        if hasattr(err, 'value'):
                            exc = err.value
                        else:
                            exc = err

                        reply = self._message_from_exception(message.Invocation.MESSAGE_TYPE, msg.request, exc, formatted_tb)

                        try:
                            self.send_message(reply)
                        except SerializationError as e:
                            errmsg = 'error return value from invoked procedure "{0}" could not be serialized: {1}'.format(registration.procedure, e)
                            log.exception(errmsg)
                            # the application-level payload returned from the invoked procedure can't be serialized
                            reply = message.Error(message.Invocation.MESSAGE_TYPE, msg.request, ApplicationError.INVALID_PAYLOAD,
                                                  args=[errmsg ])
                            self.send_message(reply)
                    except Exception as x:
                        log.exception("Exception attempting to construct reply.")
                        reply = message.Error(message.Invocation.MESSAGE_TYPE, msg.request, ApplicationError.INVALID_PAYLOAD,
                                              args=[repr(x)])
                        self.send_message(reply)

                else:
                    try:
                        # Success
                        if isinstance(res, types.CallResult):
                            reply = message.Yield(msg.request, args=res.results, kwargs=res.kwresults)
                        else:
                            reply = message.Yield(msg.request, args=[res])

                        try:
                            self.send_message(reply)
                        except SerializationError as e:
                            errmsg = 'success return value from invoked procedure "{0}" could not be serialized: {1}'.format(registration.procedure, e)
                            log.exception(errmsg)
                            # the application-level payload returned from the invoked procedure can't be serialized
                            reply = message.Error(message.Invocation.MESSAGE_TYPE, msg.request, ApplicationError.INVALID_PAYLOAD,
                                                  args=[errmsg])
                            self.send_message(reply)
                    except Exception as x:
                        log.exception("Exception attempting to construct reply.")
                        reply = message.Error(message.Invocation.MESSAGE_TYPE, msg.request, ApplicationError.INVALID_PAYLOAD,
                                              args=[repr(x)])
                        self.send_message(reply)

                finally:
                    del self._invocations[msg.request]

    @handle_message.register(message.Result)
    def _(self, msg):
        """
        Parameters:
            msg: message.Result
        """

        if msg.request in self._call_reqs:

            # TODO: Implement progress??

            call_request = self._call_reqs.pop(msg.request)
            on_reply = call_request.on_reply

            if msg.kwargs:
                if msg.args:
                    res = types.CallResult(*msg.args, **msg.kwargs)
                else:
                    res = types.CallResult(**msg.kwargs)
                on_reply.set_result(res)
            else:
                if msg.args:
                    if len(msg.args) > 1:
                        res = types.CallResult(*msg.args)
                        on_reply.set_result(res)
                    else:
                        on_reply.set_result(msg.args[0])
                else:
                    on_reply.set_result(None)
        else:
            raise ProtocolError("RESULT received for non-pending request ID {0}".format(msg.request))

    @handle_message.register(message.Unsubscribed)
    def _(self, msg):
        """
        Parameters:
            msg: message.Unsubscribed
        """

        if msg.request in self._unsubscribe_reqs:

            # get and pop outstanding subscribe request
            request = self._unsubscribe_reqs.pop(msg.request)

            # if the subscription still exists, mark as inactive and remove ..
            if request.subscription_id in self._subscriptions:
                for subscription in self._subscriptions[request.subscription_id]:
                    subscription.active = False
                del self._subscriptions[request.subscription_id]
        else:
            raise ProtocolError("UNSUBSCRIBED received for non-pending request ID {0}".format(msg.request))

    @handle_message.register(message.Subscribed)
    def _(self, msg):
        """
        Parameters:
            msg: message.Subscribed
        :return:
        """
        if msg.request in self._subscribe_reqs:

            # get and pop outstanding subscribe request
            request = self._subscribe_reqs.pop(msg.request)
            on_reply = request.on_reply

            # create new handler subscription list for subscription ID if not yet tracked
            if msg.subscription not in self._subscriptions:
                self._subscriptions[msg.subscription] = []

            subscription = Subscription(msg.subscription, request.topic, self, request.handler)

            # add handler to existing subscription
            self._subscriptions[msg.subscription].append(subscription)

            on_reply.set_result(subscription)
        else:
            raise ProtocolError("SUBSCRIBED received for non-pending request ID {0}".format(msg.request))

    @handle_message.register(message.Published)
    def _(self, msg):
        """
        Parameters:
            msg: message.Published
        """
        if msg.request in self._publish_reqs:

            # get and pop outstanding publish request
            publish_request = self._publish_reqs.pop(msg.request)

            # create a new publication object
            publication = Publication(msg.publication, was_encrypted=False)

            publish_request.on_reply.set_result(publication)
        else:
            raise ProtocolError("PUBLISHED received for non-pending request ID {0}".format(msg.request))

    @handle_message.register(message.Event)
    def _(self, msg):
        """
        Parameters:
            msg: message.Event
        """
        if msg.subscription in self._subscriptions:

            # fire all event handlers on subscription ..
            for subscription in self._subscriptions[msg.subscription]:

                handler = subscription.handler

                invoke_args = (handler.obj,) if handler.obj else tuple()
                if msg.args:
                    invoke_args = invoke_args + tuple(msg.args)

                invoke_kwargs = msg.kwargs if msg.kwargs else dict()
                if handler.details_arg:
                    invoke_kwargs[handler.details_arg] = types.EventDetails(publication=msg.publication,
                                                                            publisher=msg.publisher,
                                                                            topic=msg.topic)
                try:
                    self._invoke_fn(msg.topic, handler.fn, *invoke_args, **invoke_kwargs)
                except:
                    log.exception("error invoking handler {}".format(handler))
        else:
            raise ProtocolError(
                "EVENT received for non-subscribed subscription ID {0}".format(msg.subscription))

    @handle_message.register(message.Goodbye)
    def _(self, msg):
        """
        Parameters:
            msg: message.Goodbye
        """
        if not self._goodbye_sent:
            # the peer wants to close: send GOODBYE reply
            reply = message.Goodbye()
            self.send_message(reply)
        self._session_id = None
        # fire callback and close the transport
        details = types.CloseDetails(msg.reason, msg.message)
        try:
            self.on_leave(details)
        except:
            log.exception("error firing on_leave()")

    def define(self, exception, error=None):
        """
        Implements :func:`autobahn.wamp.interfaces.ISession.define`
        """
        if error is None:
            assert(hasattr(exception, '_wampuris'))
            self._ecls_to_uri_pat[exception] = exception._wampuris
            self._uri_to_ecls[exception._wampuris[0].uri()] = exception
        else:
            assert(not hasattr(exception, '_wampuris'))
            self._ecls_to_uri_pat[exception] = [Pattern(str(error), Pattern.URI_TARGET_HANDLER)]
            self._uri_to_ecls[str(error)] = exception

    def _message_from_exception(self, request_type, request, exc, tb=None):
        """
        Create a WAMP error message from an exception.

        :param request_type: The request type this WAMP error message is for.
        :type request_type: int
        :param request: The request ID this WAMP error message is for.
        :type request: int
        :param exc: The exception.
        :type exc: Instance of :class:`Exception` or subclass thereof.
        :param tb: Optional traceback. If present, it'll be included with the WAMP error message.
        :type tb: list or None
        """
        args = None
        if hasattr(exc, 'args'):
            args = list(exc.args)  # make sure tuples are made into lists

        kwargs = None
        if hasattr(exc, 'kwargs'):
            kwargs = exc.kwargs

        if tb:
            if kwargs:
                kwargs['traceback'] = tb
            else:
                kwargs = {'traceback': tb}

        if isinstance(exc, ApplicationError):
            error = exc.error if isinstance(exc.error, string_types) else str(exc.error)
        else:
            if exc.__class__ in self._ecls_to_uri_pat:
                error = self._ecls_to_uri_pat[exc.__class__][0]._uri
            else:
                error = "wamp.error.runtime_error"

        #log.debug("CREATING ERROR MESSAGE: request_type={!r} request={!r} error={!r} args={!r} kwargs={!r}".format(request_type, request, error, args, kwargs))
        msg = message.Error(request_type, request, error, args, kwargs)

        return msg

    def _exception_from_message(self, msg):
        """
        Create a user (or generic) exception from a WAMP error message.

        :param msg: A WAMP error message.
        :type msg: instance of :class:`autobahn.wamp.message.Error`
        """

        # FIXME:
        # 1. map to ecls based on error URI wildcard/prefix
        # 2. extract additional args/kwargs from error URI

        exc = None

        if msg.error in self._uri_to_ecls:
            ecls = self._uri_to_ecls[msg.error]
            ctorsig = inspect.signature(ecls.__init__)
            try:
                # the following might fail, eg. TypeError when
                # signature of exception constructor is incompatible
                # with args/kwargs or when the exception constructor raises
                if msg.kwargs:
                    kw = {k: v for (k,v) in msg.kwargs.items() if k in ctorsig.parameters}
                else:
                    kw = {}

                if kw:
                    if msg.args:
                        exc = ecls(*msg.args, **kw)
                    else:
                        exc = ecls(**kw)
                else:
                    if msg.args:
                        exc = ecls(*msg.args)
                    else:
                        exc = ecls()
            except Exception as e:
                log.exception("Error while reconstructing exception.")

        if not exc:
            # the following ctor never fails ..
            if msg.kwargs:
                if msg.args:
                    exc = ApplicationError(msg.error, *msg.args, **msg.kwargs)
                else:
                    exc = ApplicationError(msg.error, **msg.kwargs)
            else:
                if msg.args:
                    exc = ApplicationError(msg.error, *msg.args)
                else:
                    exc = ApplicationError(msg.error)

        return exc
