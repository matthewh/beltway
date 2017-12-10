from __future__ import unicode_literals
from six import string_types
from beltway.wamp import message
from beltway.wamp.exception import ProtocolError
from beltway.util import CustomJSONEncoder

class Serializer:
    """
    Base class for WAMP serializers. A WAMP serializer is the core glue between
    parsed WAMP message objects and the bytes on wire (the transport).
    """

    # WAMP defines the following 24 message types
    MESSAGE_TYPE_MAP = {
        message.Hello.MESSAGE_TYPE: message.Hello,
        message.Welcome.MESSAGE_TYPE: message.Welcome,
        message.Abort.MESSAGE_TYPE: message.Abort,
        message.Challenge.MESSAGE_TYPE: message.Challenge,
        message.Authenticate.MESSAGE_TYPE: message.Authenticate,
        message.Goodbye.MESSAGE_TYPE: message.Goodbye,
        message.Error.MESSAGE_TYPE: message.Error,
        message.Publish.MESSAGE_TYPE: message.Publish,
        message.Published.MESSAGE_TYPE: message.Published,
        message.Subscribe.MESSAGE_TYPE: message.Subscribe,
        message.Subscribed.MESSAGE_TYPE: message.Subscribed,
        message.Unsubscribe.MESSAGE_TYPE: message.Unsubscribe,
        message.Unsubscribed.MESSAGE_TYPE: message.Unsubscribed,
        message.Event.MESSAGE_TYPE: message.Event,
        message.Call.MESSAGE_TYPE: message.Call,
        message.Cancel.MESSAGE_TYPE: message.Cancel,
        message.Result.MESSAGE_TYPE: message.Result,
        message.Register.MESSAGE_TYPE: message.Register,
        message.Registered.MESSAGE_TYPE: message.Registered,
        message.Unregister.MESSAGE_TYPE: message.Unregister,
        message.Unregistered.MESSAGE_TYPE: message.Unregistered,
        message.Invocation.MESSAGE_TYPE: message.Invocation,
        message.Interrupt.MESSAGE_TYPE: message.Interrupt,
        message.Yield.MESSAGE_TYPE: message.Yield
    }
    """
    Mapping of WAMP message type codes to WAMP message classes.
    """

    def __init__(self, serializer):
        """
        Constructor.

        :param serializer: The object serializer to use for WAMP wire-level serialization.
        :type serializer: An object that implements :class:`autobahn.interfaces.IObjectSerializer`.
        """
        self._serializer = serializer

    def serialize(self, msg):
        """
        Implements :func:`autobahn.wamp.interfaces.ISerializer.serialize`
        """
        return msg.serialize(self._serializer), self._serializer.BINARY

    def unserialize(self, payload, isBinary=None):
        """
        Implements :func:`autobahn.wamp.interfaces.ISerializer.unserialize`
        """
        if isBinary is not None:
            if isBinary != self._serializer.BINARY:
                raise ProtocolError("invalid serialization of WAMP message (binary {0}, but expected {1})".format(isBinary, self._serializer.BINARY))

        try:
            raw_msgs = self._serializer.unserialize(payload)
        except Exception as e:
            raise ProtocolError("invalid serialization of WAMP message ({0})".format(e))

        msgs = []

        for raw_msg in raw_msgs:

            if type(raw_msg) != list:
                raise ProtocolError("invalid type {0} for WAMP message".format(type(raw_msg)))

            if len(raw_msg) == 0:
                raise ProtocolError(u"missing message type in WAMP message")

            message_type = raw_msg[0]

            if type(message_type) is not int:
                # CBOR doesn't roundtrip number types
                # https://bitbucket.org/bodhisnarkva/cbor/issues/6/number-types-dont-roundtrip
                raise ProtocolError("invalid type {0} for WAMP message type".format(type(message_type)))

            Klass = self.MESSAGE_TYPE_MAP.get(message_type)

            if Klass is None:
                raise ProtocolError("invalid WAMP message type {0}".format(message_type))

            # this might again raise `ProtocolError` ..
            msg = Klass.parse(raw_msg)

            msgs.append(msg)

        return msgs


try:
    # try import accelerated JSON implementation
    ##
    import ujson
    _json = ujson

    def _loads(val):
        return ujson.loads(val, precise_float=True)

    def _dumps(obj):
        return ujson.dumps(obj, double_precision=15, ensure_ascii=False)

except ImportError:
    # fallback to stdlib implementation
    ##
    import json
    _json = json
    _loads = json.loads

    def _dumps(obj):
        return json.dumps(obj, separators=(',', ':'), ensure_ascii=False, cls=CustomJSONEncoder)


class JsonObjectSerializer(object):

    JSON_MODULE = _json
    """
    The JSON module used (either stdib builtin or ujson).
    """

    BINARY = False

    def __init__(self, batched=False):
        """
        Ctor.

        :param batched: Flag that controls whether serializer operates in batched mode.
        :type batched: bool
        """
        self._batched = batched

    def serialize(self, obj):
        """
        Implements :func:`autobahn.wamp.interfaces.IObjectSerializer.serialize`
        """
        s = _dumps(obj)
        if isinstance(s, string_types):
            s = s.encode('utf8')
        if self._batched:
            return s + b'\30'
        else:
            return s

    def unserialize(self, payload):
        """
        Implements :func:`autobahn.wamp.interfaces.IObjectSerializer.unserialize`
        """
        if self._batched:
            chunks = payload.split(b'\30')[:-1] # This will probably fail since payload is unicode.
        else:
            chunks = [payload]
        if len(chunks) == 0:
            raise Exception("batch format error")

        return [_loads(data) for data in chunks]


class JsonSerializer(Serializer):

    SERIALIZER_ID = "json"
    """
    ID used as part of the WebSocket subprotocol name to identify the
    serializer with WAMP-over-WebSocket.
    """

    RAWSOCKET_SERIALIZER_ID = 1
    """
    ID used in lower four bits of second octet in RawSocket opening
    handshake identify the serializer with WAMP-over-RawSocket.
    """

    MIME_TYPE = "application/json"
    """
    MIME type announced in HTTP request/response headers when running
    WAMP-over-Longpoll HTTP fallback.
    """

    def __init__(self, batched=False):
        """
        Ctor.

        :param batched: Flag to control whether to put this serialized into batched mode.
        :type batched: bool
        """
        Serializer.__init__(self, JsonObjectSerializer(batched=batched))
        if batched:
            self.SERIALIZER_ID = "json.batched"


# MsgPack serialization depends on the `u-msgpack` package being available
# https://pypi.python.org/pypi/u-msgpack-python
# https://github.com/vsergeev/u-msgpack-python
#
try:
    import umsgpack
except ImportError:
    pass
else:

    class MsgPackObjectSerializer(object):

        BINARY = True
        """
        Flag that indicates whether this serializer needs a binary clean transport.
        """

        def __init__(self, batched=False, serialize_hook=None, unserialize_hook=None):
            """
            :param batched: Flag that controls whether serializer operates in batched mode.
            :type batched: bool
            """
            self._batched = batched

            if serialize_hook is not None:
                self.serialize_hook = serialize_hook

            if unserialize_hook is not None:
                self.unserialize_hook = unserialize_hook

        def serialize(self, obj):
            """
            Implements :func:`autobahn.wamp.interfaces.IObjectSerializer.serialize`
            """
            obj = self.serialize_hook(obj)
            data = umsgpack.packb(obj)
            if self._batched:
                return struct.pack("!L", len(data)) + data
            else:
                return data

        def unserialize(self, payload):
            """
            Implements :func:`autobahn.wamp.interfaces.IObjectSerializer.unserialize`
            """

            if self._batched:
                msgs = []
                N = len(payload)
                i = 0
                while i < N:
                    # read message length prefix
                    if i + 4 > N:
                        raise Exception("batch format error [1]")
                    l = struct.unpack("!L", payload[i:i + 4])[0]

                    # read message data
                    if i + 4 + l > N:
                        raise Exception("batch format error [2]")
                    data = payload[i + 4:i + 4 + l]

                    # append parsed raw message
                    msgs.append(umsgpack.unpackb(data))

                    # advance until everything consumed
                    i = i + 4 + l

                if i != N:
                    raise Exception("batch format error [3]")
                return msgs

            else:
                unpacked = umsgpack.unpackb(payload)
                unpacked = self.unserialize_hook(unpacked)
                return [unpacked]

        def serialize_hook(self, obj):
            # override to implement custom object conversion
            return obj

        def unserialize_hook(self, payload):
            # override to implement custom payload conversion
            return payload

    class MsgPackSerializer(Serializer):

        SERIALIZER_ID = u"msgpack"
        """
        ID used as part of the WebSocket subprotocol name to identify the
        serializer with WAMP-over-WebSocket.
        """

        RAWSOCKET_SERIALIZER_ID = 2
        """
        ID used in lower four bits of second octet in RawSocket opening
        handshake identify the serializer with WAMP-over-RawSocket.
        """

        MIME_TYPE = u"application/x-msgpack"
        """
        MIME type announced in HTTP request/response headers when running
        WAMP-over-Longpoll HTTP fallback.
        """

        def __init__(self, batched=False, serialize_hook=None, unserialize_hook=None):
            """
            Ctor.
            :param batched: Flag to control whether to put this serialized into batched mode.
            :type batched: bool
            """
            Serializer.__init__(self, MsgPackObjectSerializer(batched=batched, serialize_hook=serialize_hook, unserialize_hook=unserialize_hook))
            if batched:
                self.SERIALIZER_ID = u"msgpack.batched"
