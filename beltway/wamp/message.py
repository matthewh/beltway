###############################################################################
#
# The MIT License (MIT)
#
# Copyright (c) Tavendo GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
###############################################################################

import binascii
import re

from six import string_types, binary_type
from beltway import util
from beltway.wamp.exception import ProtocolError
from beltway.wamp.role import ROLE_NAME_TO_CLASS

__all__ = ('Message',
           'Hello',
           'Welcome',
           'Abort',
           'Challenge',
           'Authenticate',
           'Goodbye',
           'Error',
           'Publish',
           'Published',
           'Subscribe',
           'Subscribed',
           'Unsubscribe',
           'Unsubscribed',
           'Event',
           'Call',
           'Cancel',
           'Result',
           'Register',
           'Registered',
           'Unregister',
           'Unregistered',
           'Invocation',
           'Interrupt',
           'Yield',
           'check_or_raise_uri',
           'check_or_raise_id',
           'PAYLOAD_ENC_CRYPTO_BOX')


# strict URI check allowing empty URI components
_URI_PAT_STRICT_EMPTY = re.compile(r"^(([0-9a-z_]+\.)|\.)*([0-9a-z_]+)?$")

# loose URI check allowing empty URI components
_URI_PAT_LOOSE_EMPTY = re.compile(r"^(([^\s\.#]+\.)|\.)*([^\s\.#]+)?$")

# strict URI check disallowing empty URI components
_URI_PAT_STRICT_NON_EMPTY = re.compile(r"^([0-9a-z_]+\.)*([0-9a-z_]+)$")

# loose URI check disallowing empty URI components
_URI_PAT_LOOSE_NON_EMPTY = re.compile(r"^([^\s\.#]+\.)*([^\s\.#]+)$")

# strict URI check disallowing empty URI components in all but the last component
_URI_PAT_STRICT_LAST_EMPTY = re.compile(r"^([0-9a-z_]+\.)*([0-9a-z_]*)$")

# loose URI check disallowing empty URI components in all but the last component
_URI_PAT_LOOSE_LAST_EMPTY = re.compile(r"^([^\s\.#]+\.)*([^\s\.#]*)$")

# custom (=implementation specific) WAMP attributes (used in WAMP message details/options)
_CUSTOM_ATTRIBUTE = re.compile(r"^x_([a-z][0-9a-z_]+)?$")

# Value for algo attribute in end-to-end encrypted messages using cryptobox, which
# is a scheme based on Curve25519, SHA512, Salsa20 and Poly1305.
# See: http://cr.yp.to/highspeed/coolnacl-20120725.pdf
PAYLOAD_ENC_CRYPTO_BOX = 'cryptobox'


def b2a(data, max_len=40):
    if isinstance(data, string_types):
        s = data
    elif isinstance(data, binary_type):
        s = binascii.b2a_hex(data)
    elif data is None:
        s = '-'
    else:
        s = '{}'.format(data)
    if len(s) > max_len:
        return s[:max_len] + '..'
    else:
        return s


def check_or_raise_uri(value, message="WAMP message invalid", strict=False, allow_empty_components=False, allow_last_empty=False, allow_none=False):
    """
    Check a value for being a valid WAMP URI.

    If the value is not a valid WAMP URI is invalid, raises :class:`autobahn.wamp.exception.ProtocolError`.
    Otherwise return the value.

    :param value: The value to check.
    :type value: unicode or None
    :param message: Prefix for message in exception raised when value is invalid.
    :type message: unicode
    :param strict: If ``True``, do a strict check on the URI (the WAMP spec SHOULD behavior).
    :type strict: bool
    :param allow_empty_components: If ``True``, allow empty URI components (for pattern based
       subscriptions and registrations).
    :type allow_empty_components: bool
    :param allow_none: If ``True``, allow ``None`` for URIs.
    :type allow_none: bool

    :returns: The URI value (if valid).
    :rtype: unicode
    :raises: instance of :class:`autobahn.wamp.exception.ProtocolError`
    """
    if value is None:
        if allow_none:
            return
        else:
            raise ProtocolError("{0}: URI cannot be null".format(message))

    if not isinstance(value, string_types):
        if not (value is None and allow_none):
            raise ProtocolError("{0}: invalid type {1} for URI".format(message, type(value)))

    if strict:
        if allow_last_empty:
            pat = _URI_PAT_STRICT_LAST_EMPTY
        elif allow_empty_components:
            pat = _URI_PAT_STRICT_EMPTY
        else:
            pat = _URI_PAT_STRICT_NON_EMPTY
    else:
        if allow_last_empty:
            pat = _URI_PAT_LOOSE_LAST_EMPTY
        elif allow_empty_components:
            pat = _URI_PAT_LOOSE_EMPTY
        else:
            pat = _URI_PAT_LOOSE_NON_EMPTY

    if not pat.match(value):
        raise ProtocolError("{0}: invalid value '{1}' for URI (did not match pattern {2}, strict={3}, allow_empty_components={4}, allow_last_empty={5}, allow_none={6})".format(message, value, pat.pattern, strict, allow_empty_components, allow_last_empty, allow_none))
    else:
        return value


def check_or_raise_id(value, message="WAMP message invalid"):
    """
    Check a value for being a valid WAMP ID.

    If the value is not a valid WAMP ID, raises :class:`autobahn.wamp.exception.ProtocolError`.
    Otherwise return the value.

    :param value: The value to check.
    :type value: int
    :param message: Prefix for message in exception raised when value is invalid.
    :type message: unicode

    :returns: The ID value (if valid).
    :rtype: int
    :raises: instance of :class:`autobahn.wamp.exception.ProtocolError`
    """
    if type(value) is not int:
        raise ProtocolError("{0}: invalid type {1} for ID".format(message, type(value)))
    # the value 0 for WAMP IDs is possible in certain WAMP messages, e.g. UNREGISTERED with
    # router revocation signaling!
    if value < 0 or value > 9007199254740992:  # 2**53
        raise ProtocolError("{0}: invalid value {1} for ID".format(message, value))
    return value


def check_or_raise_extra(value, message="WAMP message invalid"):
    """
    Check a value for being a valid WAMP extra dictionary.

    If the value is not a valid WAMP extra dictionary, raises :class:`autobahn.wamp.exception.ProtocolError`.
    Otherwise return the value.

    :param value: The value to check.
    :type value: dict
    :param message: Prefix for message in exception raised when value is invalid.
    :type message: unicode

    :returns: The extra dictionary (if valid).
    :rtype: dict
    :raises: instance of :class:`autobahn.wamp.exception.ProtocolError`
    """
    if type(value) != dict:
        raise ProtocolError("{0}: invalid type {1}".format(message, type(value)))
    for k in value.keys():
        if not isinstance(k, string_types):
            raise ProtocolError("{0}: invalid type {1} for key '{2}'".format(message, type(k), k))
    return value


class Message(util.EqualityMixin):
    """
    WAMP message base class.

    .. note:: This is not supposed to be instantiated.
    """

    MESSAGE_TYPE = None
    """
    WAMP message type code.
    """

    def __init__(self):
        # serialization cache: mapping from ISerializer instances to serialized bytes
        self._serialized = {}

    @staticmethod
    def parse(wmsg):
        """
        Factory method that parses a unserialized raw message (as returned byte
        :func:`autobahn.interfaces.ISerializer.unserialize`) into an instance
        of this class.

        :returns: obj -- An instance of this class.
        """

    def uncache(self):
        """
        Resets the serialization cache.
        """
        self._serialized = {}

    def serialize(self, serializer):
        """
        Serialize this object into a wire level bytes representation and cache
        the resulting bytes. If the cache already contains an entry for the given
        serializer, return the cached representation directly.

        :param serializer: The wire level serializer to use.
        :type serializer: An instance that implements :class:`autobahn.interfaces.ISerializer`

        :returns: bytes -- The serialized bytes.
        """
        # only serialize if not cached ..
        if serializer not in self._serialized:
            self._serialized[serializer] = serializer.serialize(self.marshal())
        return self._serialized[serializer]


class Hello(Message):
    """
    A WAMP ``HELLO`` message.

    Format: ``[HELLO, Realm|uri, Details|dict]``
    """

    MESSAGE_TYPE = 1
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, realm, roles, authmethods=None, authid=None, authrole=None, authextra=None):
        """

        :param realm: The URI of the WAMP realm to join.
        :type realm: unicode
        :param roles: The WAMP session roles and features to announce.
        :type roles: dict of :class:`autobahn.wamp.role.RoleFeatures`
        :param authmethods: The authentication methods to announce.
        :type authmethods: list of unicode or None
        :param authid: The authentication ID to announce.
        :type authid: unicode or None
        :param authrole: The authentication role to announce.
        :type authrole: unicode or None
        :param authextra: Application-specific "extra data" to be forwarded to the client.
        :type authextra: arbitrary or None
        """
        assert(realm is None or isinstance(realm, string_types))
        assert(type(roles) == dict)
        assert(len(roles) > 0)
        for role in roles:
            assert(role in ['subscriber', 'publisher', 'caller', 'callee'])
            assert(isinstance(roles[role], ROLE_NAME_TO_CLASS[role]))
        if authmethods:
            assert(type(authmethods) == list)
            for authmethod in authmethods:
                assert(isinstance(authmethod, string_types))
        assert(authid is None or isinstance(authid, string_types))
        assert(authrole is None or isinstance(authrole, string_types))
        assert(authextra is None or type(authextra) == dict)

        Message.__init__(self)
        self.realm = realm
        self.roles = roles
        self.authmethods = authmethods
        self.authid = authid
        self.authrole = authrole
        self.authextra = authextra

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Hello.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for HELLO".format(len(wmsg)))

        realm = check_or_raise_uri(wmsg[1], "'realm' in HELLO", allow_none=True)
        details = check_or_raise_extra(wmsg[2], "'details' in HELLO")

        roles = {}

        if 'roles' not in details:
            raise ProtocolError("missing mandatory roles attribute in options in HELLO")

        details_roles = check_or_raise_extra(details['roles'], "'roles' in 'details' in HELLO")

        if len(details_roles) == 0:
            raise ProtocolError("empty 'roles' in 'details' in HELLO")

        for role in details_roles:
            if role not in ['subscriber', 'publisher', 'caller', 'callee']:
                raise ProtocolError("invalid role '{0}' in 'roles' in 'details' in HELLO".format(role))

            role_cls = ROLE_NAME_TO_CLASS[role]

            details_role = check_or_raise_extra(details_roles[role], "role '{0}' in 'roles' in 'details' in HELLO".format(role))

            if 'features' in details_role:
                check_or_raise_extra(details_role['features'], "'features' in role '{0}' in 'roles' in 'details' in HELLO".format(role))

                role_features = role_cls(**details_role['features'])

            else:
                role_features = role_cls()

            roles[role] = role_features

        authmethods = None
        if 'authmethods' in details:
            details_authmethods = details['authmethods']
            if type(details_authmethods) != list:
                raise ProtocolError("invalid type {0} for 'authmethods' detail in HELLO".format(type(details_authmethods)))

            for auth_method in details_authmethods:
                if not isinstance(auth_method, string_types):
                    raise ProtocolError("invalid type {0} for item in 'authmethods' detail in HELLO".format(type(auth_method)))

            authmethods = details_authmethods

        authid = None
        if 'authid' in details:
            details_authid = details['authid']
            if not isinstance(details_authid, string_types):
                raise ProtocolError("invalid type {0} for 'authid' detail in HELLO".format(type(details_authid)))

            authid = details_authid

        authrole = None
        if 'authrole' in details:
            details_authrole = details['authrole']
            if not isinstance(details_authrole, string_types):
                raise ProtocolError("invalid type {0} for 'authrole' detail in HELLO".format(type(details_authrole)))

            authrole = details_authrole

        authextra = None
        if 'authextra' in details:
            details_authextra = details['authextra']
            if type(details_authextra) != dict:
                raise ProtocolError("invalid type {0} for 'authextra' detail in HELLO".format(type(details_authextra)))

            authextra = details_authextra

        obj = Hello(realm, roles, authmethods, authid, authrole, authextra)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {'roles': {}}
        for role in self.roles.values():
            details['roles'][role.ROLE] = {}
            for feature in role.__dict__:
                if not feature.startswith('_') and feature != 'ROLE' and getattr(role, feature) is not None:
                    if 'features' not in details['roles'][role.ROLE]:
                        details['roles'][role.ROLE] = {'features': {}}
                    details['roles'][role.ROLE]['features'][feature] = getattr(role, feature)

        if self.authmethods:
            details['authmethods'] = self.authmethods

        if self.authid:
            details['authid'] = self.authid

        if self.authrole:
            details['authrole'] = self.authrole

        if self.authextra:
            details['authextra'] = self.authextra

        return [Hello.MESSAGE_TYPE, self.realm, details]

    def __str__(self):
        """
        Return a string representation of this message.
        """
        return "Hello(realm={0}, roles={1}, authmethods={2}, authid={3}, authrole={4}, authextra={5})".format(self.realm, self.roles, self.authmethods, self.authid, self.authrole, self.authextra)


class Welcome(Message):
    """
    A WAMP ``WELCOME`` message.

    Format: ``[WELCOME, Session|id, Details|dict]``
    """

    MESSAGE_TYPE = 2
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, session, roles, realm=None, authid=None, authrole=None, authmethod=None, authprovider=None, authextra=None, custom=None):
        """

        :param session: The WAMP session ID the other peer is assigned.
        :type session: int
        :param roles: The WAMP roles to announce.
        :type roles: dict of :class:`autobahn.wamp.role.RoleFeatures`
        :param realm: The effective realm the session is joined on.
        :type realm: unicode or None
        :param authid: The authentication ID assigned.
        :type authid: unicode or None
        :param authrole: The authentication role assigned.
        :type authrole: unicode or None
        :param authmethod: The authentication method in use.
        :type authmethod: unicode or None
        :param authprovider: The authentication provided in use.
        :type authprovider: unicode or None
        :param authextra: Application-specific "extra data" to be forwarded to the client.
        :type authextra: arbitrary or None
        :param custom: Implementation-specific "custom attributes" (`x_my_impl_attribute`) to be set.
        :type custom: dict or None
        """
        assert (type(session) is int)
        assert(type(roles) == dict)
        assert(len(roles) > 0)
        for role in roles:
            assert(role in ['broker', 'dealer'])
            assert(isinstance(roles[role], ROLE_NAME_TO_CLASS[role]))
        assert(realm is None or isinstance(realm, string_types))
        assert(authid is None or isinstance(authid, string_types))
        assert(authrole is None or isinstance(authrole, string_types))
        assert(authmethod is None or isinstance(authmethod, string_types))
        assert(authprovider is None or isinstance(authprovider, string_types))
        assert(authextra is None or type(authextra) == dict)
        assert(custom is None or type(custom) == dict)
        if custom:
            for k in custom:
                assert(_CUSTOM_ATTRIBUTE.match(k))

        Message.__init__(self)
        self.session = session
        self.roles = roles
        self.realm = realm
        self.authid = authid
        self.authrole = authrole
        self.authmethod = authmethod
        self.authprovider = authprovider
        self.authextra = authextra
        self.custom = custom or {}

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Welcome.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for WELCOME".format(len(wmsg)))

        session = check_or_raise_id(wmsg[1], "'session' in WELCOME")
        details = check_or_raise_extra(wmsg[2], "'details' in WELCOME")

        # FIXME: tigher value checking (types, URIs etc)
        realm = details.get('realm', None)
        authid = details.get('authid', None)
        authrole = details.get('authrole', None)
        authmethod = details.get('authmethod', None)
        authprovider = details.get('authprovider', None)
        authextra = details.get('authextra', None)

        roles = {}

        if 'roles' not in details:
            raise ProtocolError("missing mandatory roles attribute in options in WELCOME")

        details_roles = check_or_raise_extra(details['roles'], "'roles' in 'details' in WELCOME")

        if len(details_roles) == 0:
            raise ProtocolError("empty 'roles' in 'details' in WELCOME")

        for role in details_roles:
            if role not in ['broker', 'dealer']:
                raise ProtocolError("invalid role '{0}' in 'roles' in 'details' in WELCOME".format(role))

            role_cls = ROLE_NAME_TO_CLASS[role]

            details_role = check_or_raise_extra(details_roles[role], "role '{0}' in 'roles' in 'details' in WELCOME".format(role))

            if 'features' in details_role:
                check_or_raise_extra(details_role['features'], "'features' in role '{0}' in 'roles' in 'details' in WELCOME".format(role))

                role_features = role_cls(**details_roles[role]['features'])

            else:
                role_features = role_cls()

            roles[role] = role_features

        custom = {}
        for k in details:
            if _CUSTOM_ATTRIBUTE.match(k):
                custom[k] = details[k]

        obj = Welcome(session, roles, realm, authid, authrole, authmethod, authprovider, authextra, custom)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {}
        details.update(self.custom)

        if self.realm:
            details['realm'] = self.realm

        if self.authid:
            details['authid'] = self.authid

        if self.authrole:
            details['authrole'] = self.authrole

        if self.authrole:
            details['authmethod'] = self.authmethod

        if self.authprovider:
            details['authprovider'] = self.authprovider

        if self.authextra:
            details['authextra'] = self.authextra

        details['roles'] = {}
        for role in self.roles.values():
            details['roles'][role.ROLE] = {}
            for feature in role.__dict__:
                if not feature.startswith('_') and feature != 'ROLE' and getattr(role, feature) is not None:
                    if 'features' not in details['roles'][role.ROLE]:
                        details['roles'][role.ROLE] = {'features': {}}
                    details['roles'][role.ROLE]['features'][feature] = getattr(role, feature)

        return [Welcome.MESSAGE_TYPE, self.session, details]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Welcome(session={0}, roles={1}, realm={2}, authid={3}, authrole={4}, authmethod={5}, authprovider={6}, authextra={7})".format(self.session, self.roles, self.realm, self.authid, self.authrole, self.authmethod, self.authprovider, self.authextra)


class Abort(Message):
    """
    A WAMP ``ABORT`` message.

    Format: ``[ABORT, Details|dict, Reason|uri]``
    """

    MESSAGE_TYPE = 3
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, reason, message=None):
        """

        :param reason: WAMP or application error URI for aborting reason.
        :type reason: unicode
        :param message: Optional human-readable closing message, e.g. for logging purposes.
        :type message: unicode or None
        """
        assert(isinstance(reason, string_types))
        assert(message is None or isinstance(message, string_types))

        Message.__init__(self)
        self.reason = reason
        self.message = message

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        ##
        assert(len(wmsg) > 0 and wmsg[0] == Abort.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for ABORT".format(len(wmsg)))

        details = check_or_raise_extra(wmsg[1], "'details' in ABORT")
        reason = check_or_raise_uri(wmsg[2], "'reason' in ABORT")

        message = None

        if 'message' in details:

            details_message = details['message']
            if not isinstance(details_message, string_types):
                raise ProtocolError("invalid type {0} for 'message' detail in ABORT".format(type(details_message)))

            message = details_message

        obj = Abort(reason, message)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {}
        if self.message:
            details['message'] = self.message

        return [Abort.MESSAGE_TYPE, details, self.reason]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Abort(message={0}, reason={1})".format(self.message, self.reason)


class Challenge(Message):
    """
    A WAMP ``CHALLENGE`` message.

    Format: ``[CHALLENGE, Method|string, Extra|dict]``
    """

    MESSAGE_TYPE = 4
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, method, extra=None):
        """

        :param method: The authentication method.
        :type method: unicode
        :param extra: Authentication method specific information.
        :type extra: dict or None
        """
        assert(isinstance(method, string_types))
        assert(extra is None or type(extra) == dict)

        Message.__init__(self)
        self.method = method
        self.extra = extra or {}

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        ##
        assert(len(wmsg) > 0 and wmsg[0] == Challenge.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for CHALLENGE".format(len(wmsg)))

        method = wmsg[1]
        if not isinstance(method, string_types):
            raise ProtocolError("invalid type {0} for 'method' in CHALLENGE".format(type(method)))

        extra = check_or_raise_extra(wmsg[2], "'extra' in CHALLENGE")

        obj = Challenge(method, extra)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Challenge.MESSAGE_TYPE, self.method, self.extra]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Challenge(method={0}, extra={1})".format(self.method, self.extra)


class Authenticate(Message):
    """
    A WAMP ``AUTHENTICATE`` message.

    Format: ``[AUTHENTICATE, Signature|string, Extra|dict]``
    """

    MESSAGE_TYPE = 5
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, signature, extra=None):
        """

        :param signature: The signature for the authentication challenge.
        :type signature: unicode
        :param extra: Authentication method specific information.
        :type extra: dict or None
        """
        assert(isinstance(signature, string_types))
        assert(extra is None or type(extra) == dict)

        Message.__init__(self)
        self.signature = signature
        self.extra = extra or {}

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Authenticate.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for AUTHENTICATE".format(len(wmsg)))

        signature = wmsg[1]
        if not isinstance(signature, string_types):
            raise ProtocolError("invalid type {0} for 'signature' in AUTHENTICATE".format(type(signature)))

        extra = check_or_raise_extra(wmsg[2], "'extra' in AUTHENTICATE")

        obj = Authenticate(signature, extra)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Authenticate.MESSAGE_TYPE, self.signature, self.extra]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Authenticate(signature={0}, extra={1})".format(self.signature, self.extra)


class Goodbye(Message):
    """
    A WAMP ``GOODBYE`` message.

    Format: ``[GOODBYE, Details|dict, Reason|uri]``
    """

    MESSAGE_TYPE = 6
    """
    The WAMP message code for this type of message.
    """

    DEFAULT_REASON = "wamp.close.normal"
    """
    Default WAMP closing reason.
    """

    def __init__(self, reason=DEFAULT_REASON, message=None):
        """

        :param reason: Optional WAMP or application error URI for closing reason.
        :type reason: unicode
        :param message: Optional human-readable closing message, e.g. for logging purposes.
        :type message: unicode or None
        """
        assert(isinstance(reason, string_types))
        assert(message is None or isinstance(message, string_types))

        Message.__init__(self)
        self.reason = reason
        self.message = message

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        ##
        assert(len(wmsg) > 0 and wmsg[0] == Goodbye.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for GOODBYE".format(len(wmsg)))

        details = check_or_raise_extra(wmsg[1], "'details' in GOODBYE")
        reason = check_or_raise_uri(wmsg[2], "'reason' in GOODBYE")

        message = None

        if 'message' in details:

            details_message = details['message']
            if not isinstance(details_message, string_types):
                raise ProtocolError("invalid type {0} for 'message' detail in GOODBYE".format(type(details_message)))

            message = details_message

        obj = Goodbye(reason, message)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {}
        if self.message:
            details['message'] = self.message

        return [Goodbye.MESSAGE_TYPE, details, self.reason]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Goodbye(message={0}, reason={1})".format(self.message, self.reason)


class Error(Message):
    """
    A WAMP ``ERROR`` message.

    Formats:

    * ``[ERROR, REQUEST.Type|int, REQUEST.Request|id, Details|dict, Error|uri]``
    * ``[ERROR, REQUEST.Type|int, REQUEST.Request|id, Details|dict, Error|uri, Arguments|list]``
    * ``[ERROR, REQUEST.Type|int, REQUEST.Request|id, Details|dict, Error|uri, Arguments|list, ArgumentsKw|dict]``
    * ``[ERROR, REQUEST.Type|int, REQUEST.Request|id, Details|dict, Error|uri, Payload|string/binary]``
    """

    MESSAGE_TYPE = 8
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request_type, request, error, args=None, kwargs=None, payload=None,
                 enc_algo=None, enc_key=None, enc_serializer=None):
        """

        :param request_type: The WAMP message type code for the original request.
        :type request_type: int
        :param request: The WAMP request ID of the original request (`Call`, `Subscribe`, ...) this error occurred for.
        :type request: int
        :param error: The WAMP or application error URI for the error that occurred.
        :type error: unicode
        :param args: Positional values for application-defined exception.
           Must be serializable using any serializers in use.
        :type args: list or None
        :param kwargs: Keyword values for application-defined exception.
           Must be serializable using any serializers in use.
        :type kwargs: dict or None
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(request_type) is int)
        assert (type(request) is int)
        assert(isinstance(error, string_types))
        assert(args is None or type(args) in [list, tuple])
        assert(kwargs is None or type(kwargs) == dict)
        assert(payload is None or type(payload) in (string_types, binary_type))
        assert(payload is None or (payload is not None and args is None and kwargs is None))

        Message.__init__(self)
        self.request_type = request_type
        self.request = request
        self.error = error
        self.args = args
        self.kwargs = kwargs
        self.payload = payload

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Error.MESSAGE_TYPE)

        if len(wmsg) not in (5, 6, 7):
            raise ProtocolError("invalid message length {0} for ERROR".format(len(wmsg)))

        request_type = wmsg[1]
        if type(request_type) is not int:
            raise ProtocolError("invalid type {0} for 'request_type' in ERROR".format(request_type))

        if request_type not in [Subscribe.MESSAGE_TYPE,
                                Unsubscribe.MESSAGE_TYPE,
                                Publish.MESSAGE_TYPE,
                                Register.MESSAGE_TYPE,
                                Unregister.MESSAGE_TYPE,
                                Call.MESSAGE_TYPE,
                                Invocation.MESSAGE_TYPE]:
            raise ProtocolError("invalid value {0} for 'request_type' in ERROR".format(request_type))

        request = check_or_raise_id(wmsg[2], "'request' in ERROR")
        details = check_or_raise_extra(wmsg[3], "'details' in ERROR")
        error = check_or_raise_uri(wmsg[4], "'error' in ERROR")

        args = None
        kwargs = None
        payload = None
        enc_algo = None
        enc_key = None
        enc_serializer = None

        if len(wmsg) == 6 and type(wmsg[5]) in (string_types, binary_type):

            payload = wmsg[5]

            enc_algo = details.get('enc_algo', None)
            if enc_algo and enc_algo not in [PAYLOAD_ENC_CRYPTO_BOX]:
                raise ProtocolError("invalid value {0} for 'enc_algo' detail in EVENT".format(enc_algo))

            enc_key = details.get('enc_key', None)
            if enc_key and type(enc_key) not in (string_types, binary_type):
                raise ProtocolError("invalid type {0} for 'enc_key' detail in EVENT".format(type(enc_key)))

            enc_serializer = details.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' detail in EVENT".format(enc_serializer))

        else:
            if len(wmsg) > 5:
                args = wmsg[5]
                if type(args) != list:
                    raise ProtocolError("invalid type {0} for 'args' in ERROR".format(type(args)))

            if len(wmsg) > 6:
                kwargs = wmsg[6]
                if type(kwargs) != dict:
                    raise ProtocolError("invalid type {0} for 'kwargs' in ERROR".format(type(kwargs)))

        obj = Error(request_type,
                    request,
                    error,
                    args=args,
                    kwargs=kwargs,
                    payload=payload,
                    enc_algo=enc_algo,
                    enc_key=enc_key,
                    enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {}

        if self.payload:
            if self.enc_algo is not None:
                details['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                details['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                details['enc_serializer'] = self.enc_serializer
            return [self.MESSAGE_TYPE, self.request_type, self.request, details, self.error, self.payload]
        else:
            if self.kwargs:
                return [self.MESSAGE_TYPE, self.request_type, self.request, details, self.error, self.args, self.kwargs]
            elif self.args:
                return [self.MESSAGE_TYPE, self.request_type, self.request, details, self.error, self.args]
            else:
                return [self.MESSAGE_TYPE, self.request_type, self.request, details, self.error]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Error(request_type={0}, request={1}, error={2}, args={3}, kwargs={4}, enc_algo={5}, enc_key={6}, enc_serializer={7}, payload={8})".format(self.request_type, self.request, self.error, self.args, self.kwargs, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


class Publish(Message):
    """
    A WAMP ``PUBLISH`` message.

    Formats:

    * ``[PUBLISH, Request|id, Options|dict, Topic|uri]``
    * ``[PUBLISH, Request|id, Options|dict, Topic|uri, Arguments|list]``
    * ``[PUBLISH, Request|id, Options|dict, Topic|uri, Arguments|list, ArgumentsKw|dict]``
    * ``[PUBLISH, Request|id, Options|dict, Topic|uri, Payload|string/binary]``
    """

    MESSAGE_TYPE = 16
    """
    The WAMP message code for this type of message.
    """

    def __init__(self,
                 request,
                 topic,
                 args=None,
                 kwargs=None,
                 payload=None,
                 acknowledge=None,
                 exclude_me=None,
                 exclude=None,
                 exclude_authid=None,
                 exclude_authrole=None,
                 eligible=None,
                 eligible_authid=None,
                 eligible_authrole=None,
                 enc_algo=None,
                 enc_key=None,
                 enc_serializer=None):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param topic: The WAMP or application URI of the PubSub topic the event should
           be published to.
        :type topic: unicode
        :param args: Positional values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type args: list or tuple or None
        :param kwargs: Keyword values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type kwargs: dict or None
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :param acknowledge: If True, acknowledge the publication with a success or
           error response.
        :type acknowledge: bool or None
        :param exclude_me: If ``True``, exclude the publisher from receiving the event, even
           if he is subscribed (and eligible).
        :type exclude_me: bool or None
        :param exclude: List of WAMP session IDs to exclude from receiving this event.
        :type exclude: list of int or None
        :param exclude_authid: List of WAMP authids to exclude from receiving this event.
        :type exclude_authid: list of unicode or None
        :param exclude_authrole: List of WAMP authroles to exclude from receiving this event.
        :type exclude_authrole: list of unicode or None
        :param eligible: List of WAMP session IDs eligible to receive this event.
        :type eligible: list of int or None
        :param eligible_authid: List of WAMP authids eligible to receive this event.
        :type eligible_authid: list of unicode or None
        :param eligible_authrole: List of WAMP authroles eligible to receive this event.
        :type eligible_authrole: list of unicode or None
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(request) is int)
        assert(isinstance(topic, string_types))
        assert(args is None or type(args) in [list, tuple, string_types, binary_type])
        assert(kwargs is None or type(kwargs) in [dict, string_types, binary_type])
        assert(payload is None or type(payload) in (string_types, binary_type))
        assert(payload is None or (payload is not None and args is None and kwargs is None))
        assert(acknowledge is None or type(acknowledge) == bool)

        # publisher exlusion and black-/whitelisting
        assert(exclude_me is None or type(exclude_me) == bool)

        assert(exclude is None or type(exclude) == list)
        if exclude:
            for sessionid in exclude:
                assert (type(sessionid) is int)

        assert(exclude_authid is None or type(exclude_authid) == list)
        if exclude_authid:
            for authid in exclude_authid:
                assert(isinstance(authid, string_types))

        assert(exclude_authrole is None or type(exclude_authrole) == list)
        if exclude_authrole:
            for authrole in exclude_authrole:
                assert(isinstance(authrole, string_types))

        assert(eligible is None or type(eligible) == list)
        if eligible:
            for sessionid in eligible:
                assert (type(sessionid) is int)

        assert(eligible_authid is None or type(eligible_authid) == list)
        if eligible_authid:
            for authid in eligible_authid:
                assert(isinstance(authid, string_types))

        assert(eligible_authrole is None or type(eligible_authrole) == list)
        if eligible_authrole:
            for authrole in eligible_authrole:
                assert(isinstance(authrole, string_types))

        # end-to-end app payload encryption
        assert(enc_algo is None or enc_algo in [PAYLOAD_ENC_CRYPTO_BOX])
        assert(enc_key is None or type(enc_key) in (string_types, binary_type))
        assert(enc_serializer is None or enc_serializer in ['json', 'msgpack', 'cbor', 'ubjson'])
        assert((enc_algo is None and enc_key is None and enc_serializer is None) or (enc_algo is not None and payload is not None))

        Message.__init__(self)
        self.request = request
        self.topic = topic
        self.args = args
        self.kwargs = kwargs
        self.payload = payload
        self.acknowledge = acknowledge

        # publisher exlusion and black-/whitelisting
        self.exclude_me = exclude_me
        self.exclude = exclude
        self.exclude_authid = exclude_authid
        self.exclude_authrole = exclude_authrole
        self.eligible = eligible
        self.eligible_authid = eligible_authid
        self.eligible_authrole = eligible_authrole

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Publish.MESSAGE_TYPE)

        if len(wmsg) not in (4, 5, 6):
            raise ProtocolError("invalid message length {0} for PUBLISH".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in PUBLISH")
        options = check_or_raise_extra(wmsg[2], "'options' in PUBLISH")
        topic = check_or_raise_uri(wmsg[3], "'topic' in PUBLISH")

        args = None
        kwargs = None
        payload = None

        if len(wmsg) == 5 and type(wmsg[4]) in (string_types, binary_type):

            payload = wmsg[4]

            enc_algo = options.get('enc_algo', None)
            if enc_algo and enc_algo != PAYLOAD_ENC_CRYPTO_BOX:
                raise ProtocolError("invalid value {0} for 'enc_algo' option in PUBLISH".format(enc_algo))

            enc_key = options.get('enc_key', None)
            if enc_key and type(enc_key) not in (string_types, binary_type):
                raise ProtocolError("invalid type {0} for 'enc_key' option in PUBLISH".format(type(enc_key)))

            enc_serializer = options.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' option in PUBLISH".format(enc_serializer))

        else:
            if len(wmsg) > 4:
                args = wmsg[4]
                if type(args) not in [list, string_types, binary_type]:
                    raise ProtocolError("invalid type {0} for 'args' in PUBLISH".format(type(args)))

            if len(wmsg) > 5:
                kwargs = wmsg[5]
                if type(kwargs) not in [dict, string_types, binary_type]:
                    raise ProtocolError("invalid type {0} for 'kwargs' in PUBLISH".format(type(kwargs)))

            enc_algo = None
            enc_key = None
            enc_serializer = None

        acknowledge = None

        exclude_me = None
        exclude = None
        exclude_authid = None
        exclude_authrole = None
        eligible = None
        eligible_authid = None
        eligible_authrole = None

        if 'acknowledge' in options:

            option_acknowledge = options['acknowledge']
            if type(option_acknowledge) != bool:
                raise ProtocolError("invalid type {0} for 'acknowledge' option in PUBLISH".format(type(option_acknowledge)))

            acknowledge = option_acknowledge

        if 'exclude_me' in options:

            option_exclude_me = options['exclude_me']
            if type(option_exclude_me) != bool:
                raise ProtocolError("invalid type {0} for 'exclude_me' option in PUBLISH".format(type(option_exclude_me)))

            exclude_me = option_exclude_me

        if 'exclude' in options:

            option_exclude = options['exclude']
            if type(option_exclude) != list:
                raise ProtocolError("invalid type {0} for 'exclude' option in PUBLISH".format(type(option_exclude)))

            for _sessionid in option_exclude:
                if type(_sessionid) is not int:
                    raise ProtocolError("invalid type {0} for value in 'exclude' option in PUBLISH".format(type(_sessionid)))

            exclude = option_exclude

        if 'exclude_authid' in options:

            option_exclude_authid = options['exclude_authid']
            if type(option_exclude_authid) != list:
                raise ProtocolError("invalid type {0} for 'exclude_authid' option in PUBLISH".format(type(option_exclude_authid)))

            for _authid in option_exclude_authid:
                if not isinstance(_authid, string_types):
                    raise ProtocolError("invalid type {0} for value in 'exclude_authid' option in PUBLISH".format(type(_authid)))

            exclude_authid = option_exclude_authid

        if 'exclude_authrole' in options:

            option_exclude_authrole = options['exclude_authrole']
            if type(option_exclude_authrole) != list:
                raise ProtocolError("invalid type {0} for 'exclude_authrole' option in PUBLISH".format(type(option_exclude_authrole)))

            for _authrole in option_exclude_authrole:
                if not isinstance(_authrole, string_types):
                    raise ProtocolError("invalid type {0} for value in 'exclude_authrole' option in PUBLISH".format(type(_authrole)))

            exclude_authrole = option_exclude_authrole

        if 'eligible' in options:

            option_eligible = options['eligible']
            if type(option_eligible) != list:
                raise ProtocolError("invalid type {0} for 'eligible' option in PUBLISH".format(type(option_eligible)))

            for sessionId in option_eligible:
                if type(sessionId) is not int:
                    raise ProtocolError("invalid type {0} for value in 'eligible' option in PUBLISH".format(type(sessionId)))

            eligible = option_eligible

        if 'eligible_authid' in options:

            option_eligible_authid = options['eligible_authid']
            if type(option_eligible_authid) != list:
                raise ProtocolError("invalid type {0} for 'eligible_authid' option in PUBLISH".format(type(option_eligible_authid)))

            for _authid in option_eligible_authid:
                if not isinstance(_authid, string_types):
                    raise ProtocolError("invalid type {0} for value in 'eligible_authid' option in PUBLISH".format(type(_authid)))

            eligible_authid = option_eligible_authid

        if 'eligible_authrole' in options:

            option_eligible_authrole = options['eligible_authrole']
            if type(option_eligible_authrole) != list:
                raise ProtocolError("invalid type {0} for 'eligible_authrole' option in PUBLISH".format(type(option_eligible_authrole)))

            for _authrole in option_eligible_authrole:
                if not isinstance(_authrole, string_types):
                    raise ProtocolError("invalid type {0} for value in 'eligible_authrole' option in PUBLISH".format(type(_authrole)))

            eligible_authrole = option_eligible_authrole

        obj = Publish(request,
                      topic,
                      args=args,
                      kwargs=kwargs,
                      payload=payload,
                      acknowledge=acknowledge,
                      exclude_me=exclude_me,
                      exclude=exclude,
                      exclude_authid=exclude_authid,
                      exclude_authrole=exclude_authrole,
                      eligible=eligible,
                      eligible_authid=eligible_authid,
                      eligible_authrole=eligible_authrole,
                      enc_algo=enc_algo,
                      enc_key=enc_key,
                      enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.acknowledge is not None:
            options['acknowledge'] = self.acknowledge

        if self.exclude_me is not None:
            options['exclude_me'] = self.exclude_me
        if self.exclude is not None:
            options['exclude'] = self.exclude
        if self.exclude_authid is not None:
            options['exclude_authid'] = self.exclude_authid
        if self.eligible_authrole is not None:
            options['eligible_authrole'] = self.eligible_authrole
        if self.eligible is not None:
            options['eligible'] = self.eligible
        if self.eligible_authid is not None:
            options['eligible_authid'] = self.eligible_authid
        if self.eligible_authrole is not None:
            options['eligible_authrole'] = self.eligible_authrole

        if self.payload:
            if self.enc_algo is not None:
                options['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                options['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                options['enc_serializer'] = self.enc_serializer
            return [Publish.MESSAGE_TYPE, self.request, options, self.topic, self.payload]
        else:
            if self.kwargs:
                return [Publish.MESSAGE_TYPE, self.request, options, self.topic, self.args, self.kwargs]
            elif self.args:
                return [Publish.MESSAGE_TYPE, self.request, options, self.topic, self.args]
            else:
                return [Publish.MESSAGE_TYPE, self.request, options, self.topic]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Publish(request={0}, topic={1}, args={2}, kwargs={3}, acknowledge={4}, exclude_me={5}, exclude={6}, exclude_authid={7}, exclude_authrole={8}, eligible={9}, eligible_authid={10}, eligible_authrole={11}, enc_algo={12}, enc_key={13}, enc_serializer={14}, payload={15})".format(self.request, self.topic, self.args, self.kwargs, self.acknowledge, self.exclude_me, self.exclude, self.exclude_authid, self.exclude_authrole, self.eligible, self.eligible_authid, self.eligible_authrole, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


class Published(Message):
    """
    A WAMP ``PUBLISHED`` message.

    Format: ``[PUBLISHED, PUBLISH.Request|id, Publication|id]``
    """

    MESSAGE_TYPE = 17
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, publication):
        """

        :param request: The request ID of the original `PUBLISH` request.
        :type request: int
        :param publication: The publication ID for the published event.
        :type publication: int
        """
        assert (type(request) is int)
        assert (type(publication) is int)

        Message.__init__(self)
        self.request = request
        self.publication = publication

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Published.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for PUBLISHED".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in PUBLISHED")
        publication = check_or_raise_id(wmsg[2], "'publication' in PUBLISHED")

        obj = Published(request, publication)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Published.MESSAGE_TYPE, self.request, self.publication]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Published(request={0}, publication={1})".format(self.request, self.publication)


class Subscribe(Message):
    """
    A WAMP ``SUBSCRIBE`` message.

    Format: ``[SUBSCRIBE, Request|id, Options|dict, Topic|uri]``
    """

    MESSAGE_TYPE = 32
    """
    The WAMP message code for this type of message.
    """

    MATCH_EXACT = 'exact'
    MATCH_PREFIX = 'prefix'
    MATCH_WILDCARD = 'wildcard'

    def __init__(self, request, topic, match=None):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param topic: The WAMP or application URI of the PubSub topic to subscribe to.
        :type topic: unicode
        :param match: The topic matching method to be used for the subscription.
        :type match: unicode
        """
        assert (type(request) is int)
        assert(isinstance(topic, string_types))
        assert(match is None or isinstance(match, string_types))
        assert(match is None or match in [Subscribe.MATCH_EXACT, Subscribe.MATCH_PREFIX, Subscribe.MATCH_WILDCARD])

        Message.__init__(self)
        self.request = request
        self.topic = topic
        self.match = match or Subscribe.MATCH_EXACT

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Subscribe.MESSAGE_TYPE)

        if len(wmsg) != 4:
            raise ProtocolError("invalid message length {0} for SUBSCRIBE".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in SUBSCRIBE")
        options = check_or_raise_extra(wmsg[2], "'options' in SUBSCRIBE")
        topic = check_or_raise_uri(wmsg[3], "'topic' in SUBSCRIBE", allow_empty_components=True)

        match = Subscribe.MATCH_EXACT

        if 'match' in options:

            option_match = options['match']
            if not isinstance(option_match, string_types):
                raise ProtocolError("invalid type {0} for 'match' option in SUBSCRIBE".format(type(option_match)))

            if option_match not in [Subscribe.MATCH_EXACT, Subscribe.MATCH_PREFIX, Subscribe.MATCH_WILDCARD]:
                raise ProtocolError("invalid value {0} for 'match' option in SUBSCRIBE".format(option_match))

            match = option_match

        obj = Subscribe(request, topic, match=match)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.match and self.match != Subscribe.MATCH_EXACT:
            options['match'] = self.match

        return [Subscribe.MESSAGE_TYPE, self.request, options, self.topic]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Subscribe(request={0}, topic={1}, match={2})".format(self.request, self.topic, self.match)


class Subscribed(Message):
    """
    A WAMP ``SUBSCRIBED`` message.

    Format: ``[SUBSCRIBED, SUBSCRIBE.Request|id, Subscription|id]``
    """

    MESSAGE_TYPE = 33
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, subscription):
        """

        :param request: The request ID of the original ``SUBSCRIBE`` request.
        :type request: int
        :param subscription: The subscription ID for the subscribed topic (or topic pattern).
        :type subscription: int
        """
        assert (type(request) is int)
        assert (type(subscription) is int)

        Message.__init__(self)
        self.request = request
        self.subscription = subscription

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Subscribed.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for SUBSCRIBED".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in SUBSCRIBED")
        subscription = check_or_raise_id(wmsg[2], "'subscription' in SUBSCRIBED")

        obj = Subscribed(request, subscription)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Subscribed.MESSAGE_TYPE, self.request, self.subscription]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Subscribed(request={0}, subscription={1})".format(self.request, self.subscription)


class Unsubscribe(Message):
    """
    A WAMP ``UNSUBSCRIBE`` message.

    Format: ``[UNSUBSCRIBE, Request|id, SUBSCRIBED.Subscription|id]``
    """

    MESSAGE_TYPE = 34
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, subscription):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param subscription: The subscription ID for the subscription to unsubscribe from.
        :type subscription: int
        """
        assert (type(request) is int)
        assert (type(subscription) is int)

        Message.__init__(self)
        self.request = request
        self.subscription = subscription

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Unsubscribe.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for WAMP UNSUBSCRIBE".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in UNSUBSCRIBE")
        subscription = check_or_raise_id(wmsg[2], "'subscription' in UNSUBSCRIBE")

        obj = Unsubscribe(request, subscription)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Unsubscribe.MESSAGE_TYPE, self.request, self.subscription]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Unsubscribe(request={0}, subscription={1})".format(self.request, self.subscription)


class Unsubscribed(Message):
    """
    A WAMP ``UNSUBSCRIBED`` message.

    Formats:

    * ``[UNSUBSCRIBED, UNSUBSCRIBE.Request|id]``
    * ``[UNSUBSCRIBED, UNSUBSCRIBE.Request|id, Details|dict]``
    """

    MESSAGE_TYPE = 35
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, subscription=None, reason=None):
        """

        :param request: The request ID of the original ``UNSUBSCRIBE`` request or
            ``0`` is router triggered unsubscribe ("router revocation signaling").
        :type request: int
        :param subscription: If unsubscribe was actively triggered by router, the ID
            of the subscription revoked.
        :type subscription: int or None
        :param reason: The reason (an URI) for revocation.
        :type reason: unicode or None.
        """
        assert (type(request) is int)
        assert (subscription is None or type(subscription) is int)
        assert(reason is None or isinstance(reason, string_types))
        assert((request != 0 and subscription is None) or (request == 0 and subscription != 0))

        Message.__init__(self)
        self.request = request
        self.subscription = subscription
        self.reason = reason

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Unsubscribed.MESSAGE_TYPE)

        if len(wmsg) not in [2, 3]:
            raise ProtocolError("invalid message length {0} for UNSUBSCRIBED".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in UNSUBSCRIBED")

        subscription = None
        reason = None

        if len(wmsg) > 2:

            details = check_or_raise_extra(wmsg[2], "'details' in UNSUBSCRIBED")

            if "subscription" in details:
                details_subscription = details["subscription"]
                if type(details_subscription) is not int:
                    raise ProtocolError("invalid type {0} for 'subscription' detail in UNSUBSCRIBED".format(type(details_subscription)))
                subscription = details_subscription

            if "reason" in details:
                reason = check_or_raise_uri(details["reason"], "'reason' in UNSUBSCRIBED")

        obj = Unsubscribed(request, subscription, reason)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        if self.reason is not None or self.subscription is not None:
            details = {}
            if self.reason is not None:
                details["reason"] = self.reason
            if self.subscription is not None:
                details["subscription"] = self.subscription
            return [Unsubscribed.MESSAGE_TYPE, self.request, details]
        else:
            return [Unsubscribed.MESSAGE_TYPE, self.request]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Unsubscribed(request={0}, reason={1}, subscription={2})".format(self.request, self.reason, self.subscription)


class Event(Message):
    """
    A WAMP ``EVENT`` message.

    Formats:

    * ``[EVENT, SUBSCRIBED.Subscription|id, PUBLISHED.Publication|id, Details|dict]``
    * ``[EVENT, SUBSCRIBED.Subscription|id, PUBLISHED.Publication|id, Details|dict, PUBLISH.Arguments|list]``
    * ``[EVENT, SUBSCRIBED.Subscription|id, PUBLISHED.Publication|id, Details|dict, PUBLISH.Arguments|list, PUBLISH.ArgumentsKw|dict]``
    * ``[EVENT, SUBSCRIBED.Subscription|id, PUBLISHED.Publication|id, Details|dict, PUBLISH.Payload|string/binary]``
    """

    MESSAGE_TYPE = 36
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, subscription, publication, args=None, kwargs=None, payload=None,
                 publisher=None, publisher_authid=None, publisher_authrole=None, topic=None,
                 enc_algo=None, enc_key=None, enc_serializer=None):
        """

        :param subscription: The subscription ID this event is dispatched under.
        :type subscription: int
        :param publication: The publication ID of the dispatched event.
        :type publication: int
        :param args: Positional values for application-defined exception.
           Must be serializable using any serializers in use.
        :type args: list or tuple or None
        :param kwargs: Keyword values for application-defined exception.
           Must be serializable using any serializers in use.
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :type kwargs: dict or None
        :param publisher: The WAMP session ID of the pubisher. Only filled if pubisher is disclosed.
        :type publisher: None or int
        :param publisher_authid: The WAMP authid of the pubisher. Only filled if pubisher is disclosed.
        :type publisher_authid: None or unicode
        :param publisher_authrole: The WAMP authrole of the pubisher. Only filled if pubisher is disclosed.
        :type publisher_authrole: None or unicode
        :param topic: For pattern-based subscriptions, the event MUST contain the actual topic published to.
        :type topic: unicode or None
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(subscription) is int)
        assert (type(publication) is int)
        assert(args is None or type(args) in [list, tuple])
        assert(kwargs is None or type(kwargs) == dict)
        assert(payload is None or (isinstance(payload, string_types) or isinstance(payload, binary_type)))
        assert(payload is None or (payload is not None and args is None and kwargs is None))
        assert (publisher is None or type(publisher) is int)
        assert(publisher_authid is None or isinstance(publisher_authid, string_types))
        assert(publisher_authrole is None or isinstance(publisher_authrole, string_types))
        assert(topic is None or isinstance(topic, string_types))

        # end-to-end app payload encryption
        assert(enc_algo is None or enc_algo in [PAYLOAD_ENC_CRYPTO_BOX])
        assert(enc_key is None or (isinstance(enc_key, string_types) or isinstance(enc_key, binary_type)))
        assert(enc_serializer is None or enc_serializer in ['json', 'msgpack', 'cbor', 'ubjson'])
        assert((enc_algo is None and enc_key is None and enc_serializer is None) or (enc_algo is not None and payload is not None))

        Message.__init__(self)
        self.subscription = subscription
        self.publication = publication
        self.args = args
        self.kwargs = kwargs
        self.payload = payload
        self.publisher = publisher
        self.publisher_authid = publisher_authid
        self.publisher_authrole = publisher_authrole
        self.topic = topic

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Event.MESSAGE_TYPE)

        if len(wmsg) not in (4, 5, 6):
            raise ProtocolError("invalid message length {0} for EVENT".format(len(wmsg)))

        subscription = check_or_raise_id(wmsg[1], "'subscription' in EVENT")
        publication = check_or_raise_id(wmsg[2], "'publication' in EVENT")
        details = check_or_raise_extra(wmsg[3], "'details' in EVENT")

        args = None
        kwargs = None
        payload = None
        enc_algo = None
        enc_key = None
        enc_serializer = None

        if len(wmsg) == 5 and (isinstance(wmsg[4], string_types) or isinstance(wmsg[4], binary_type)):

            payload = wmsg[4]

            enc_algo = details.get('enc_algo', None)
            if enc_algo and enc_algo not in [PAYLOAD_ENC_CRYPTO_BOX]:
                raise ProtocolError("invalid value {0} for 'enc_algo' detail in EVENT".format(enc_algo))

            enc_key = details.get('enc_key', None)
            if enc_key and (not isinstance(enc_key, string_types) and not isinstance(enc_key, binary_type)):
                raise ProtocolError("invalid type {0} for 'enc_key' detail in EVENT".format(type(enc_key)))

            enc_serializer = details.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' detail in EVENT".format(enc_serializer))

        else:
            if len(wmsg) > 4:
                args = wmsg[4]
                if not (isinstance(args, list) or isinstance(args, string_types) or isinstance(args, binary_type)):
                    raise ProtocolError("invalid type {0} for 'args' in EVENT".format(type(args)))
            if len(wmsg) > 5:
                kwargs = wmsg[5]
                if type(kwargs) not in [dict]:
                    raise ProtocolError("invalid type {0} for 'kwargs' in EVENT".format(type(kwargs)))

        publisher = None
        publisher_authid = None
        publisher_authrole = None
        topic = None

        if 'publisher' in details:

            detail_publisher = details['publisher']
            if type(detail_publisher) is not int:
                raise ProtocolError("invalid type {0} for 'publisher' detail in EVENT".format(type(detail_publisher)))

            publisher = detail_publisher

        if 'publisher_authid' in details:

            detail_publisher_authid = details['publisher_authid']
            if not isinstance(detail_publisher_authid, string_types):
                raise ProtocolError("invalid type {0} for 'publisher_authid' detail in INVOCATION".format(type(detail_publisher_authid)))

            publisher_authid = detail_publisher_authid

        if 'publisher_authrole' in details:

            detail_publisher_authrole = details['publisher_authrole']
            if not isinstance(detail_publisher_authrole, string_types):
                raise ProtocolError("invalid type {0} for 'publisher_authrole' detail in INVOCATION".format(type(detail_publisher_authrole)))

            publisher_authrole = detail_publisher_authrole

        if 'topic' in details:

            detail_topic = details['topic']
            if not isinstance(detail_topic, string_types):
                raise ProtocolError("invalid type {0} for 'topic' detail in EVENT".format(type(detail_topic)))

            topic = detail_topic

        obj = Event(subscription,
                    publication,
                    args=args,
                    kwargs=kwargs,
                    payload=payload,
                    publisher=publisher,
                    publisher_authid=publisher_authid,
                    publisher_authrole=publisher_authrole,
                    topic=topic,
                    enc_algo=enc_algo,
                    enc_key=enc_key,
                    enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {}

        if self.publisher is not None:
            details['publisher'] = self.publisher

        if self.publisher_authid is not None:
            details['publisher_authid'] = self.publisher_authid

        if self.publisher_authrole is not None:
            details['publisher_authrole'] = self.publisher_authrole

        if self.topic is not None:
            details['topic'] = self.topic

        if self.payload:
            if self.enc_algo is not None:
                details['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                details['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                details['enc_serializer'] = self.enc_serializer
            return [Event.MESSAGE_TYPE, self.subscription, self.publication, details, self.payload]
        else:
            if self.kwargs:
                return [Event.MESSAGE_TYPE, self.subscription, self.publication, details, self.args, self.kwargs]
            elif self.args:
                return [Event.MESSAGE_TYPE, self.subscription, self.publication, details, self.args]
            else:
                return [Event.MESSAGE_TYPE, self.subscription, self.publication, details]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Event(subscription={0}, publication={1}, args={2}, kwargs={3}, publisher={4}, publisher_authid={5}, publisher_authrole={6}, topic={7}, enc_algo={8}, enc_key={9}, enc_serializer={9}, payload={10})".format(self.subscription, self.publication, self.args, self.kwargs, self.publisher, self.publisher_authid, self.publisher_authrole, self.topic, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


class Call(Message):
    """
    A WAMP ``CALL`` message.

    Formats:

    * ``[CALL, Request|id, Options|dict, Procedure|uri]``
    * ``[CALL, Request|id, Options|dict, Procedure|uri, Arguments|list]``
    * ``[CALL, Request|id, Options|dict, Procedure|uri, Arguments|list, ArgumentsKw|dict]``
    * ``[CALL, Request|id, Options|dict, Procedure|uri, Payload|string/binary]``
    """

    MESSAGE_TYPE = 48
    """
    The WAMP message code for this type of message.
    """

    def __init__(self,
                 request,
                 procedure,
                 args=None,
                 kwargs=None,
                 payload=None,
                 timeout=None,
                 receive_progress=None,
                 enc_algo=None,
                 enc_key=None,
                 enc_serializer=None):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param procedure: The WAMP or application URI of the procedure which should be called.
        :type procedure: unicode
        :param args: Positional values for application-defined call arguments.
           Must be serializable using any serializers in use.
        :type args: list or tuple or None
        :param kwargs: Keyword values for application-defined call arguments.
           Must be serializable using any serializers in use.
        :type kwargs: dict or None
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :param timeout: If present, let the callee automatically cancel
           the call after this ms.
        :type timeout: int or None
        :param receive_progress: If ``True``, indicates that the caller wants to receive
           progressive call results.
        :type receive_progress: bool or None
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(request) is int)
        assert(isinstance(procedure, string_types))
        assert(args is None or type(args) in [list, tuple])
        assert(kwargs is None or type(kwargs) == dict)
        assert(payload is None or (isinstance(payload, string_types) or isinstance(payload, binary_type)))
        assert(payload is None or (payload is not None and args is None and kwargs is None))
        assert (timeout is None or type(timeout) is int)
        assert(receive_progress is None or type(receive_progress) == bool)

        # end-to-end app payload encryption
        assert(enc_algo is None or enc_algo in [PAYLOAD_ENC_CRYPTO_BOX])
        assert(enc_key is None or (isinstance(enc_key, string_types) or isinstance(enc_key, binary_type)))
        assert(enc_serializer is None or enc_serializer in ['json', 'msgpack', 'cbor', 'ubjson'])
        assert((enc_algo is None and enc_key is None and enc_serializer is None) or (enc_algo is not None and payload is not None))

        Message.__init__(self)
        self.request = request
        self.procedure = procedure
        self.args = args
        self.kwargs = kwargs
        self.payload = payload
        self.timeout = timeout
        self.receive_progress = receive_progress

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Call.MESSAGE_TYPE)

        if len(wmsg) not in (4, 5, 6):
            raise ProtocolError("invalid message length {0} for CALL".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in CALL")
        options = check_or_raise_extra(wmsg[2], "'options' in CALL")
        procedure = check_or_raise_uri(wmsg[3], "'procedure' in CALL")

        args = None
        kwargs = None
        payload = None
        enc_algo = None
        enc_key = None
        enc_serializer = None

        if len(wmsg) == 5 and (isinstance(wmsg[4], string_types) or isinstance(wmsg[4], binary_type)):

            payload = wmsg[4]

            enc_algo = options.get('enc_algo', None)
            if enc_algo and enc_algo not in [PAYLOAD_ENC_CRYPTO_BOX]:
                raise ProtocolError("invalid value {0} for 'enc_algo' detail in EVENT".format(enc_algo))

            enc_key = options.get('enc_key', None)
            if enc_key and (not isinstance(enc_key, string_types) and not isinstance(enc_key, binary_type)):
                raise ProtocolError("invalid type {0} for 'enc_key' detail in EVENT".format(type(enc_key)))

            enc_serializer = options.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' detail in EVENT".format(enc_serializer))

        else:
            if len(wmsg) > 4:
                args = wmsg[4]
                if type(args) != list:
                    raise ProtocolError("invalid type {0} for 'args' in CALL".format(type(args)))

            if len(wmsg) > 5:
                kwargs = wmsg[5]
                if type(kwargs) != dict:
                    raise ProtocolError("invalid type {0} for 'kwargs' in CALL".format(type(kwargs)))

        timeout = None
        receive_progress = None

        if 'timeout' in options:

            option_timeout = options['timeout']
            if type(option_timeout) is not int:
                raise ProtocolError("invalid type {0} for 'timeout' option in CALL".format(type(option_timeout)))

            if option_timeout < 0:
                raise ProtocolError("invalid value {0} for 'timeout' option in CALL".format(option_timeout))

            timeout = option_timeout

        if 'receive_progress' in options:

            option_receive_progress = options['receive_progress']
            if type(option_receive_progress) != bool:
                raise ProtocolError("invalid type {0} for 'receive_progress' option in CALL".format(type(option_receive_progress)))

            receive_progress = option_receive_progress

        obj = Call(request,
                   procedure,
                   args=args,
                   kwargs=kwargs,
                   payload=payload,
                   timeout=timeout,
                   receive_progress=receive_progress,
                   enc_algo=enc_algo,
                   enc_key=enc_key,
                   enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.timeout is not None:
            options['timeout'] = self.timeout

        if self.receive_progress is not None:
            options['receive_progress'] = self.receive_progress

        if self.payload:
            if self.enc_algo is not None:
                options['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                options['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                options['enc_serializer'] = self.enc_serializer
            return [Call.MESSAGE_TYPE, self.request, options, self.procedure, self.payload]
        else:
            if self.kwargs:
                return [Call.MESSAGE_TYPE, self.request, options, self.procedure, self.args, self.kwargs]
            elif self.args:
                return [Call.MESSAGE_TYPE, self.request, options, self.procedure, self.args]
            else:
                return [Call.MESSAGE_TYPE, self.request, options, self.procedure]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Call(request={0}, procedure={1}, args={2}, kwargs={3}, timeout={4}, receive_progress={5}, enc_algo={6}, enc_key={7}, enc_serializer={8}, payload={9})".format(self.request, self.procedure, self.args, self.kwargs, self.timeout, self.receive_progress, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


class Cancel(Message):
    """
    A WAMP ``CANCEL`` message.

    Format: ``[CANCEL, CALL.Request|id, Options|dict]``
    """

    MESSAGE_TYPE = 49
    """
    The WAMP message code for this type of message.
    """

    SKIP = 'skip'
    ABORT = 'abort'
    KILL = 'kill'

    def __init__(self, request, mode=None):
        """

        :param request: The WAMP request ID of the original `CALL` to cancel.
        :type request: int
        :param mode: Specifies how to cancel the call (``"skip"``, ``"abort"`` or ``"kill"``).
        :type mode: unicode or None
        """
        assert (type(request) is int)
        assert(mode is None or isinstance(mode, string_types))
        assert(mode in [None, self.SKIP, self.ABORT, self.KILL])

        Message.__init__(self)
        self.request = request
        self.mode = mode

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Cancel.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for CANCEL".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in CANCEL")
        options = check_or_raise_extra(wmsg[2], "'options' in CANCEL")

        # options
        #
        mode = None

        if 'mode' in options:

            option_mode = options['mode']
            if not isinstance(option_mode, string_types):
                raise ProtocolError("invalid type {0} for 'mode' option in CANCEL".format(type(option_mode)))

            if option_mode not in [Cancel.SKIP, Cancel.ABORT, Cancel.KILL]:
                raise ProtocolError("invalid value '{0}' for 'mode' option in CANCEL".format(option_mode))

            mode = option_mode

        obj = Cancel(request, mode=mode)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.mode is not None:
            options['mode'] = self.mode

        return [Cancel.MESSAGE_TYPE, self.request, options]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Cancel(request={0}, mode={1})".format(self.request, self.mode)


class Result(Message):
    """
    A WAMP ``RESULT`` message.

    Formats:

    * ``[RESULT, CALL.Request|id, Details|dict]``
    * ``[RESULT, CALL.Request|id, Details|dict, YIELD.Arguments|list]``
    * ``[RESULT, CALL.Request|id, Details|dict, YIELD.Arguments|list, YIELD.ArgumentsKw|dict]``
    * ``[RESULT, CALL.Request|id, Details|dict, Payload|string/binary]``
    """

    MESSAGE_TYPE = 50
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, args=None, kwargs=None, payload=None, progress=None,
                 enc_algo=None, enc_key=None, enc_serializer=None):
        """

        :param request: The request ID of the original `CALL` request.
        :type request: int
        :param args: Positional values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type args: list or tuple or None
        :param kwargs: Keyword values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type kwargs: dict or None
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :param progress: If ``True``, this result is a progressive call result, and subsequent
           results (or a final error) will follow.
        :type progress: bool or None
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(request) is int)
        assert(args is None or type(args) in [list, tuple])
        assert(kwargs is None or type(kwargs) == dict)
        assert(payload is None or (isinstance(payload, string_types) or isinstance(payload, binary_type)))
        assert(payload is None or (payload is not None and args is None and kwargs is None))
        assert(progress is None or type(progress) == bool)

        # end-to-end app payload encryption
        assert(enc_algo is None or enc_algo in [PAYLOAD_ENC_CRYPTO_BOX])
        assert(enc_key is None or (isinstance(enc_key, string_types) or isinstance(enc_key, binary_type)))
        assert(enc_serializer is None or enc_serializer in ['json', 'msgpack', 'cbor', 'ubjson'])
        assert((enc_algo is None and enc_key is None and enc_serializer is None) or (enc_algo is not None and payload is not None))

        Message.__init__(self)
        self.request = request
        self.args = args
        self.kwargs = kwargs
        self.payload = payload
        self.progress = progress

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Result.MESSAGE_TYPE)

        if len(wmsg) not in (3, 4, 5):
            raise ProtocolError("invalid message length {0} for RESULT".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in RESULT")
        details = check_or_raise_extra(wmsg[2], "'details' in RESULT")

        args = None
        kwargs = None
        payload = None
        enc_algo = None
        enc_key = None
        enc_serializer = None

        if len(wmsg) == 4 and (isinstance(wmsg[3], string_types) or isinstance(wmsg[3], binary_type)):

            payload = wmsg[3]

            enc_algo = details.get('enc_algo', None)
            if enc_algo and enc_algo not in [PAYLOAD_ENC_CRYPTO_BOX]:
                raise ProtocolError("invalid value {0} for 'enc_algo' detail in EVENT".format(enc_algo))

            enc_key = details.get('enc_key', None)
            if enc_key and (not isinstance(enc_key, string_types) and not isinstance(enc_key, binary_type)):
                raise ProtocolError("invalid type {0} for 'enc_key' detail in EVENT".format(type(enc_key)))

            enc_serializer = details.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' detail in EVENT".format(enc_serializer))

        else:
            if len(wmsg) > 3:
                args = wmsg[3]
                if type(args) != list:
                    raise ProtocolError("invalid type {0} for 'args' in RESULT".format(type(args)))

            if len(wmsg) > 4:
                kwargs = wmsg[4]
                if type(kwargs) != dict:
                    raise ProtocolError("invalid type {0} for 'kwargs' in RESULT".format(type(kwargs)))

        progress = None

        if 'progress' in details:

            detail_progress = details['progress']
            if type(detail_progress) != bool:
                raise ProtocolError("invalid type {0} for 'progress' option in RESULT".format(type(detail_progress)))

            progress = detail_progress

        obj = Result(request,
                     args=args,
                     kwargs=kwargs,
                     payload=payload,
                     progress=progress,
                     enc_algo=enc_algo,
                     enc_key=enc_key,
                     enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        details = {}

        if self.progress is not None:
            details['progress'] = self.progress

        if self.payload:
            if self.enc_algo is not None:
                details['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                details['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                details['enc_serializer'] = self.enc_serializer
            return [Result.MESSAGE_TYPE, self.request, details, self.payload]
        else:
            if self.kwargs:
                return [Result.MESSAGE_TYPE, self.request, details, self.args, self.kwargs]
            elif self.args:
                return [Result.MESSAGE_TYPE, self.request, details, self.args]
            else:
                return [Result.MESSAGE_TYPE, self.request, details]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Result(request={0}, args={1}, kwargs={2}, progress={3}, enc_algo={4}, enc_key={5}, enc_serializer={6}, payload={7})".format(self.request, self.args, self.kwargs, self.progress, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


class Register(Message):
    """
    A WAMP ``REGISTER`` message.

    Format: ``[REGISTER, Request|id, Options|dict, Procedure|uri]``
    """

    MESSAGE_TYPE = 64
    """
    The WAMP message code for this type of message.
    """

    MATCH_EXACT = 'exact'
    MATCH_PREFIX = 'prefix'
    MATCH_WILDCARD = 'wildcard'

    INVOKE_SINGLE = 'single'
    INVOKE_FIRST = 'first'
    INVOKE_LAST = 'last'
    INVOKE_ROUNDROBIN = 'roundrobin'
    INVOKE_RANDOM = 'random'
    INVOKE_ALL = 'all'

    def __init__(self, request, procedure, match=None, invoke=None):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param procedure: The WAMP or application URI of the RPC endpoint provided.
        :type procedure: unicode
        :param match: The procedure matching policy to be used for the registration.
        :type match: unicode
        :param invoke: The procedure invocation policy to be used for the registration.
        :type invoke: unicode
        """
        assert (type(request) is int)
        assert(isinstance(procedure, string_types))
        assert(match is None or isinstance(match, string_types))
        assert(match is None or match in [Register.MATCH_EXACT, Register.MATCH_PREFIX, Register.MATCH_WILDCARD])
        assert(invoke is None or isinstance(invoke, string_types))
        assert(invoke is None or invoke in [Register.INVOKE_SINGLE, Register.INVOKE_FIRST, Register.INVOKE_LAST, Register.INVOKE_ROUNDROBIN, Register.INVOKE_RANDOM])

        Message.__init__(self)
        self.request = request
        self.procedure = procedure
        self.match = match or Register.MATCH_EXACT
        self.invoke = invoke or Register.INVOKE_SINGLE

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Register.MESSAGE_TYPE)

        if len(wmsg) != 4:
            raise ProtocolError("invalid message length {0} for REGISTER".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in REGISTER")
        options = check_or_raise_extra(wmsg[2], "'options' in REGISTER")

        match = Register.MATCH_EXACT
        invoke = Register.INVOKE_SINGLE

        if 'match' in options:

            option_match = options['match']
            if not isinstance(option_match, string_types):
                raise ProtocolError("invalid type {0} for 'match' option in REGISTER".format(type(option_match)))

            if option_match not in [Register.MATCH_EXACT, Register.MATCH_PREFIX, Register.MATCH_WILDCARD]:
                raise ProtocolError("invalid value {0} for 'match' option in REGISTER".format(option_match))

            match = option_match

        if match == Register.MATCH_EXACT:
            allow_empty_components = False
            allow_last_empty = False

        elif match == Register.MATCH_PREFIX:
            allow_empty_components = False
            allow_last_empty = True

        elif match == Register.MATCH_WILDCARD:
            allow_empty_components = True
            allow_last_empty = False

        else:
            raise Exception("logic error")

        procedure = check_or_raise_uri(wmsg[3], "'procedure' in REGISTER", allow_empty_components=allow_empty_components, allow_last_empty=allow_last_empty)

        if 'invoke' in options:

            option_invoke = options['invoke']
            if not isinstance(option_invoke, string_types):
                raise ProtocolError("invalid type {0} for 'invoke' option in REGISTER".format(type(option_invoke)))

            if option_invoke not in [Register.INVOKE_SINGLE, Register.INVOKE_FIRST, Register.INVOKE_LAST, Register.INVOKE_ROUNDROBIN, Register.INVOKE_RANDOM]:
                raise ProtocolError("invalid value {0} for 'invoke' option in REGISTER".format(option_invoke))

            invoke = option_invoke

        obj = Register(request, procedure, match=match, invoke=invoke)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.match and self.match != Register.MATCH_EXACT:
            options['match'] = self.match

        if self.invoke and self.invoke != Register.INVOKE_SINGLE:
            options['invoke'] = self.invoke

        return [Register.MESSAGE_TYPE, self.request, options, self.procedure]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Register(request={0}, procedure={1}, match={2}, invoke={3})".format(self.request, self.procedure, self.match, self.invoke)


class Registered(Message):
    """
    A WAMP ``REGISTERED`` message.

    Format: ``[REGISTERED, REGISTER.Request|id, Registration|id]``
    """

    MESSAGE_TYPE = 65
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, registration):
        """

        :param request: The request ID of the original ``REGISTER`` request.
        :type request: int
        :param registration: The registration ID for the registered procedure (or procedure pattern).
        :type registration: int
        """
        assert (type(request) is int)
        assert (type(registration) is int)

        Message.__init__(self)
        self.request = request
        self.registration = registration

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Registered.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for REGISTERED".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in REGISTERED")
        registration = check_or_raise_id(wmsg[2], "'registration' in REGISTERED")

        obj = Registered(request, registration)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Registered.MESSAGE_TYPE, self.request, self.registration]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Registered(request={0}, registration={1})".format(self.request, self.registration)


class Unregister(Message):
    """
    A WAMP `UNREGISTER` message.

    Format: ``[UNREGISTER, Request|id, REGISTERED.Registration|id]``
    """

    MESSAGE_TYPE = 66
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, registration):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param registration: The registration ID for the registration to unregister.
        :type registration: int
        """
        assert (type(request) is int)
        assert (type(registration) is int)

        Message.__init__(self)
        self.request = request
        self.registration = registration

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Unregister.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for WAMP UNREGISTER".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in UNREGISTER")
        registration = check_or_raise_id(wmsg[2], "'registration' in UNREGISTER")

        obj = Unregister(request, registration)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        return [Unregister.MESSAGE_TYPE, self.request, self.registration]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Unregister(request={0}, registration={1})".format(self.request, self.registration)


class Unregistered(Message):
    """
    A WAMP ``UNREGISTERED`` message.

    Formats:

    * ``[UNREGISTERED, UNREGISTER.Request|id]``
    * ``[UNREGISTERED, UNREGISTER.Request|id, Details|dict]``
    """

    MESSAGE_TYPE = 67
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, registration=None, reason=None):
        """

        :param request: The request ID of the original ``UNREGISTER`` request.
        :type request: int
        :param registration: If unregister was actively triggered by router, the ID
            of the registration revoked.
        :type registration: int or None
        :param reason: The reason (an URI) for revocation.
        :type reason: unicode or None.
        """
        assert (type(request) is int)
        assert (registration is None or type(registration) is int)
        assert(reason is None or isinstance(reason, string_types))
        assert((request != 0 and registration is None) or (request == 0 and registration != 0))

        Message.__init__(self)
        self.request = request
        self.registration = registration
        self.reason = reason

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Unregistered.MESSAGE_TYPE)

        if len(wmsg) not in [2, 3]:
            raise ProtocolError("invalid message length {0} for UNREGISTERED".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in UNREGISTERED")

        registration = None
        reason = None

        if len(wmsg) > 2:

            details = check_or_raise_extra(wmsg[2], "'details' in UNREGISTERED")

            if "registration" in details:
                details_registration = details["registration"]
                if type(details_registration) is not int:
                    raise ProtocolError("invalid type {0} for 'registration' detail in UNREGISTERED".format(type(details_registration)))
                registration = details_registration

            if "reason" in details:
                reason = check_or_raise_uri(details["reason"], "'reason' in UNREGISTERED")

        obj = Unregistered(request, registration, reason)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        if self.reason is not None or self.registration is not None:
            details = {}
            if self.reason is not None:
                details["reason"] = self.reason
            if self.registration is not None:
                details["registration"] = self.registration
            return [Unregistered.MESSAGE_TYPE, self.request, details]
        else:
            return [Unregistered.MESSAGE_TYPE, self.request]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Unregistered(request={0}, reason={1}, registration={2})".format(self.request, self.reason, self.registration)


class Invocation(Message):
    """
    A WAMP ``INVOCATION`` message.

    Formats:

    * ``[INVOCATION, Request|id, REGISTERED.Registration|id, Details|dict]``
    * ``[INVOCATION, Request|id, REGISTERED.Registration|id, Details|dict, CALL.Arguments|list]``
    * ``[INVOCATION, Request|id, REGISTERED.Registration|id, Details|dict, CALL.Arguments|list, CALL.ArgumentsKw|dict]``
    * ``[INVOCATION, Request|id, REGISTERED.Registration|id, Details|dict, Payload|string/binary]``
    """

    MESSAGE_TYPE = 68
    """
    The WAMP message code for this type of message.
    """

    def __init__(self,
                 request,
                 registration,
                 args=None,
                 kwargs=None,
                 payload=None,
                 timeout=None,
                 receive_progress=None,
                 caller=None,
                 caller_authid=None,
                 caller_authrole=None,
                 procedure=None,
                 enc_algo=None,
                 enc_key=None,
                 enc_serializer=None):
        """

        :param request: The WAMP request ID of this request.
        :type request: int
        :param registration: The registration ID of the endpoint to be invoked.
        :type registration: int
        :param args: Positional values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type args: list or tuple or None
        :param kwargs: Keyword values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type kwargs: dict or None
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :param timeout: If present, let the callee automatically cancels
           the invocation after this ms.
        :type timeout: int or None
        :param receive_progress: Indicates if the callee should produce progressive results.
        :type receive_progress: bool or None
        :param caller: The WAMP session ID of the caller. Only filled if caller is disclosed.
        :type caller: None or int
        :param caller_authid: The WAMP authid of the caller. Only filled if caller is disclosed.
        :type caller_authid: None or unicode
        :param caller_authrole: The WAMP authrole of the caller. Only filled if caller is disclosed.
        :type caller_authrole: None or unicode
        :param procedure: For pattern-based registrations, the invocation MUST include the actual procedure being called.
        :type procedure: unicode or None
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(request) is int)
        assert (type(registration) is int)
        assert(args is None or type(args) in [list, tuple])
        assert(kwargs is None or type(kwargs) == dict)
        assert(payload is None or (isinstance(payload, string_types) or isinstance(payload, binary_type)))
        assert(payload is None or (payload is not None and args is None and kwargs is None))
        assert (timeout is None or type(timeout) is int)
        assert(receive_progress is None or type(receive_progress) == bool)
        assert (caller is None or type(caller) is int)
        assert(caller_authid is None or isinstance(caller_authid, string_types))
        assert(caller_authrole is None or isinstance(caller_authrole, string_types))
        assert(procedure is None or isinstance(procedure, string_types))

        # end-to-end app payload encryption
        assert(enc_algo is None or enc_algo in [PAYLOAD_ENC_CRYPTO_BOX])
        assert(enc_key is None or (isinstance(enc_key, string_types) or isinstance(enc_key, binary_type)))
        assert(enc_serializer is None or enc_serializer in ['json', 'msgpack', 'cbor', 'ubjson'])
        assert((enc_algo is None and enc_key is None and enc_serializer is None) or (enc_algo is not None and payload is not None))

        Message.__init__(self)
        self.request = request
        self.registration = registration
        self.args = args
        self.kwargs = kwargs
        self.payload = payload
        self.timeout = timeout
        self.receive_progress = receive_progress
        self.caller = caller
        self.caller_authid = caller_authid
        self.caller_authrole = caller_authrole
        self.procedure = procedure

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Invocation.MESSAGE_TYPE)

        if len(wmsg) not in (4, 5, 6):
            raise ProtocolError("invalid message length {0} for INVOCATION".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in INVOCATION")
        registration = check_or_raise_id(wmsg[2], "'registration' in INVOCATION")
        details = check_or_raise_extra(wmsg[3], "'details' in INVOCATION")

        args = None
        kwargs = None
        payload = None
        enc_algo = None
        enc_key = None
        enc_serializer = None

        if len(wmsg) == 5 and (isinstance(wmsg[4], string_types) or isinstance(wmsg[4], binary_type)):

            payload = wmsg[4]

            enc_algo = details.get('enc_algo', None)
            if enc_algo and enc_algo not in [PAYLOAD_ENC_CRYPTO_BOX]:
                raise ProtocolError("invalid value {0} for 'enc_algo' detail in EVENT".format(enc_algo))

            enc_key = details.get('enc_key', None)
            if enc_key and (not isinstance(enc_key, string_types) and not isinstance(enc_key, binary_type)):
                raise ProtocolError("invalid type {0} for 'enc_key' detail in EVENT".format(type(enc_key)))

            enc_serializer = details.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' detail in EVENT".format(enc_serializer))

        else:
            if len(wmsg) > 4:
                args = wmsg[4]
                if type(args) != list:
                    raise ProtocolError("invalid type {0} for 'args' in INVOCATION".format(type(args)))

            if len(wmsg) > 5:
                kwargs = wmsg[5]
                if type(kwargs) != dict:
                    raise ProtocolError("invalid type {0} for 'kwargs' in INVOCATION".format(type(kwargs)))

        timeout = None
        receive_progress = None
        caller = None
        caller_authid = None
        caller_authrole = None
        procedure = None

        if 'timeout' in details:

            detail_timeout = details['timeout']
            if type(detail_timeout) is not int:
                raise ProtocolError("invalid type {0} for 'timeout' detail in INVOCATION".format(type(detail_timeout)))

            if detail_timeout < 0:
                raise ProtocolError("invalid value {0} for 'timeout' detail in INVOCATION".format(detail_timeout))

            timeout = detail_timeout

        if 'receive_progress' in details:

            detail_receive_progress = details['receive_progress']
            if type(detail_receive_progress) != bool:
                raise ProtocolError("invalid type {0} for 'receive_progress' detail in INVOCATION".format(type(detail_receive_progress)))

            receive_progress = detail_receive_progress

        if 'caller' in details:

            detail_caller = details['caller']
            if type(detail_caller) is not int:
                raise ProtocolError("invalid type {0} for 'caller' detail in INVOCATION".format(type(detail_caller)))

            caller = detail_caller

        if 'caller_authid' in details:

            detail_caller_authid = details['caller_authid']
            if not isinstance(detail_caller_authid, string_types):
                raise ProtocolError("invalid type {0} for 'caller_authid' detail in INVOCATION".format(type(detail_caller_authid)))

            caller_authid = detail_caller_authid

        if 'caller_authrole' in details:

            detail_caller_authrole = details['caller_authrole']
            if not isinstance(detail_caller_authrole, string_types):
                raise ProtocolError("invalid type {0} for 'caller_authrole' detail in INVOCATION".format(type(detail_caller_authrole)))

            caller_authrole = detail_caller_authrole

        if 'procedure' in details:

            detail_procedure = details['procedure']
            if not isinstance(detail_procedure, string_types):
                raise ProtocolError("invalid type {0} for 'procedure' detail in INVOCATION".format(type(detail_procedure)))

            procedure = detail_procedure

        obj = Invocation(request,
                         registration,
                         args=args,
                         kwargs=kwargs,
                         payload=payload,
                         timeout=timeout,
                         receive_progress=receive_progress,
                         caller=caller,
                         caller_authid=caller_authid,
                         caller_authrole=caller_authrole,
                         procedure=procedure,
                         enc_algo=enc_algo,
                         enc_key=enc_key,
                         enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.timeout is not None:
            options['timeout'] = self.timeout

        if self.receive_progress is not None:
            options['receive_progress'] = self.receive_progress

        if self.caller is not None:
            options['caller'] = self.caller

        if self.caller_authid is not None:
            options['caller_authid'] = self.caller_authid

        if self.caller_authrole is not None:
            options['caller_authrole'] = self.caller_authrole

        if self.procedure is not None:
            options['procedure'] = self.procedure

        if self.payload:
            if self.enc_algo is not None:
                options['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                options['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                options['enc_serializer'] = self.enc_serializer
            return [Invocation.MESSAGE_TYPE, self.request, self.registration, options, self.payload]
        else:
            if self.kwargs:
                return [Invocation.MESSAGE_TYPE, self.request, self.registration, options, self.args, self.kwargs]
            elif self.args:
                return [Invocation.MESSAGE_TYPE, self.request, self.registration, options, self.args]
            else:
                return [Invocation.MESSAGE_TYPE, self.request, self.registration, options]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Invocation(request={0}, registration={1}, args={2}, kwargs={3}, timeout={4}, receive_progress={5}, caller={6}, caller_authid={7}, caller_authrole={8}, procedure={9}, enc_algo={10}, enc_key={11}, enc_serializer={12}, payload={13})".format(self.request, self.registration, self.args, self.kwargs, self.timeout, self.receive_progress, self.caller, self.caller_authid, self.caller_authrole, self.procedure, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


class Interrupt(Message):
    """
    A WAMP ``INTERRUPT`` message.

    Format: ``[INTERRUPT, INVOCATION.Request|id, Options|dict]``
    """

    MESSAGE_TYPE = 69
    """
   The WAMP message code for this type of message.
   """

    ABORT = 'abort'
    KILL = 'kill'

    def __init__(self, request, mode=None):
        """

        :param request: The WAMP request ID of the original ``INVOCATION`` to interrupt.
        :type request: int
        :param mode: Specifies how to interrupt the invocation (``"abort"`` or ``"kill"``).
        :type mode: unicode or None
        """
        assert (type(request) is int)
        assert(mode is None or isinstance(mode, string_types))
        assert(mode is None or mode in [self.ABORT, self.KILL])

        Message.__init__(self)
        self.request = request
        self.mode = mode

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Interrupt.MESSAGE_TYPE)

        if len(wmsg) != 3:
            raise ProtocolError("invalid message length {0} for INTERRUPT".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in INTERRUPT")
        options = check_or_raise_extra(wmsg[2], "'options' in INTERRUPT")

        # options
        #
        mode = None

        if 'mode' in options:

            option_mode = options['mode']
            if not isinstance(option_mode, string_types):
                raise ProtocolError("invalid type {0} for 'mode' option in INTERRUPT".format(type(option_mode)))

            if option_mode not in [Interrupt.ABORT, Interrupt.KILL]:
                raise ProtocolError("invalid value '{0}' for 'mode' option in INTERRUPT".format(option_mode))

            mode = option_mode

        obj = Interrupt(request, mode=mode)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.mode is not None:
            options['mode'] = self.mode

        return [Interrupt.MESSAGE_TYPE, self.request, options]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Interrupt(request={0}, mode={1})".format(self.request, self.mode)


class Yield(Message):
    """
    A WAMP ``YIELD`` message.

    Formats:

    * ``[YIELD, INVOCATION.Request|id, Options|dict]``
    * ``[YIELD, INVOCATION.Request|id, Options|dict, Arguments|list]``
    * ``[YIELD, INVOCATION.Request|id, Options|dict, Arguments|list, ArgumentsKw|dict]``
    * ``[YIELD, INVOCATION.Request|id, Options|dict, Payload|string/binary]``
    """

    MESSAGE_TYPE = 70
    """
    The WAMP message code for this type of message.
    """

    def __init__(self, request, args= None, kwargs=None,
                 payload=None, progress=None,
                 enc_algo=None, enc_key=None, enc_serializer=None):
        """

        :param request: The WAMP request ID of the original call.
        :type request: int
        :param args: Positional values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type args: list or tuple or None
        :param kwargs: Keyword values for application-defined event payload.
           Must be serializable using any serializers in use.
        :type kwargs: dict or None
        :param payload: Alternative, transparent payload. If given, `args` and `kwargs` must be left unset.
        :type payload: unicode or bytes
        :param progress: If ``True``, this result is a progressive invocation result, and subsequent
           results (or a final error) will follow.
        :type progress: bool or None
        :param enc_algo: If using payload encryption, the algorithm used (currently, only "cryptobox" is valid).
        :type enc_algo: unicode
        :param enc_key: If using payload encryption, the message encryption key.
        :type enc_key: unicode or binary
        :param enc_serializer: If using payload encryption, the encrypted payload object serializer.
        :type enc_serializer: unicode
        """
        assert (type(request) is int)
        assert(args is None or type(args) in [list, tuple])
        assert(kwargs is None or type(kwargs) == dict)
        assert (payload is None or (isinstance(payload, string_types) or isinstance(payload, binary_type)))
        assert(payload is None or (payload is not None and args is None and kwargs is None))
        assert(progress is None or type(progress) == bool)

        # end-to-end app payload encryption
        assert(enc_algo is None or enc_algo in [PAYLOAD_ENC_CRYPTO_BOX])
        assert (enc_key is None or (isinstance(enc_key, string_types) or isinstance(enc_key, binary_type)))
        assert(enc_serializer is None or enc_serializer in ['json', 'msgpack', 'cbor', 'ubjson'])
        assert((enc_algo is None and enc_key is None and enc_serializer is None) or (enc_algo is not None and payload is not None))

        Message.__init__(self)
        self.request = request
        self.args = args
        self.kwargs = kwargs
        self.payload = payload
        self.progress = progress

        # end-to-end app payload encryption
        self.enc_algo = enc_algo
        self.enc_key = enc_key
        self.enc_serializer = enc_serializer

    @staticmethod
    def parse(wmsg):
        """
        Verifies and parses an unserialized raw message into an actual WAMP message instance.

        :param wmsg: The unserialized raw message.
        :type wmsg: list

        :returns: An instance of this class.
        """
        # this should already be verified by WampSerializer.unserialize
        #
        assert(len(wmsg) > 0 and wmsg[0] == Yield.MESSAGE_TYPE)

        if len(wmsg) not in (3, 4, 5):
            raise ProtocolError("invalid message length {0} for YIELD".format(len(wmsg)))

        request = check_or_raise_id(wmsg[1], "'request' in YIELD")
        options = check_or_raise_extra(wmsg[2], "'options' in YIELD")

        args = None
        kwargs = None
        payload = None
        enc_algo = None
        enc_key = None
        enc_serializer = None

        if len(wmsg) == 4 and (isinstance(wmsg[3], string_types) or isinstance(wmsg[3], binary_type)):

            payload = wmsg[3]

            enc_algo = options.get('enc_algo', None)
            if enc_algo and enc_algo not in [PAYLOAD_ENC_CRYPTO_BOX]:
                raise ProtocolError("invalid value {0} for 'enc_algo' detail in EVENT".format(enc_algo))

            enc_key = options.get('enc_key', None)
            if enc_key and (not isinstance(enc_key, string_types) and not isinstance(enc_key, binary_type)):
                raise ProtocolError("invalid type {0} for 'enc_key' detail in EVENT".format(type(enc_key)))

            enc_serializer = options.get('enc_serializer', None)
            if enc_serializer and enc_serializer not in ['json', 'msgpack', 'cbor', 'ubjson']:
                raise ProtocolError("invalid value {0} for 'enc_serializer' detail in EVENT".format(enc_serializer))

        else:
            if len(wmsg) > 3:
                args = wmsg[3]
                if type(args) != list:
                    raise ProtocolError("invalid type {0} for 'args' in YIELD".format(type(args)))

            if len(wmsg) > 4:
                kwargs = wmsg[4]
                if type(kwargs) != dict:
                    raise ProtocolError("invalid type {0} for 'kwargs' in YIELD".format(type(kwargs)))

        progress = None

        if 'progress' in options:

            option_progress = options['progress']
            if type(option_progress) != bool:
                raise ProtocolError("invalid type {0} for 'progress' option in YIELD".format(type(option_progress)))

            progress = option_progress

        obj = Yield(request,
                    args=args,
                    kwargs=kwargs,
                    payload=payload,
                    progress=progress,
                    enc_algo=enc_algo,
                    enc_key=enc_key,
                    enc_serializer=enc_serializer)

        return obj

    def marshal(self):
        """
        Marshal this object into a raw message for subsequent serialization to bytes.

        :returns: list -- The serialized raw message.
        """
        options = {}

        if self.progress is not None:
            options['progress'] = self.progress

        if self.payload:
            if self.enc_algo is not None:
                options['enc_algo'] = self.enc_algo
            if self.enc_key is not None:
                options['enc_key'] = self.enc_key
            if self.enc_serializer is not None:
                options['enc_serializer'] = self.enc_serializer
            return [Yield.MESSAGE_TYPE, self.request, options, self.payload]
        else:
            if self.kwargs:
                return [Yield.MESSAGE_TYPE, self.request, options, self.args, self.kwargs]
            elif self.args:
                return [Yield.MESSAGE_TYPE, self.request, options, self.args]
            else:
                return [Yield.MESSAGE_TYPE, self.request, options]

    def __str__(self):
        """
        Returns string representation of this message.
        """
        return "Yield(request={0}, args={1}, kwargs={2}, progress={3}, enc_algo={4}, enc_key={5}, enc_serializer={6}, payload={7})".format(self.request, self.args, self.kwargs, self.progress, self.enc_algo, self.enc_key, self.enc_serializer, b2a(self.payload))


OUT_OF_SESSION_RECV_MESSAGES = frozenset({Welcome, Abort, Challenge})
IN_SESSION_RECV_MESSAGES = frozenset({Event, Published, Subscribed, Unsubscribed, Result, Invocation, Interrupt, Registered, Unregistered, Error})

#

#                 else:
#                     raise ProtocolError("Received {0} message, and session is not yet established".format(msg.__class__))
#
#             else:
#                 # self._session_id != None (aka "session established")
#                 if isinstance(msg, message.Goodbye):
#                     self._handlemsg_goodbye(msg)
#
#                 elif isinstance(msg, message.Event):
#
#                     self._handlemsg_event(msg)
#
#                 elif isinstance(msg, message.Published):
#
#                     self._handlemsg_published(msg)
#
#                 elif isinstance(msg, message.Subscribed):
#
#                     self._handlemsg_subscribed(msg)
#
#                 elif isinstance(msg, message.Unsubscribed):
#
#                     self._handlemsg_unsubscribed(msg)
#
#                 elif isinstance(msg, message.Result):
#
#                     self._handlemsg_result(msg)
#
#                 elif isinstance(msg, message.Invocation):
#
#                     self._handlemsg_invocation(msg)
#
#                 elif isinstance(msg, message.Interrupt):
#
#                     self._handlemsg_interrupt()
#
#                 elif isinstance(msg, message.Registered):
#
#                     self._handlemsg_registered(msg)
#
#                 elif isinstance(msg, message.Unregistered):
#
#                     self._handlemsg_unregistered(msg)
#
#                 elif isinstance(msg, message.Error):
#
#                     self._handlemsg_error(msg)
#
#                 else:
#
#                     raise ProtocolError("Unexpected message {0}".format(msg.__class__))