try:
    from enum import Enum
except ImportError:
    from enum34 import Enum


class MessageType(Enum):
    TEXT = 1
    BINARY = 2



class ProtocolState(Enum):

    CLOSED = 0
    CONNECTING = 1
    CLOSING = 2
    OPEN = 3
    PROXY_CONNECTING = 4

class StreamingSendState(Enum):
    # Streaming Send State
    GROUND = 0
    MESSAGE_BEGIN = 1
    INSIDE_MESSAGE = 2
    INSIDE_MESSAGE_FRAME = 3


class CloseStatusCode(Enum):
    # WebSocket protocol close codes
    # See:https://www.iana.org/assignments/websocket/websocket.xml#close-code-number-rules
    #
    NORMAL = 1000
    """Normal close of connection."""

    GOING_AWAY = 1001
    """Going away."""

    PROTOCOL_ERROR = 1002
    """Protocol error."""

    UNSUPPORTED_DATA = 1003
    """Unsupported data."""

    RESERVED1 = 1004
    """RESERVED"""

    NULL = 1005  # MUST NOT be set in close frame!
    """No status received. (MUST NOT be used as status code when sending a close)."""

    ABNORMAL_CLOSE = 1006  # MUST NOT be set in close frame!
    """Abnormal close of connection. (MUST NOT be used as status code when sending a close)."""

    INVALID_PAYLOAD = 1007
    """Invalid frame payload data."""

    POLICY_VIOLATION = 1008
    """Policy violation."""

    MESSAGE_TOO_BIG = 1009
    """Message too big."""

    MANDATORY_EXTENSION = 1010
    """Mandatory extension."""

    INTERNAL_ERROR = 1011
    """The peer encountered an unexpected condition or internal error."""

    SERVICE_RESTART = 1012
    """Service restart."""

    TRY_AGAIN_LATER = 1013
    """Try again later."""

    UNASSIGNED1 = 1014
    """Unassiged."""

    TLS_HANDSHAKE_FAILED = 1015  # MUST NOT be set in close frame!
    """TLS handshake failed, i.e. server certificate could not be verified. (MUST NOT be used as status code when sending a close)."""

    # CLOSE_STATUS_CODES_ALLOWED = [NORMAL,
    #                               GOING_AWAY,
    #                               PROTOCOL_ERROR,
    #                               UNSUPPORTED_DATA,
    #                               INVALID_PAYLOAD,
    #                               POLICY_VIOLATION,
    #                               MESSAGE_TOO_BIG,
    #                               MANDATORY_EXTENSION,
    #                               INTERNAL_ERROR,
    #                               SERVICE_RESTART,
    #                               TRY_AGAIN_LATER]
