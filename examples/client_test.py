import logging

from beltway.client import WampWebSocketClient
from beltway.autolog import log
from beltway.wamp.exception import ApplicationError

class MyException(Exception):
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(name)s] %(threadName)s %(message)s')

    client = WampWebSocketClient('ws://127.0.0.1:8080/ws', timeout=5)
    client.connect()
    try:
        client.joined_event.wait()

        client.subscribe(lambda event: print("EVENT: {}".format(event)), 'hello')
        client.define(MyException, 'myexc')

        print(client.call('service.helloWorld'))
        print()
        print(client.call('service.sleepy', 2))
        print()
        print(client.call('service.twisty'))
    except MyException as x:
        print("THANK YOU, YES, I GOT IT.")
    except Exception as x:
        log.exception("You suck: {}".format(x.__class__))
    finally:
        client.close()