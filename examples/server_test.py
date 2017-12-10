import logging
import time

from beltway.client import WampWebSocketClient
from beltway.autolog import log
from beltway.wamp.exception import ApplicationError


class MyException(Exception):
    pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(name)s] %(threadName)s %(message)s')

    client = WampWebSocketClient('ws://127.0.0.1:8080/ws', timeout=5)
    client.connect()

    def hello_world():
        log.info("In hello_worlld()")
        client.publish('hello', "You called?")
        return "Hello World"

    def sleepy(seconds):
        log.info("In sleepy()")
        time.sleep(seconds)
        raise MyException("Blah")
        return "Slept for {} seconds".format(seconds)

    def twisty():
        log.info("In twisty()")
        return client.call('service.twisty2')

    def twisty2():
        log.info("In twisty2()")
        return "Woah. Twisted."

    client.joined_event.wait(timeout=3.0)

    client.define(MyException, 'myexc')

    client.register(hello_world, 'service.helloWorld')
    client.register(sleepy, 'service.sleepy')
    client.register(twisty, 'service.twisty')
    client.register(twisty2, 'service.twisty2')

    try:
        log.debug("Running forever")
        client.run_forever()
    except KeyboardInterrupt:
        log.debug("CTRL-C")
        client.close()
    except:
        log.exception("Unhandled error.")
        client.close()