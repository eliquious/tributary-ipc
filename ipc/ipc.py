from tributary.streams import StreamProducer, StreamElement
from tributary.core import Message, Engine
from tributary.utilities import validateType
from tributary.events import StopMessage, STOP, START

import gipc
import os, glob, re

__all__ = ['BaseEngineFactory', 'SimpleEngineFactory', 'IPCDispatcher', 'IPCSubscriber']

class BaseEngineFactory(object):
    """EngineFactories produce engines for child processes. The 'create' method 
    should return a function which composes an 'Engine' and start it. This 
    function will run as main on the child process. The function should take 
    one argument as a duplex pipe to read and write to the parent process.
    """
    def __init__(self):
        super(BaseEngineFactory, self).__init__()

    def create(self):
        raise NotImplementedError("'EngineFactory.create' not implemented")

class SimpleEngineFactory(BaseEngineFactory):
    """SimpleEngineFactory passes the given function to a child process"""
    def __init__(self, function):
        super(SimpleEngineFactory, self).__init__()
        self.function = function
    
    def create(self):
        """Returns a function which runs an Engine on a child process. 
        The returned function should take one argument as a duplex pipe 
        to read and write to the parent process."""
        return self.function

class IPCSubscriber(StreamProducer):
    """IPCSubscriber"""
    def __init__(self, name, pipe):
        super(IPCSubscriber, self).__init__(name)
        self.pipe = pipe

    def process(self, msg):
        self.scatter(msg)

    # def postProcess(self, msg):
        # self.log("Stopping child nodes...")
        # self.running = False

    def execute(self):
        """Handles the data flow for IPC"""
        self.running = True

        self.log("Starting...")

        # io loop
        while self.running:            
            try:
                message = self.pipe.get()

                # This needs to go here
                # If 'handle' processes the message 
                #   it will go to postProcess and running will be set to false.
                # However, the get() method above will block before the while statement is executed again
                if message.channel == STOP:
                    self.log('Exiting event loop...')
                    self.stop()
                    break

                # handles message just like any other actor
                self.handle(message)

            except gevent.Timeout:
                pass
            except Exception:
                tributary.log_exception(self.name, "Error in '%s': %s" % (self.__class__.__name__, self.name))
                self.tick()

        self.log("Exiting...")

class IPCDispatcher(StreamElement):
    """IPCDispatcher"""
    def __init__(self, name, factory):
        super(IPCDispatcher, self).__init__(name)
        self.factory = factory

        # create pipes
        cend, pipe = gipc.pipe(duplex=True)
        self.pipe = pipe

        # create child
        self.child = gipc.start_process(self.factory.create(), args=(cend,))
        self.log('Started child process')

        # register on start
        self.on(START, self.onStart)

        # register on stop
        self.on(STOP, self.onStop)

        self.registered = False

    def onStart(self, msg):
        
        # any child initialization logic
        if not self.registered:
            self.registered = True
            self.onConnection()


    def onConnection(self):
        """"""
        pass

    def onClose(self):
        """"""
        pass

    def onStop(self, msg=None):
        """Called after the child is joined"""
        self.log('Stopping child process')
        self.pipe.put(StopMessage)

        self.log('Waiting for child process to stop')
        self.child.join()

        # allow user actions
        self.onClose()
