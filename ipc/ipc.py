from tributary.streams import StreamProducer, StreamElement
from tributary.core import Message, Engine
from tributary.utilities import validateType
from tributary.events import StopMessage, STOP

import gipc
import os, glob, re

__all__ = ['BaseEngineFactory', 'SimpleEngineFactory', 'IPCProducer']

class BaseEngineFactory(object):
    """EngineFactories produce engines for child processes. The 'create' method 
    should return a function which composes an 'Engine' and start it. This 
    function will run as main on the child process. The function should take 
    one argument as a duplex pipe to read and write to the parent process.
    """
    def __init__(self):
        super(EngineFactory, self).__init__()

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

class IPCChild(StreamProducer):
    """IPCChild"""
    def __init__(self, name, pipe):
        super(IPCChild, self).__init__(name)
        self.pipe = pipe

    def process(self, msg):
        """Can be overridden."""
        pass

    def execute(self):
        """Handles the data flow for IPC"""
        self.running = True

        self.log("Starting...")

        # io loop
        while self.running:            
            try:
                message = self.pipe.get()

                # handles message just like any other actor
                self.handle(message)

            except Exception:
                tributary.log_exception(self.name, "Error in '%s': %s" % (self.__class__.__name__, self.name))
                self.tick()

        self.log("Exiting...")

class IPCParent(StreamElement):
    """docstring for ClassName"""
    def __init__(self, name, factory):
        super(ClassName, self).__init__(name)

        # create pipes
        cend, pipe = gipc.pipe(duplex=True)
        self.pipe = pipe

        # create child
        self.child = gipc.start_process(factory.create(), args=(cend,))

        # any child initialization logic
        self.onConnection()

        # register on close
        self.on(events.STOP, self.onClose)

    def onConnection(self):
        """"""
        pass

    def onClose(self):
        """Called after the child is joined"""
        self.log('Stopping child process')
        self.pipe.put(StopMessage)

        self.log('Waiting for child process to stop')
        self.child.join()
        self.log('Child process stopped')
