"""
Decorator for streamlining functions that accept argument passed via NSQ
messages.

Run this script from the command line to execute a little demo.
"""

import cPickle as pickle
import nsq

class NSQPipeline(object):
    """This is a class that functions as a decorator."""
    def __init__(self, lookupd_http_addresses=None,
                 channel='default_channel', lookupd_poll_interval=15,
                 listen_topic=None, send_topic=None,
                 max_in_flight=128, source=False,
                 publication_addresses=None):
        self.max_in_flight=max_in_flight
        if lookupd_http_addresses is None:
            lookupd_http_addresses = ['127.0.0.1:4161']
        if publication_addresses is None:
            publication_addresses = ['127.0.0.1:4150']
        self.lookupd_http_addresses=lookupd_http_addresses
        self.channel = channel
        self.lookupd_poll_interval=lookupd_poll_interval
        self.listen_topic = listen_topic
        self.send_topic = send_topic
        self.writer = nsq.Writer(publication_addresses)
        if source:
            self.input_transform = lambda x: x
        else:
            self.input_transform = pickle.loads
        if send_topic is None:
            self.output_transform = lambda x: x
        else:
            self.output_transform = pickle.dumps

    def __call__(self, obj):
        """This function is executed when a function is decorated, but after
           the class is instantiated and the `__init__` function is called.

           Its purpose is to wrap the function and instantiate an
           `nsq.Reader` object.
        """
        def wrapper(function):
            def wrapped_function(message):
                message.enable_async()
                output = function(self.input_transform(message.body))
                if self.send_topic is not None:
                    self.writer.pub(self.send_topic,
                                    self.output_transform(output))
                message.finish()
            return wrapped_function

        nsq_reader = nsq.Reader(
            message_handler=wrapper(obj),
            lookupd_http_addresses=self.lookupd_http_addresses,
            topic=self.listen_topic,
            channel=self.channel,
            lookupd_poll_interval=self.lookupd_poll_interval,
            max_in_flight=self.max_in_flight)

def demo():
    @NSQPipeline(listen_topic='input', send_topic='topic1', source=True)
    def start_chain(message):
       print 'in the start...'
       return 'foo'

    @NSQPipeline(listen_topic='topic1', send_topic='topic2')
    def foobar1(message):
        print 'in function foobar1 with message %s...' % (message)
        return 'bar'

    @NSQPipeline(listen_topic='topic2')
    def foobar2(message):
        print 'in function foobar2 with message %s...' % (message)
        return 'baz'

    nsq.run()

if __name__ == '__main__':
    demo()
