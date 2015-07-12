# nsq_pipeline

## What is it?
`nsq_pipeline` is a simple Python decorator for lazy people (like me) who are
using the excellent `NSQ` message-queuing system. It is built on top of the
official Python client library for NSQ, `pynsq`.

It provides a function decorator in which you specify the topic from which
the function will read its arguments, and (optionally) another topic to
which it will write its output. This makes it easy to chain together a series
of functions that implement a multi-step process for transforming data. It
also has the advantage of pickling and unpickling the data silently, so you
don't have to worry about serializing and deserializing objects.

## Why is it?
This exists because although the `pynsq` client is very good, you can still
end up writing a lot of boilerplate code. This decorator class drastically
reduces the amount of boilerplate you need to write.

## How do I use it?
Functions that you decorate should have a single argument, which is the NSQ
message body. It's easiest to explain via an example:

    from nsq_pipeline import NSQPipeline

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

If we send a message to NSQ like so:

    curl -d 'hello world 1' 'http://127.0.0.1:4151/put?topic=input'

the message body (`'hello world 1'`) will arrive at the `start_chain`
function because the `listen_topic` has been set to `input`, and the
`topic` argument in the curl command is `input`.

You'll notice that the decorator for `start_chain` contains the optional
keyword argument `source=True`. This tells the decorator that the message
will not have been serialized, and so it should not be deserialized before
being processed.

The `start_chain` returns the output `"bar"`, serializes it with `cPickle`,
and sends it as a message with the topic `topic1`. It is then picked up by
`foobar1` because it is listening to the topic `topic1`. The function returns
the string `"bar"`, which is serialized and sent as a message with the topic
`topic2`'. Finally, it is picked up by `foobar2`, which returns the string
`"baz"`.

The decorator for `foobar2` has no `send_topic`, so the result is not sent
back to NSQ.

Note that it is necessary to explicitly call `nsq.run()` after your functions
have been defined.

There are a number of optional arguments that can be given in the decorator,
and these correspond directly to the parameters in the `pynsq` classes.
