"""
Make a class, define some methods, get a free NSQ-backed pipeline.
"""

import types
import inspect
import cPickle as pickle
import time
import nsq
from nsq_pipeline import NSQPipeline

writer = nsq.Writer(['127.0.0.1:4150'])

class NSQPipelineException(Exception):
    pass


class NSQFunctionTemplate(object):
    """We'll instantiate a new object each time new data appears."""
    def __init__(self, datum_identifier, target_function_identifier,
                 signature_tuple):
        self.signature_tuple = signature_tuple
        self.saturated_arguments = {}
        self.datum_identifier = datum_identifier
        self.target_function_identifier = target_function_identifier
        self.mark_for_deletion = False  #: we'll change to true when saturated
        self.time_created = time.time()

    def __repr__(self):
        representation = [self.signature_tuple, self.saturated_arguments,
                          self.datum_identifier,
                          self.target_function_identifier]
        representation = '---\n' + '\n'.join(str(i) for i in representation)
        return representation

    def set_argument_list(self, *args):
        self.argument_list = tuple(args)

    def saturated(self):
        """Returns ``True`` if we have all the necessary arguments"""
        return len([i for i in self.signature_tuple if i in
                    self.saturated_arguments.keys()]) == len(
                        self.signature_tuple)

    def matches_template(self, function_identifier, datum_identifier):
        """Returns ``True`` if the data matches the template."""
        return (function_identifier in self.signature_tuple and
                datum_identifier == self.datum_identifier)

    def add_datum_if_matches(self, datum):
        global writer
        """Assume `datum` is a dictionary containing keys:
           1. `function_identifier`  #: source of the data
           2. `datum_identifier`     #: unique id for keeping in sync
           3. `payload`              #: the data
        """
        print 'Got a datum:'
        print datum
        function_identifier = datum['function_identifier']
        datum_identifier = datum['datum_identifier']
        payload = datum['payload']
        found_matching_template = False
        if self.matches_template(function_identifier, datum_identifier):
            self.saturated_arguments[function_identifier] = datum['payload']
            print 'added datum in matches_template loop:', datum
            found_matching_template = True

        # If saturated, then we need to send a message
        # send dictionary to topic with target_function_identifier as topic
        if self.saturated():
            print 'saturated', self
            datum_to_send = {
                'signature_tuple': self.signature_tuple,
                'datum_identifier': self.datum_identifier,
                'target_function_identifier': self.target_function_identifier,
                'time_saturated': time.time(),
                'saturated_arguments': self.saturated_arguments}
            writer.pub(self.target_function_identifier, pickle.dumps(datum_to_send))

            # Send to the topic identified by the destination function's name
        else:
            print 'not saturated', self


class NSQAutoPipeline(object):
    """Pipeline"""
    def __init__(self):

        def _wrap(function, signature):
            def _wrapped_function(**kwargs):
                # change this so that instead of kwargs, it gets a Message
                # and the kwargs are extracted from the message.body payload
                # (after it's been unpickled)
                out = function(**kwargs)
                return out
            return _wrapped_function

        @NSQPipeline(listen_topic='_pool') # Assume ALL messages are pickled
        def _listener(message):
            print 'Just heard: %s' % message
            function_identifier = message['function_identifier']
            datum_identifier = message['datum_identifier']
            if function_identifier.startswith('_source'):
                #: instantiate a new series of NSQFunctionTemplates
                #:add the new template to self.function_templates
                #: initiate a check on that data to start saturating functions
                print 'I am making a template...'
                self._make_function_templates(datum_identifier)
            # initiate a check to see what template(s) the datum fits
            print 'checking tamplates...'
            found_matching_template = False
            for template in self.function_templates:
                print template
                if template.add_datum_if_matches(message):
                    found_matching_template = True
            if not found_matching_template:
                print 'Warning: datum with no matching template.'

        # Go through the user-defined functions in the subclass
        self.function_signatures = {}
        self.function_templates = []
        for attribute_key in dir(self):
            if attribute_key[0] == '_':
                continue
            attribute = getattr(self, attribute_key)
            if isinstance(attribute, types.MethodType):
                function_name = attribute_key
                argument_list = [
                    argument for argument in inspect.getargspec(attribute).args
                    if argument != 'self']
                self.function_signatures[function_name] = argument_list

        print 'self.function_signatures:', self.function_signatures
        for function_name, function_signature in self.function_signatures.iteritems():
            some_function = getattr(self, function_name)
            f = _wrap(some_function, function_signature)
        # now we can call f(**{'_source_1': 'foo'})
        import pdb; pdb.set_trace()

    def _make_function_templates(self, datum_identifier):
        """Make a new set of templates for when new _source data appears."""
        for function_name, signature in self.function_signatures.iteritems():
            function_template = NSQFunctionTemplate(
                datum_identifier, function_name, signature)
            self.function_templates.append(function_template)

def demo():
    function_template = NSQFunctionTemplate(
        'datum_identifier_abc', 'target_function_f', ('a',))
    function_template.set_argument_list('f', 'g')
    print function_template
    datum_1 = {'function_identifier': 'f', 'payload': 1,
               'datum_identifier': 'abc_datum_identifier'}
    datum_2 = {'function_identifier': 'g', 'payload': 2,
               'datum_identifier': 'abc_datum_identifier'}
    print 'adding datum_1'
    function_template.add_datum_if_matches(datum_1)
    print 'adding datum_2'
    function_template.add_datum_if_matches(datum_2)


    class MyPipeline(NSQAutoPipeline):
        #def some_function_2(self, some_function_1):
        #    print some_data
        #    return 'foo'
        def some_function_1(self, _source_1):
            print 'some_function_1 got %s:' % (_source_1)
            return 'bar'

    pipeline = MyPipeline()
    nsq.run()

    import pdb; pdb.set_trace()
    exit()

if __name__ == '__main__':
    demo()
