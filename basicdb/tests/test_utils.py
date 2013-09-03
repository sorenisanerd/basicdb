import unittest2

import basicdb
from basicdb import utils

class UtilsTests(unittest2.TestCase):
    def _create_request(self, params):
        class Request(object):
            def __init__(self, params):
                self._params = params

        return Request(params)

    def test_extract_numbered_args(self):
        request = self._create_request({'Attribute.1.Name': 'attr1',
                                        'Attribute.1.Value': 'attr1val1',
                                        'Attribute.3.Name': 'attr3',
                                        'Attribute.3.Value': 'attr3val1',
                                        'Attribute.6.Name': 'attr6',
                                        'Attribute.7.Value': 'attr7val1',
                                        'Attribute.7.Replace': 'attr7replace',
                                        'Attribute.8.Foobar': 'blah'})

        args = utils.extract_numbered_args(utils.PUT_ATTRIBUTE_QUERY_REGEX,
                                           request)

        self.assertEquals(args, {'1': {'Name': 'attr1',
                                       'Value': 'attr1val1'},
                                 '3': {'Name': 'attr3',
                                       'Value': 'attr3val1'},
                                 '6': {'Name': 'attr6'},
                                 '7': {'Value': 'attr7val1',
                                       'Replace': 'attr7replace'}})
      
    def test_extract_additions_and_replacements_from_query_params(self):
        request = self._create_request({'Attribute.1.Name': 'attr1',
                                        'Attribute.1.Value': 'attr1val1',
                                        'Attribute.3.Name': 'attr3',
                                        'Attribute.3.Value': 'attr3val1',
                                        'Attribute.6.Name': 'attr6',
                                        'Attribute.7.Name': 'attr7',
                                        'Attribute.7.Value': 'attr7val1',
                                        'Attribute.7.Replace': 'true',
                                        'Attribute.8.Foobar': 'blah'})

        additions, replacements = utils.extract_additions_and_replacements_from_query_params(request)

        self.assertEquals(additions, {'attr1': set(['attr1val1']),
                                       'attr3': set(['attr3val1'])})
        self.assertEquals(replacements, {'attr7': set(['attr7val1'])})

    def test_extract_deletions_from_query_params(self):
        request = self._create_request({'Attribute.1.Name': 'attr1',
                                        'Attribute.1.Value': 'attr1val1',
                                        'Attribute.3.Name': 'attr3',
                                        'Attribute.3.Value': 'attr3val1',
                                        'Attribute.6.Name': 'attr6',
                                        'Attribute.12.Value': 'attr12val1',
                                        'Attribute.7.Name': 'attr7',
                                        'Attribute.7.Value': 'attr7val1',
                                        'Attribute.7.Replace': 'true',
                                        'Attribute.8.Foobar': 'blah'})

        deletions = utils.extract_deletions_from_query_params(request)

        self.assertEquals(deletions, {'attr1': set(['attr1val1']),
                                      'attr3': set(['attr3val1']),
                                      'attr6': set([basicdb.AllAttributes]),
                                      'attr7': set(['attr7val1'])})

    def test_extract_expectations_from_query_params(self):
        request = self._create_request({'Expected.1.Name': 'attr1',
                                        'Expected.1.Value': 'attr1val1',
                                        'Expected.3.Name': 'attr3',
                                        'Expected.3.Exists': 'true',
                                        'Expected.6.Name': 'attr6',
                                        'Expected.6.Exists': 'false'})

        expectations = utils.extract_expectations_from_query_params(request)

        self.assertEquals(expectations, set([('attr1', 'attr1val1'),
                                             ('attr3', True),
                                             ('attr6', False)]))
