import boto
import boto.exception
import boto.regioninfo
import errno
import os
from multiprocessing import Process, Event
import signal
import testtools as unittest

import basicdb

def run_server(port, server_ready, done):
    from coverage.collector import Collector
    from coverage.control import coverage
    if Collector._collectors:
        cov = coverage(data_suffix=True)
        cov.start()
        def stop_cov(*args):
            cov.stop()
            cov.save()
            raise SystemExit('killed')
    else:
        def stop_cov(*args):
            raise SystemExit('killed')

    fp = open('/tmp/null', 'a+')
    os.dup2(fp.fileno(), 0)
    os.dup2(fp.fileno(), 1)
    os.dup2(fp.fileno(), 2)

    os.environ['REMOTE_USER'] = 'fake'
    from wsgiref.simple_server import make_server
    s = make_server('localhost', port, basicdb.app)

    signal.signal(signal.SIGINT, stop_cov)
    server_ready.set()
    s.serve_forever()

class FunctionalTests(object):
    def setUp(self):
        super(FunctionalTests, self).setUp()
        existing_server = os.environ.get('BASICDB_PORT', False)
        if not existing_server:
            def kill_server():
                while True:
                    try:
                        os.kill(self.server.pid, signal.SIGINT)
                        self.server.join(1)
                    except OSError, e:
                        if e.errno == errno.ESRCH:
                            break
                        raise

            self.server_ready = Event()
            self.done = Event()
            self.server = Process(target=run_server, args=(8000, self.server_ready, self.done))
            self.server.start()
            self.port = 8000

            self.server_ready.wait()
        else:
            def kill_server():
                pass

            self.port = int(existing_server)

        self.kill_server = kill_server

    def tearDown(self):
        super(FunctionalTests, self).tearDown()
        self.kill_server()

class _BotoTests(FunctionalTests):
    def setUp(self):
        super(_BotoTests, self).setUp()

        local_region = boto.regioninfo.RegionInfo(name='local',
                                                  endpoint='localhost')
        self.conn = boto.connect_sdb('', '',
                                     region=local_region,
                                     is_secure=False, port=self.port)

    def _test_create_list_delete_domains(self):
        self.conn.create_domain('test-domain')
        self.conn.create_domain('test-domain-2')
        domains = self.conn.get_all_domains()
        self.assertEquals(len(domains), 2)
        self.assertIn('test-domain', set([d.name for d in domains]))
        self.assertIn('test-domain-2', set([d.name for d in domains]))

        self.conn.delete_domain('test-domain')

        domains = self.conn.get_all_domains()
        self.assertEquals(len(domains), 1)
        self.assertIn('test-domain-2', set([d.name for d in domains]))

        self.conn.delete_domain('test-domain-2')

        domains = self.conn.get_all_domains()
        self.assertEquals(len(domains), 0)

    def test_get_domain(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')
        self.assertIsNot(dom, None)
        self.conn.delete_domain('test-domain')

    def test_get_domain_metadata(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')
        domain_meta = self.conn.domain_metadata(dom)
        self.conn.delete_domain('test-domain')

    def test_add_item(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')

        item_name = 'ABC_123'
        item_attrs = {'Artist': 'The Jackson 5', 'Genera':'Pop'}
        retval = dom.put_attributes(item_name, item_attrs)
        self.assertEquals(retval, True)
        domain_meta = self.conn.domain_metadata(dom)
        self.assertEquals(domain_meta.item_count, 1)

        self.conn.delete_domain('test-domain')

    def test_delete_attrs(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')

        item_name = 'ABC_123'
        item_attrs = {'Artist': 'The Jackson 5', 'Genera':'Pop'}
        retval = dom.put_attributes(item_name, item_attrs)
        self.assertEquals(retval, True)
        dom.delete_attributes(item_name, ['Artist'])

        self.assertEquals(dom.get_attributes(item_name),
                          {'Genera': 'Pop'})

        self.conn.delete_domain('test-domain')

    def test_add_item_conditionally(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')

        item_name = 'test-item'
        item_attrs = {'attr1': 'attr1val1', 'attr2': 'attr2val1'}
        retval = dom.put_attributes(item_name, item_attrs)
        self.assertEquals(retval, True)

        self.assertEquals(dom.get_attributes('test-item'),
                          {'attr1': 'attr1val1', 'attr2': 'attr2val1'})

        item_attrs = {'attr1': 'attr1val2'}

        self.assertRaises(boto.exception.BotoServerError,
                          dom.put_attributes, item_name, item_attrs,
                          replace=False, expected_value=('attr1', 'attr1val2'))

        self.assertEquals(dom.get_attributes('test-item'),
                          {'attr1': 'attr1val1', 'attr2': 'attr2val1'},
                          "Updated value even thought expectations were not met")

        self.assertRaises(boto.exception.BotoServerError,
                          dom.put_attributes, item_name, item_attrs,
                          replace=False, expected_value=('attr1', False))

        self.assertEquals(dom.get_attributes('test-item'),
                          {'attr1': 'attr1val1', 'attr2': 'attr2val1'},
                          "Updated value even thought expectations were not met")

        retval = dom.put_attributes(item_name, item_attrs,
                                    replace=False, expected_value=('attr1', True))

        self.assertEquals(dom.get_attributes('test-item'),
                          {'attr1': ['attr1val1', 'attr1val2'], 'attr2': 'attr2val1'},
                          "Did not update value even thought expectations were met")

        self.conn.delete_domain('test-domain')

    def test_batch_add_items(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain', validate=False)

        items = {'item1':{'attr1':'val1'},'item2':{'attr2':'val2'}}
        dom.batch_put_attributes(items)

        self.assertEquals(dom.get_attributes('item1'), {'attr1': 'val1'})
        self.assertEquals(dom.get_attributes('item2'), {'attr2': 'val2'})
        self.conn.delete_domain('test-domain')


    def test_batch_delete_items(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain', validate=False)

        items = {'item1':{'attr1':'val1'},'item2':{'attr2':'val2'}}
        dom.batch_put_attributes(items)

        items = {'item1':{'attr1':'val2'},'item2':{'attr3':'val3'}}
        dom.batch_put_attributes(items, replace=False)

        item1_attrs = dom.get_attributes('item1')
        self.assertEquals(len(item1_attrs), 1)
        self.assertEquals(set(item1_attrs['attr1']), set(["val1", "val2"]))

        item2_attrs = dom.get_attributes('item2')
        self.assertEquals(item2_attrs, {"attr2": "val2", "attr3": "val3"})

        dom.batch_delete_attributes({"item1": {"attr1": "val2"}, "item2": None})

        item1_attrs = dom.get_attributes('item1')
        self.assertEquals(item1_attrs, {"attr1": "val1"})

    def _load_sample_query_data_set(self):
        dom = self.conn.create_domain('mydomain')
        dom.put_attributes("0385333498",
                           {"Title": "The Sirens of Titan",
                            "Author": "Kurt Vonnegut",
                            "Year": "1959",
                            "Pages": "00336",
                            "Keyword": ["Book", "Paperback"],
                            "Rating": ["*****", "5 stars", "Excellent"]})

        dom.put_attributes("0802131786",
                           {"Title": "Tropic of Cancer",
                            "Author": "Henry Miller",
                            "Year": "1934",
                            "Pages": "00318",
                            "Keyword": "Book",
                            "Rating": "****"})

        dom.put_attributes("1579124585",
                           {"Title": "The Right Stuff",
                            "Author": "Tom Wolfe",
                            "Year": "1979",
                            "Pages": "00304",
                            "Keyword": ["Book", "Hardcover", "American"],
                            "Rating": ["****", "4 stars"]})

        dom.put_attributes("B000T9886K",
                           {"Title": "In Between",
                            "Author": "Paul Van Dyk",
                            "Year": "2007",
                            "Keyword": ["CD", "Trance"],
                            "Rating": "4 stars"})

        dom.put_attributes("B00005JPLW",
                           {"Title": "300",
                            "Author": "Zack Snyder",
                            "Year": "2007",
                            "Keyword": ["DVD", "Action", "Frank Miller"],
                            "Rating": ["***", "3 stars", "Not bad"]})

        dom.put_attributes("B000SF3NGK",
                           {"Title": "Heaven's Gonna Burn Your Eyes",
                            "Author": "Thievery Corporation",
                            "Year": "2002",
                            "Rating": "*****"})
        return dom

    def test_select(self):
        dom = self._load_sample_query_data_set()
        res = dom.select("select * from mydomain where Title = 'The Right Stuff'")
        for row in res:
            print row


class FakeBackedBotoTests(_BotoTests, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super(FakeBackedBotoTests, cls).setUpClass()
        basicdb.load_backend('fake')


class _FilesystemBackedBotoTests(_BotoTests):
    @classmethod
    def setUpClass(cls):
        super(_FilesystemBackedBotoTests, cls).setUpClass()
        basicdb.load_backend('filesystem')


class _RiakBackedBotoTests(_BotoTests):
    @classmethod
    def setUpClass(cls):
        super(_RiakBackedBotoTests, cls).setUpClass()
        basicdb.load_backend('riak')


if __name__ == "__main__":
    unittest.main()
