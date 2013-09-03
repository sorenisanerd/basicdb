import boto
import boto.exception
import boto.regioninfo
import os
from multiprocessing import Process, Event
import signal
import unittest2

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
    else:
        def stop_cov(*args):
            pass

    fp = open('/tmp/null', 'a+')
    os.dup2(fp.fileno(), 0)
    os.dup2(fp.fileno(), 1)
    os.dup2(fp.fileno(), 2)

    from wsgiref.simple_server import make_server
    s = make_server('localhost', port, basicdb.app)

    signal.signal(signal.SIGINT, stop_cov)
    server_ready.set()
    s.serve_forever()

class FunctionalTests(unittest2.TestCase):
    def setUp(self):
        existing_server = os.environ.get('BASICDB_PORT', False)
        if not existing_server:
            def kill_server():
                os.kill(self.server.pid, signal.SIGINT)
                self.server.join()
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
        self.kill_server()

class BotoTests(FunctionalTests):
    def setUp(self):
        super(BotoTests, self).setUp()

        local_region = boto.regioninfo.RegionInfo(name='local',
                                                  endpoint='localhost')
        self.conn = boto.connect_sdb('', '',
                                     region=local_region,
                                     is_secure=False, port=self.port)

    def test_create_list_delete_domains(self):
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

    def _test_batch_add_items(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')

        items = {'item1':{'attr1':'val1'},'item2':{'attr2':'val2'}}
        dom.batch_put_attributes(items)

        self.conn.delete_domain('test-domain')


if __name__ == "__main__":
    unittest2.main()
