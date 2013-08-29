import boto
import boto.regioninfo
import httplib2
import os
from multiprocessing import Process, Event
import signal
import unittest2
import urllib
import xml.dom.minidom

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
#    os.dup2(fp.fileno(), 1)
    os.dup2(fp.fileno(), 2)

    from wsgiref.simple_server import make_server
    s = make_server('localhost', port, basicdb.app)

    signal.signal(signal.SIGINT, stop_cov)
    server_ready.set()
    s.serve_forever()

class BotoTests(unittest2.TestCase):
    def setUp(self):
        self.server_ready = Event()
        self.done = Event()
        self.server = Process(target=run_server, args=(8000, self.server_ready, self.done))
        self.server.start()
        self.port = 8000

        self.server_ready.wait()
        local_region = boto.regioninfo.RegionInfo(name='local',
                                                  endpoint='localhost')
        self.conn = boto.connect_sdb('', '',
                                     region=local_region,
                                     is_secure=False, port=self.port)

    def tearDown(self):
        os.kill(self.server.pid, signal.SIGINT)
        self.server.join()

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

    def _test_batch_add_items(self):
        self.conn.create_domain('test-domain')
        dom = self.conn.get_domain('test-domain')

        items = {'item1':{'attr1':'val1'},'item2':{'attr2':'val2'}}
        dom.batch_put_attributes(items)

        self.conn.delete_domain('test-domain')


class BasicTests(unittest2.TestCase):
    def setUp(self):
        self.http = httplib2.Http()
        self.server_ready = Event()
        self.done = Event()
        self.server = Process(target=run_server, args=(8000, self.server_ready, self.done))
        self.server.start()
        self.port = 8000
        self.server_ready.wait()

    def tearDown(self):
        os.kill(self.server.pid, signal.SIGINT)
        self.server.join()

    def list_domains(self):
        resp, content = self.http.request("http://localhost:8000/?Action=ListDomains&AWSAccessKeyId=valid_access_key_id&MaxNumberOfDomains=2&NextToken=valid_next_token&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%3A02%3A19-07%3A00&Version=2009-04-15&Signature=valid_signature", "GET")
        self.assertEquals(resp.status, 200)
        dom = xml.dom.minidom.parseString(content)
        domains_result = dom.getElementsByTagName('ListDomainsResult')[0]
        return [x.childNodes[0].data for x in domains_result.getElementsByTagName('DomainName')]

    def create_domain(self, domain_name):
        resp, content = self.http.request("http://localhost:8000/?Action=CreateDomain&AWSAccessKeyId=valid_access_key_id&DomainName=%s&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%%3A01%%3A28-07%%3A00&Version=2009-04-15&Signature=valid_signature" % (domain_name,), "GET")
        self.assertEquals(resp.status, 200)
        xml.dom.minidom.parseString(content)

    def delete_domain(self, domain_name):
        resp, content = self.http.request("http://localhost:8000/?Action=DeleteDomain&AWSAccessKeyId=valid_access_key_id&DomainName=%s&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%%3A02%%3A20-07%%3A00&Version=2009-04-15&Signature=valid_signature" % (domain_name,), "GET")
        self.assertEquals(resp.status, 200)
        xml.dom.minidom.parseString(content)

    def test_list_domains_empty(self):
        self.assertEquals(self.list_domains(), [])

    def test_create_delete_domain(self):
        self.create_domain('MyDomain')
        self.assertEquals(self.list_domains(), ['MyDomain'])
        self.create_domain('MyDomain2')
        self.assertEquals(set(self.list_domains()), set(['MyDomain', 'MyDomain2']))
        self.delete_domain('MyDomain')
        self.delete_domain('MyDomain2')
        self.assertEquals(self.list_domains(), [])

    def test_put_attributes(self):
        self.create_domain('MyDomain')
        self.put_attributes('MyDomain', 'MyItem', {'colour': 'red'}, ['colour'])

    def test_put_get_attributes(self):
        self.create_domain('MyDomain')
        self.put_attributes('MyDomain', 'MyItem', {'colour': 'red'}, ['colour'])
        attrs = self.get_attributes('MyDomain', 'MyItem')
        self.assertEquals(attrs, {'colour': set(['red'])})

        self.put_attributes('MyDomain', 'MyItem', {'colour': 'blue'}, ['colour'])
        attrs = self.get_attributes('MyDomain', 'MyItem')
        self.assertEquals(attrs, {'colour': set(['blue'])})

        self.put_attributes('MyDomain', 'MyItem', {'colour': 'red'})
        attrs = self.get_attributes('MyDomain', 'MyItem')
        self.assertEquals(attrs, {'colour': set(['blue', 'red'])})
        self.delete_domain('MyDomain')

    def test_delete_attributes(self):
        self.create_domain('MyDomain')
        try:
            self.put_attributes('MyDomain', 'MyItem', {'colour': 'red'})
            self.put_attributes('MyDomain', 'MyItem', {'shape': 'square'} )

            attrs = self.get_attributes('MyDomain', 'MyItem')
            self.assertEquals(attrs, {'colour': set(['red']), 'shape': set(['square'])})

            self.delete_attributes('MyDomain', 'MyItem', {'colour': None})
            attrs = self.get_attributes('MyDomain', 'MyItem')
            self.assertEquals(attrs, {'shape': set(['square'])})
        finally:
            self.delete_domain('MyDomain')

    def test_select(self):
        self.create_domain('MyDomain')
        try:
            self.put_attributes('MyDomain', 'MyItem1', {'colour': 'red'})
            self.put_attributes('MyDomain', 'MyItem2', {'colour': 'Blue'})
            self.put_attributes('MyDomain', 'MyItem3', {'shape': 'Blue'})
            self.put_attributes('MyDomain', 'MyItem4', {'shape': 'triangle'})
            self.put_attributes('MyDomain', 'MyItem4', {'shape': 'square'})

            res = self.select('select colour from MyDomain where colour like "Blue"')
            self.assertEquals(res, {'MyItem2': {'colour': set(['Blue'])}})
            res = self.select('select blah from MyDomain where colour like "Blue"')
            self.assertEquals(res, {})
            res = self.select('select shape from MyDomain where shape like "square"')
            self.assertEquals(res, {'MyItem4': {'shape': set(['triangle', 'square'])}})
        finally:
            self.delete_domain('MyDomain')

    def select(self, sql):
        resp, content = self.http.request("http://localhost:8000/?Action=Select&AWSAccessKeyId=valid_access_key_id&NextToken=valid_next_token&SelectExpression=%s&ConsistentRead=true&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%%3A03%%3A09-07%%3A00&Version=2009-04-15&Signature=valid_signature" % (urllib.quote(sql),), "GET")
        self.assertEquals(resp.status, 200)
        dom = xml.dom.minidom.parseString(content)
        select_result = dom.getElementsByTagName('SelectResult')[0]
        res = {}
        for item in select_result.getElementsByTagName('Item'):
            name = item.getElementsByTagName('Name')[0].childNodes[0].data
            attrs = {}
            for attr in item.getElementsByTagName('Attribute'):
                attr_name = attr.getElementsByTagName('Name')[0].childNodes[0].data
                attr_value = attr.getElementsByTagName('Value')[0].childNodes[0].data
                if attr_name not in attrs:
                    attrs[attr_name] = set()
                attrs[attr_name].add(attr_value)
            res[name] = attrs
        return res

    def delete_attributes(self, domain_name, item_name, attrs):
        s = ''
        i = 1
        for k, v in attrs.iteritems():
            s += '&Attribute.%d.Name=%s' % (i, k,)
            if v:
                s += '&Attribute.%d.Value=%s' % (i, v,)
            i += 1
        resp, content = self.http.request("http://localhost:8000/?Action=DeleteAttributes%s&AWSAccessKeyId=valid_access_key_id&DomainName=%s&ItemName=%s&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%%3A03%%3A05-07%%3A00&Version=2009-04-15&Signature=valid_signature" % (s, domain_name, item_name), "GET")
        self.assertEquals(resp.status, 200)
        xml.dom.minidom.parseString(content)

    def get_attributes(self, domain_name, item_name):
        resp, content = self.http.request("http://localhost:8000/?Action=GetAttributes&AWSAccessKeyId=valid_access_key:id&DomainName=%s&ItemName=%s&ConsistentRead=true&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%%3A03%%3A07-07%%3A00&Version=2009-04-15&Signature=valid_signature" % (domain_name, item_name), "GET")
        self.assertEquals(resp.status, 200)
        attrs = {}
        dom = xml.dom.minidom.parseString(content)
        res = dom.getElementsByTagName('GetAttributesResult')[0]
        for attr in res.getElementsByTagName('Attribute'):
            name = attr.getElementsByTagName('Name')[0].childNodes[0].data
            value = attr.getElementsByTagName('Value')[0].childNodes[0].data
            if not name in attrs:
                attrs[name] = set()
            attrs[name].add(value)
        return attrs

    def put_attributes(self, domain_name, item_name, attrs, replacements=None):
        s = ''
        i = 1
        for k, v in attrs.iteritems():
            s += '&Attribute.%d.Name=%s' % (i, k,)
            s += '&Attribute.%d.Value=%s' % (i, v,)
            if replacements and k in replacements:
                s += '&Attribute.%d.Replace=true' % (i,)
            i += 1
        resp, content = self.http.request("http://localhost:8000/?Action=PutAttributes%s&AWSAccessKeyId=valid_access_key_id&DomainName=%s&ItemName=%s&SignatureVersion=2&SignatureMethod=HmacSHA256&Timestamp=2010-01-25T15%%3A03%%3A05-07%%3A00&Version=2009-04-15&Signature=valid_signature" % (s, domain_name, item_name), "GET")
        self.assertEquals(resp.status, 200)
        xml.dom.minidom.parseString(content)

if __name__ == "__main__":
    unittest2.main()
