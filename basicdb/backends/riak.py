from __future__ import absolute_import
import riak
import riak.resolver

import time
import uuid

import basicdb
import basicdb.backends
import basicdb.sqlparser as sqlparser

"""
Data model:

We have a bucket with a configurable name. Let's call it basebucket.

For each owner, there's a key in basebucket. It holds a dictionary:
{"domainname": "domainbucket",
 "domainname2": "domainbucket2",
 etc.}

Domainbucket names are randomly generated uuids.

For each item in the domain, there's a key in the corresponding bucket named "<item_name>".
It holds a dictionary of {"attr_name": ["attrvalue1", "attrvalue2"...]}
"""

class RiakBackend(basicdb.backends.StorageBackend):
    def __init__(self, base_bucket='basicdb'):
        self.riak = riak.RiakClient(pb_port=8087, protocol='pbc')
        self.base_bucket = self.riak.bucket(base_bucket)
        self.base_bucket.resolver = riak.resolver.last_written_resolver

    def _reset(self):
        for key in self.base_bucket.get_keys():
            self.base_bucket.delete(key, dw=3)

    def _owner_object(self, owner):
        obj = self.base_bucket.get(owner)
        if obj.data is None:
            obj.data = {}
        return obj

    def _domain_bucket_name(self, owner, domain):
        owner_domains = self._owner_object(owner).data
        return owner_domains[domain]

    def _domain_bucket(self, owner, domain):
        owner_domains = self._owner_object(owner).data
        if domain in owner_domains:
            return self.riak.bucket(owner_domains[domain])

    def _item_object(self, owner, domain_name, item_name, init=False):
        domain_bucket = self._domain_bucket(owner, domain_name)
        if domain_bucket is None:
            return
        item_object = domain_bucket.get(item_name)
        if item_object.data is None:
            if init:
                item_object.data = {}
            else:
                return None
        return item_object

    def create_domain(self, owner, domain_name):
        owner_object = self._owner_object(owner)

        if domain_name in owner_object.data:
            return

        bucket_uuid = str(uuid.uuid4())
        owner_object.data[domain_name] = bucket_uuid
        owner_object.store()

    def delete_domain(self, owner, domain_name):
        owner_object = self._owner_object(owner)

        if domain_name not in owner_object.data:
            return

        del owner_object.data[domain_name]
        owner_object.store()

    def list_domains(self, owner):
        return self._owner_object(owner).data.keys()

    def delete_attribute_all(self, owner, domain_name, item_name, attr_name):
        item_object = self._item_object(owner, domain_name, item_name)
        if item_object is None:
            return

        if item_object.data is not None and attr_name in item_object.data:
            del item_object.data[attr_name]

        item_object.store()

    def delete_attribute_value(self, owner, domain_name, item_name, attr_name, attr_value):
        item_object = self._item_object(owner, domain_name, item_name)
        if item_object is None:
            return

        if item_object.data is not None and attr_name in item_object.data:
            try:
                item_object.data[attr_name].remove(attr_value)
                if not item_object.data[attr_name]:
                    del item_object.data[attr_name]
                item_object.store()
            except ValueError:
                pass

    def add_attribute_value(self, owner, domain_name, item_name, attr_name, attr_value):
        item_object = self._item_object(owner, domain_name, item_name, init=True)

        if attr_name not in item_object.data:
            item_object.data[attr_name] = [attr_value]
        else:
            item_object.data[attr_name].append(attr_value)
        
        item_object.store()

    def get_attributes(self, owner, domain_name, item_name):
        try:
            item_object = self._item_object(owner, domain_name, item_name)
            if item_object is not None and item_object.data is not None:
                return dict([(attr_name, set(attr_values)) for attr_name, attr_values in item_object.data.iteritems()])
        except KeyError:
            pass
        return {}

    def select(self, owner, sql_expr):
        import riak.mapreduce
        mapred = riak.mapreduce.RiakMapReduce(self.riak)

        parsed = sqlparser.simpleSQL.parseString(sql_expr)
        domain_name = parsed.tables[0] # Only one table supported
        desired_attributes = parsed.columns

        mapred.add_bucket(self._domain_bucket_name(owner, domain_name))
        
        if parsed.where[0] == '':
            item_filter_js_expr = 'matched = true;'
        else:
            item_filter_js_expr = '''for (x in val) {
                                         for (y in val[x]) {
                                             if ('''
            for clause in parsed.where[0][1:]:
                col_name, rel, rval = clause
                if rel == '=':
                    item_filter_js_expr += "val[x][y] == '%s'" % (rval,)
                elif rel == 'like':
                    item_filter_js_expr += "val[x][y].match('%s')" % (rval.replace('_', '.').replace('%', '.*'),)
            item_filter_js_expr += ''') { matched = true; break; } } }'''


        if desired_attributes == '*':
            attr_filter_expr = 'true'
        else:
            attr_filter_expr = ' || '.join(["key == '%s'" % col for col in desired_attributes])

        js_func = '''function(riakObject) {
                          var retval = {};
                          var val = JSON.parse(riakObject.values[0].data);
                          var matched = false;
                          var nonempty = false;

                          %s

                          if (!matched) {
                              return [[]];
                          }
                          for (key in val) {
                              if (%s) {
                                  retval[key] = val[key];
                                  nonempty = true;
                              }
                          }
                          if (nonempty) {
                              return [[riakObject.key, retval]];
                          } else {
                              return [[]];
                          }
                      }''' % (item_filter_js_expr, attr_filter_expr,)
        mapred.map(js_func)
        result = mapred.run()
        if not result:
            return {}
        retval = {}
        for row in result:
            if not row:
                continue
            item_name, data = row
            retval[str(item_name)] = dict((str(attr_name), set([str(s) for s in attr_values])) for attr_name, attr_values in data.iteritems())

        return retval

    def domain_metadata(self, owner, domain_name):
        return {"ItemCount": len(self._domain_bucket(owner, domain_name).get_keys()),
                "ItemNamesSizeBytes": '120',
                "AttributeNameCount": '12',
                "AttributeNamesSizeBytes": '120',
                "AttributeValueCount": '120',
                "AttributeValuesSizeBytes": '100020',
                "Timestamp": str(int(time.time()))}

    def check_expectation(self, owner, domain_name, item_name, expectation):
        attr_name, attr_value_expected = expectation

        attrs = self.get_attributes(owner, domain_name, item_name)

        attr_value = attrs.get(attr_name, False)

        if attr_value == False:
            if attr_value_expected == False:
                return True
            return False
        elif attr_value_expected == True:
            return True
        else:
            return attr_value_expected in attr_value

driver = RiakBackend()
