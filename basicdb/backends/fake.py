import re
import time

import basicdb
import basicdb.backends
import basicdb.sqlparser as sqlparser

class FakeBackend(basicdb.backends.StorageBackend):
    _domains = {}

    def _reset(self):
        self._domains = {}

    def create_domain(self, domain_name):
        self._domains[domain_name] = {}

    def delete_domain(self, domain_name):
        del self._domains[domain_name]

    def list_domains(self):
        return self._domains.keys() 

    def delete_attribute_all(self, domain_name, item_name, attr_name):
        if domain_name not in self._domains:
            return

        if item_name not in self._domains[domain_name]:
            return

        if attr_name in self._domains[domain_name][item_name]:
            del self._domains[domain_name][item_name][attr_name]

    def delete_attribute_value(self, domain_name, item_name, attr_name, attr_value):
        if (domain_name in self._domains and
            item_name in self._domains[domain_name] and
            attr_name in self._domains[domain_name][item_name]):
            try:
                self._domains[domain_name][item_name][attr_name].remove(attr_value)
            except KeyError:
                pass
            if not self._domains[domain_name][item_name][attr_name]:
                del self._domains[domain_name][item_name][attr_name]

    def add_attribute_value(self, domain_name, item_name, attr_name, attr_value):
        if not item_name in self._domains[domain_name]:
            self._domains[domain_name][item_name] = {}

        if not attr_name in self._domains[domain_name][item_name]:
            self._domains[domain_name][item_name][attr_name] = set()

        self._domains[domain_name][item_name][attr_name].add(attr_value)

    def get_attributes(self, domain_name, item_name):
        return self._domains[domain_name][item_name]

    def _get_all_items(self, domain_name):
        return self._domains[domain_name]

    def select(self, sql_expr):
        parsed = sqlparser.simpleSQL.parseString(sql_expr)
        domain_name = parsed.tables[0] # Only one table supported
        desired_attributes = parsed.columns

        filters = []
        if parsed.where:
            for clause in parsed.where[0][1:]:
                col_name, rel, rval = clause
                if rel == '=':
                    filters += [lambda x:any([f == rval for f in x.get(col_name, [])])]
                elif rel == 'like':
                    regex = re.compile(rval.replace('_', '.').replace('%', '.*'))
                    filters += [lambda x:any([regex.match(f) for f in x.get(col_name, [])])]

        matching_items = {}
        for item, item_attrs in self._get_all_items(domain_name).iteritems():
            if all(f(item_attrs) for f in filters):
                matching_items[item] = item_attrs

        if desired_attributes == '*':
            result = matching_items
        else:
            result = {}

            for item, item_attrs in matching_items.iteritems():
                matching_attributes = dict([(attr_name, attr_values) for attr_name, attr_values in item_attrs.iteritems() if attr_name in desired_attributes.asList()])
                if matching_attributes:
                    result[item] = matching_attributes

        return result

    def domain_metadata(self, domain_name):
        return {"ItemCount": len(self._domains[domain_name]),
                "ItemNamesSizeBytes": sum((len(s) for s in self._domains[domain_name].keys())),
                "AttributeNameCount": '12',
                "AttributeNamesSizeBytes": '120',
                "AttributeValueCount": '120',
                "AttributeValuesSizeBytes": '100020',
                "Timestamp": str(int(time.time()))}


driver = FakeBackend()
