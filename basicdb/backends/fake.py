import time

import basicdb
import basicdb.backends
import basicdb.sqlparser as sqlparser

class FakeBackend(basicdb.backends.StorageBackend):
    _users = {}

    def _reset(self):
        self._users = {}

    def _ensure_owner(self, owner):
        if not owner in self._users:
            self._users[owner] = {}

    def create_domain(self, owner, domain_name):
        self._ensure_owner(owner)
        self._users[owner][domain_name] = {}

    def delete_domain(self, owner, domain_name):
        self._ensure_owner(owner)
        del self._users[owner][domain_name]

    def list_domains(self, owner):
        self._ensure_owner(owner)
        return self._users[owner].keys() 

    def delete_attribute_all(self, owner, domain_name, item_name, attr_name):
        self._ensure_owner(owner)
        if domain_name not in self._users[owner]:
            return

        if item_name not in self._users[owner][domain_name]:
            return

        if attr_name in self._users[owner][domain_name][item_name]:
            del self._users[owner][domain_name][item_name][attr_name]

    def delete_attribute_value(self, owner, domain_name, item_name, attr_name, attr_value):
        self._ensure_owner(owner)
        if (domain_name in self._users[owner] and
            item_name in self._users[owner][domain_name] and
            attr_name in self._users[owner][domain_name][item_name]):
            try:
                self._users[owner][domain_name][item_name][attr_name].remove(attr_value)
            except KeyError:
                pass
            if not self._users[owner][domain_name][item_name][attr_name]:
                del self._users[owner][domain_name][item_name][attr_name]

    def add_attribute_value(self, owner, domain_name, item_name, attr_name, attr_value):
        self._ensure_owner(owner)
        if not item_name in self._users[owner][domain_name]:
            self._users[owner][domain_name][item_name] = {}

        if not attr_name in self._users[owner][domain_name][item_name]:
            self._users[owner][domain_name][item_name][attr_name] = set()

        self._users[owner][domain_name][item_name][attr_name].add(attr_value)

    def get_attributes(self, owner, domain_name, item_name):
        self._ensure_owner(owner)
        return self._users[owner][domain_name][item_name]

    def _get_all_items(self, owner, domain_name):
        self._ensure_owner(owner)
        return self._users[owner][domain_name]

    def select(self, owner, sql_expr):
        self._ensure_owner(owner)
        parsed = sqlparser.parse(sql_expr)
        domain_name = parsed.table
        desired_attributes = parsed.columns

        if parsed.where_expr != '':
            f = lambda kv:parsed.where_expr.match(*kv)
        else:
            f = lambda _:True

        matching_items = dict(filter(f, self._get_all_items(owner, domain_name).iteritems()))

        if desired_attributes == ['*']:
            result = matching_items
        else:
            result = {}

            for item, item_attrs in matching_items.iteritems():
                matching_attributes = dict([(attr_name, attr_values) for attr_name, attr_values in item_attrs.iteritems() if attr_name in desired_attributes])
                if matching_attributes:
                    result[item] = matching_attributes

        return result

    def domain_metadata(self, owner, domain_name):
        self._ensure_owner(owner)
        return {"ItemCount": len(self._users[owner][domain_name]),
                "ItemNamesSizeBytes": sum((len(s) for s in self._users[owner][domain_name].keys())),
                "AttributeNameCount": '12',
                "AttributeNamesSizeBytes": '120',
                "AttributeValueCount": '120',
                "AttributeValuesSizeBytes": '100020',
                "Timestamp": str(int(time.time()))}

    def check_expectation(self, owner, domain_name, item_name, expectation):
        self._ensure_owner(owner)
        if (domain_name not in self._users[owner] or
            item_name not in self._users[owner][domain_name]):
            return False

        attr_name, attr_value_expected = expectation

        attr_value = self._users[owner][domain_name][item_name].get(attr_name, False)

        if attr_value == False:
            if attr_value_expected == False:
                return True
            return False
        elif attr_value_expected == True:
            return True
        else:
            return attr_value_expected in attr_value
         

driver = FakeBackend()
