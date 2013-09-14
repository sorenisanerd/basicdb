import errno
import glob
import md5
import os
import re
import shutil
import time

import basicdb
import basicdb.backends
import basicdb.sqlparser as sqlparser

class FileSystemBackend(basicdb.backends.StorageBackend):
    _domains = {}

    def __init__(self, base_dir='/tmp/mystor'):
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def _md5_hex(self, s):
        return md5.md5(s).hexdigest()

    def _attr_value_filename(self, owner, domain_name, item_name, attr_name, attr_value):
        return os.path.join(self._attr_dir(owner, domain_name, item_name, attr_name),
                            self._md5_hex(attr_value))

    def _reset(self):
        # What to put here as some sort of safeguard?
        try:
            shutil.rmtree(self.base_dir)
        except OSError, e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise
        os.mkdir(self.base_dir)

    def _owner_dir(self, owner):
        owner_dir = os.path.join(self.base_dir, owner)
        if not os.path.exists(owner_dir):
            os.makedirs(owner_dir)
        return owner_dir

    def _domain_dir(self, owner, domain_name):
        return os.path.join(self._owner_dir(owner), domain_name)

    def _item_dir(self, owner, domain_name, item_name):
        return os.path.join(self._domain_dir(owner, domain_name), item_name)

    def _attr_dir(self, owner, domain_name, item_name, attr_name):
        return os.path.join(self._item_dir(owner, domain_name, item_name), attr_name)

    def create_domain(self, owner, domain_name):
        try:
            os.mkdir(self._domain_dir(owner, domain_name))
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise

    def delete_domain(self, owner, domain_name):
        try:
            shutil.rmtree(self._domain_dir(owner, domain_name))
        except OSError, e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise

    def list_domains(self, owner):
        return [d.split('/')[-1] for d in glob.glob(os.path.join(self._owner_dir(owner), '*'))]

    def delete_attribute_all(self, owner, domain_name, item_name, attr_name):
        attr_dir = self._attr_dir(owner, domain_name, item_name, attr_name)
        if os.path.exists(attr_dir):
            shutil.rmtree(attr_dir)

    def delete_attribute_value(self, owner, domain_name, item_name, attr_name, attr_value):
        try:
            os.unlink(self._attr_value_filename(owner, domain_name, item_name, attr_name, attr_value))
        except OSError, e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise
        try:
            os.rmdir(self._attr_dir(owner, domain_name, item_name, attr_name))
        except OSError, e:
            if e.errno in (errno.ENOTEMPTY, errno.ENOENT):
                pass
            else:
                raise

    def add_attribute_value(self, owner, domain_name, item_name, attr_name, attr_value):
        try:
            os.makedirs(self._attr_dir(owner, domain_name, item_name, attr_name))
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass

        with file(self._attr_value_filename(owner, domain_name, item_name,
                                            attr_name, attr_value), 'w') as fp:
            fp.write(attr_value)

    def get_attributes(self, owner, domain_name, item_name):
        retval = {}
        for attr_dir in glob.glob(os.path.join(self._item_dir(owner, domain_name, item_name), '*')):
            attr_name = attr_dir.split('/')[-1]
            retval[attr_name] = set()
            for attr_value_file in glob.glob(os.path.join(attr_dir, '*')):
                with file(attr_value_file, 'r') as fp:
                    retval[attr_name].add(fp.read())
        return retval

    def _get_all_items_names(self, owner, domain_name):
        return [d.split('/')[-1] for d in glob.glob(os.path.join(self._domain_dir(owner, domain_name), '*'))]

    def _get_all_items(self, owner, domain_name):
        retval = {}
        for item_name in self._get_all_items_names(owner, domain_name):
            retval[item_name] = self.get_attributes(owner, domain_name, item_name)
        return retval

    def select(self, owner, sql_expr):
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
        return {"ItemCount": len(self._get_all_items_names(owner, domain_name)),
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


driver = FileSystemBackend()
