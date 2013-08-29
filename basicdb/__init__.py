import falcon
import re
import time
import urllib
import uuid
import xml.etree.cElementTree as etree

_domains = {}

class Authentication(object):
    pass

class DomainResource(object):
    def on_get(self, req, resp):
        start_time = time.time()

        metadata = etree.Element("ResponseMetadata")
        etree.SubElement(metadata, "RequestId").text = str(uuid.uuid4())

        action = req.get_param("Action")
        if action == "CreateDomain":
            domain_name = req.get_param("DomainName")
            _domains[domain_name] = {}
            resp.status = falcon.HTTP_200  # This is the default status

            dom = etree.Element("CreateDomainResponse")
        elif action == "DeleteDomain":
            domain_name = req.get_param("DomainName")
            del _domains[domain_name]
            resp.status = falcon.HTTP_200  # This is the default status

            dom = etree.Element("DeleteDomainResponse")
        elif action == "ListDomains":
            resp.status = falcon.HTTP_200
            dom = etree.Element("ListDomainsResponse")

            result = etree.SubElement(dom, "ListDomainsResult")
            for name in _domains.keys():
                domain_name = etree.SubElement(result, "DomainName")
                domain_name.text = name
        elif action == "DeleteAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")
            if not item_name in _domains[domain_name]:
                return

            attrs = {}
            r = re.compile(r'Attribute\.(\d+)\.(Name|Value)')
            for (k, v) in req._params.iteritems():
                match = r.match(k)
                if not match:
                    continue

                idx, elem = match.groups()
                if idx not in attrs:
                    attrs[idx] = {}
                attrs[idx][elem] = v

            for idx, data in attrs.iteritems():
                if 'Name' not in attrs[idx]:
                    return

                if 'Value' in attrs[idx]:
                    _domains[domain_name][item_name][attrs[idx]['Name']].remove(attrs[idx]['Value'])
                else:
                    del _domains[domain_name][item_name][attrs[idx]['Name']]

            resp.status = falcon.HTTP_200
            dom = etree.Element("DeleteAttributesResponse")
        elif action == "PutAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")
            if not item_name in _domains[domain_name]:
                _domains[domain_name][item_name] = {}

            attrs = {}
            r = re.compile(r'Attribute\.(\d+)\.(Name|Value|Replace)')
            for (k, v) in req._params.iteritems():
                match = r.match(k)
                if not match:
                    continue

                idx, elem = match.groups()
                if idx not in attrs:
                    attrs[idx] = {}
                attrs[idx][elem] = v

            for idx, data in attrs.iteritems():
                if 'Name' in attrs[idx] and 'Value' in attrs[idx]:
                    name = attrs[idx]['Name']
                    value = attrs[idx]['Value']
                    if (name not in _domains[domain_name][item_name]) or ('Replace' in attrs[idx] and attrs[idx]['Replace'] == 'true'):
                        _domains[domain_name][item_name][name] = []
                    _domains[domain_name][item_name][name] += [value]

            resp.status = falcon.HTTP_200
            dom = etree.Element("PutAttributesResponse")
        elif action == "Select":
            import sqlparser
            sql_stmt = urllib.unquote(req.get_param('SelectExpression'))
            parsed = sqlparser.simpleSQL.parseString(sql_stmt)
            domain_name = parsed.tables[0]
            attrs = parsed.columns
            matching_items = {}
            filters = []
            if parsed.where:
                for clause in parsed.where[0][1:]:
                    if len(clause) == 0:
                        continue
                    col_name, rel, rval = clause
                    if rel == '=':
                        filters += [lambda x:x[col_name] == rval]
                    elif rel == 'like':
                        regex = re.compile(rval.replace('_', '.').replace('%', '.*'))
                        filters += [lambda x:any([regex.match(f) for f in x.get(col_name, [])])]

            for item, item_attrs in _domains[domain_name].iteritems():
                if all(f(item_attrs) for f in filters):
                    matching_items[item] = item_attrs

            resp.status = falcon.HTTP_200
            dom = etree.Element("SelectResponse")
            result = etree.SubElement(dom, "SelectResult")

            for item, item_attrs in matching_items.iteritems():
                requested_attributes = []
                for name, l in item_attrs.iteritems():
                    if (attrs == '*') or (name in attrs.asList()):
                        requested_attributes += [(name, value) for value in l]

                if not requested_attributes:
                    continue
                item_elem = etree.SubElement(result, "Item")
                etree.SubElement(item_elem, "Name").text = item
                for name, value in requested_attributes:
                    attr_elem = etree.SubElement(item_elem, "Attribute")
                    etree.SubElement(attr_elem, "Name").text = name
                    etree.SubElement(attr_elem, "Value").text = value
        elif action == "DomainMetadata":
            domain_name = req.get_param("DomainName")
            resp.status = falcon.HTTP_200
            dom = etree.Element("DomainMetadataResponse")
            result = etree.SubElement(dom, "DomainMetadataResult")
            etree.SubElement(result, "ItemCount").text = str(len(_domains[domain_name]))
            etree.SubElement(result, "ItemNamesSizeBytes").text = str(sum((len(s) for s in _domains[domain_name].keys())))
            etree.SubElement(result, "AttributeNameCount").text = '12'
            etree.SubElement(result, "AttributeNamesSizeBytes").text = '120'
            etree.SubElement(result, "AttributeValueCount").text = '120'
            etree.SubElement(result, "AttributeValuesSizeBytes").text = '100020'
            etree.SubElement(result, "Timestamp").text = str(int(time.time()))

        elif action == "GetAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")
            resp.status = falcon.HTTP_200
            dom = etree.Element("GetAttributesResponse")
            result = etree.SubElement(dom, "GetAttributesResult")

            for k, v in _domains[domain_name][item_name].iteritems():
                for x in v:
                    attr_elem = etree.SubElement(result, "Attribute")
                    etree.SubElement(attr_elem, "Name").text = k
                    etree.SubElement(attr_elem, "Value").text = x
        else:
            print "Unknown action: %s" % (action,)

        time_spent = time.time() - start_time
        etree.SubElement(metadata, "BoxUsage").text = str(time_spent)
        dom.append(metadata)
        resp.body = etree.tostring(dom)

    on_post = on_get

app = api = falcon.API()
domains = DomainResource()
api.add_route('/', domains)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    import sys
    port = int(sys.argv[1])
    s = make_server('localhost', port, app)
    s.serve_forever()
