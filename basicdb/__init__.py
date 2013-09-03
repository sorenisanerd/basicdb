import falcon
import importlib
import re
import time
import urllib
import uuid
import xml.etree.cElementTree as etree

import basicdb.exceptions

backend_driver = 'fake'

backend =  importlib.import_module('basicdb.backends.%s' % (backend_driver,)).driver

class Authentication(object):
    pass

class AllAttributes(object):
    pass

class DomainResource(object):
    def on_get(self, req, resp):
        start_time = time.time()

        metadata = etree.Element("ResponseMetadata")
        etree.SubElement(metadata, "RequestId").text = str(uuid.uuid4())

        action = req.get_param("Action")
        if action == "CreateDomain":
            domain_name = req.get_param("DomainName")

            backend.create_domain(domain_name)

            resp.status = falcon.HTTP_200  # This is the default status
            dom = etree.Element("CreateDomainResponse")
        elif action == "DeleteDomain":
            domain_name = req.get_param("DomainName")

            backend.delete_domain(domain_name)

            resp.status = falcon.HTTP_200  # This is the default status
            dom = etree.Element("DeleteDomainResponse")
        elif action == "ListDomains":
            resp.status = falcon.HTTP_200
            dom = etree.Element("ListDomainsResponse")

            result = etree.SubElement(dom, "ListDomainsResult")
            for name in backend.list_domains():
                domain_name = etree.SubElement(result, "DomainName")
                domain_name.text = name
        elif action == "DeleteAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")

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

            deletions = {}
            for idx, data in attrs.iteritems():
                if 'Name' not in attrs[idx]:
                    continue

                attr_name = attrs[idx]['Name']

                if attr_name not in deletions:
                    deletions[attr_name] = set()

                if 'Value' in attrs[idx]:
                    deletions[attr_name].add(attrs[idx]['Value'])
                else:
                    deletions[attr_name].add(AllAttributes)

            backend.delete_attributes(domain_name, item_name, deletions)

            resp.status = falcon.HTTP_200
            dom = etree.Element("DeleteAttributesResponse")
        elif action == "PutAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")

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

            additions = {}
            replacements = {}
            for idx, data in attrs.iteritems():
                if 'Name' in attrs[idx] and 'Value' in attrs[idx]:
                    name = attrs[idx]['Name']
                    value = attrs[idx]['Value']
                    if 'Replace' in attrs[idx] and attrs[idx]['Replace'] == 'true':
                        if name not in replacements:
                            replacements[name] = set()
                        replacements[name].add(value)
                    else:
                        if name not in additions:
                            additions[name] = set()
                        additions[name].add(value)

            expectation_params = {}
            r = re.compile(r'Expected\.(\d+)\.(Name|Value|Exists)')
            for (k, v) in req._params.iteritems():
                match = r.match(k)
                if not match:
                    continue

                idx, elem = match.groups()
                if idx not in expectation_params:
                    expectation_params[idx] = {}
                expectation_params[idx][elem] = v

            expectations = []
            for idx, data in expectation_params.iteritems():
                if 'Name' in expectation_params[idx]:
                    if  'Value' in expectation_params[idx]:
                        expected_value = expectation_params[idx]['Value']
                    elif  'Exists' in expectation_params[idx]:
                        val = expectation_params[idx]['Exists']
                        expected_value = not (val == 'false')
                    expectations += [(expectation_params[idx]['Name'],
                                     expected_value)]


            try:
                backend.put_attributes(domain_name, item_name, additions, replacements,
                                       expectations)
                resp.status = falcon.HTTP_200
                dom = etree.Element("PutAttributesResponse")
            except basicdb.exceptions.APIException, e:
                resp.status = e.http_status
                dom = etree.Element(e.root_element)

        elif action == "Select":
            sql_expr = urllib.unquote(req.get_param('SelectExpression'))
            results = backend.select(sql_expr)

            resp.status = falcon.HTTP_200
            dom = etree.Element("SelectResponse")
            result = etree.SubElement(dom, "SelectResult")

            for item_name, item_attrs in results.iteritems():
                item_elem = etree.SubElement(result, "Item")
                etree.SubElement(item_elem, "Name").text = item_name
                for attr_name, attr_values in item_attrs.iteritems():
                    for attr_value in attr_values:
                        attr_elem = etree.SubElement(item_elem, "Attribute")
                        etree.SubElement(attr_elem, "Name").text = attr_name
                        etree.SubElement(attr_elem, "Value").text = attr_value
        elif action == "DomainMetadata":
            domain_name = req.get_param("DomainName")
            resp.status = falcon.HTTP_200
            dom = etree.Element("DomainMetadataResponse")
            result = etree.SubElement(dom, "DomainMetadataResult")
            for k, v in backend.domain_metadata(domain_name).iteritems():
                etree.SubElement(result, k).text = str(v)
        elif action == "GetAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")
            resp.status = falcon.HTTP_200
            dom = etree.Element("GetAttributesResponse")
            result = etree.SubElement(dom, "GetAttributesResult")

            for attr_name, attr_values in backend.get_attributes(domain_name, item_name).iteritems():
                for attr_value in attr_values:
                    attr_elem = etree.SubElement(result, "Attribute")
                    etree.SubElement(attr_elem, "Name").text = attr_name
                    etree.SubElement(attr_elem, "Value").text = attr_value
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
