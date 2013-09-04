import falcon
import importlib
import os
import time
import urllib
import uuid
import xml.etree.cElementTree as etree

from basicdb import utils
import basicdb.exceptions

global backend
backend = None

def load_backend(name):
    global backend
    backend = importlib.import_module('basicdb.backends.%s' % (name,)).driver

load_backend(os.environ.get('BASICDB_BACKEND_DRIVER', 'fake'))

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

            deletions = utils.extract_deletions_from_query_params(req)
            expectations = utils.extract_expectations_from_query_params(req)

            backend.delete_attributes(domain_name, item_name, deletions)

            resp.status = falcon.HTTP_200
            dom = etree.Element("DeleteAttributesResponse")
        elif action == "PutAttributes":
            domain_name = req.get_param("DomainName")
            item_name = req.get_param("ItemName")

            additions, replacements = utils.extract_additions_and_replacements_from_query_params(req)
            expectations = utils.extract_expectations_from_query_params(req)

            try:
                backend.put_attributes(domain_name, item_name, additions, replacements,
                                       expectations)
                resp.status = falcon.HTTP_200
                dom = etree.Element("PutAttributesResponse")
            except basicdb.exceptions.APIException, e:
                resp.status = e.http_status
                dom = etree.Element(e.root_element)

        elif action == "BatchDeleteAttributes":
            domain_name = req.get_param("DomainName")

            deletions = utils.extract_batch_deletions_from_query_params(req)

            try:
                backend.batch_delete_attributes(domain_name, deletions)
                resp.status = falcon.HTTP_200
                dom = etree.Element("BatchDeleteAttributesResponse")
            except basicdb.exceptions.APIException, e:
                resp.status = e.http_status
                dom = etree.Element(e.root_element)

        elif action == "BatchPutAttributes":
            domain_name = req.get_param("DomainName")

            additions, replacements = utils.extract_batch_additions_and_replacements_from_query_params(req)

            try:
                backend.batch_put_attributes(domain_name, additions, replacements)
                resp.status = falcon.HTTP_200
                dom = etree.Element("BatchPutAttributesResponse")
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
            resp.status = falcon.HTTP_500
            dom = etree.Element("UnknownCommand")
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
