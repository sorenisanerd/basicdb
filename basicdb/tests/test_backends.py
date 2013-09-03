import unittest2

import basicdb.backends

class BaseStorageBackendTests(unittest2.TestCase):
    def test_create_domain_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.create_domain, "domain-name")
        
    def test_delete_domain_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.delete_domain, "domain-name")
        
    def test_domain_metadata_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.domain_metadata, "domain-name")

    def test_list_domains_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.list_domains)
        
    def test_add_attribute_value_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.add_attribute_value, "domain", "item", "attrname", "attrvalue")
        
    def test_delete_attribute_all_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.delete_attribute_all, "domain", "item", "attrname")
        
    def test_delete_attribute_value_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.delete_attribute_value, "domain", "item", "attrname", "attrvalue")
        
    def test_put_attributes(self):
        self.add_attributes_call_args = []
        self.replace_attributes_call_args = []

        class TestStoreBackend(basicdb.backends.StorageBackend):
            def add_attributes(self2, *args):
                self.add_attributes_call_args += [args]

            def replace_attributes(self2, *args):
                self.replace_attributes_call_args += [args]
                
        backend = TestStoreBackend()
        backend.put_attributes("domain", "item",
                               {"attr1": set(["attr1val1", "attr1val2"])},
                               {"attr2": set(["attr2val1"])})
        self.assertIn(("domain", "item", {"attr1": set(["attr1val1", "attr1val2"])}),
                      self.add_attributes_call_args)
        self.assertIn(("domain", "item", {"attr2": set(["attr2val1"])}),
                      self.replace_attributes_call_args)
                
    def test_get_attributes_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.get_attributes, "domain", "item")
        
    def test_add_attributes(self):
        self.add_attribute_call_args = []

        class TestStoreBackend(basicdb.backends.StorageBackend):
            def add_attribute(self2, *args):
                self.add_attribute_call_args += [args]
                
        backend = TestStoreBackend()
        backend.add_attributes("domain", "item", {"attr1": set(["attr1val1", "attr1val2"]),
                                                      "attr2": set(["attr2val1"])})

        self.assertEquals(len(self.add_attribute_call_args), 2)
        self.assertIn(("domain", "item", "attr1", set(["attr1val1", "attr1val2"])),
                      self.add_attribute_call_args)
        self.assertIn(("domain", "item", "attr2", set(["attr2val1"])),
                      self.add_attribute_call_args)
        
    def test_add_attribute(self):
        self.add_attribute_value_call_args = []

        class TestStoreBackend(basicdb.backends.StorageBackend):
            def add_attribute_value(self2, *args):
                self.add_attribute_value_call_args += [args]
                
        backend = TestStoreBackend()
        backend.add_attribute("domain", "item", "attr1", set(["attr1val1", "attr1val2"]))
        self.assertIn(("domain", "item", "attr1", "attr1val1"),
                      self.add_attribute_value_call_args)
        self.assertIn(("domain", "item", "attr1", "attr1val2"),
                      self.add_attribute_value_call_args)
        
        
    def test_replace_attributes(self):
        self.replace_attribute_call_args = []

        class TestStoreBackend(basicdb.backends.StorageBackend):
            def replace_attribute(self2, *args):
                self.replace_attribute_call_args += [args]
                
        backend = TestStoreBackend()
        backend.replace_attributes("domain", "item", {"attr1": set(["attr1val1", "attr1val2"]),
                                                      "attr2": set(["attr2val1"])})

        self.assertEquals(len(self.replace_attribute_call_args), 2)
        self.assertIn(("domain", "item", "attr1", set(["attr1val1", "attr1val2"])),
                      self.replace_attribute_call_args)
        self.assertIn(("domain", "item", "attr2", set(["attr2val1"])),
                      self.replace_attribute_call_args)

    def test_replace_attribute(self):
        self.delete_attributes_call_args = []
        self.add_attribute_call_args = []

        class TestStoreBackend(basicdb.backends.StorageBackend):
            def delete_attributes(self2, *args):
                self.delete_attributes_call_args += [args]
                
            def add_attribute(self2, *args):
                self.assertIsNot(self.delete_attributes_call_args, None,
                                 "Attribute wasn't deleted first")
                self.add_attribute_call_args += [args]
                
        backend = TestStoreBackend()
        backend.replace_attribute("domain", "item", "attr1", set(["attr1val1", "attr1val2"]))

        self.assertIn(("domain", "item", {"attr1": set([basicdb.AllAttributes])}), self.delete_attributes_call_args)
        self.assertIn(("domain", "item", "attr1", set(["attr1val1", "attr1val2"])),
                      self.add_attribute_call_args)

    def test_delete_attributes(self):
        self.delete_attribute_call_args = []

        class TestStoreBackend(basicdb.backends.StorageBackend):
            def delete_attribute(self2, *args):
                self.delete_attribute_call_args += [args]
                
        backend = TestStoreBackend()
        backend.delete_attributes("domain", "item", {"attr1": set(["attr1val1", "attr1val2"])})

        self.assertIn(("domain", "item", "attr1", set(["attr1val1", "attr1val2"])),
                      self.delete_attribute_call_args)

    def test_delete_attribute_only_calls_delete_all_if_all_should_be_removed(self):
        self.delete_attribute_all_call_args = []
        class TestStoreBackend(basicdb.backends.StorageBackend):
            def delete_attribute_all(self2, *args):
                self.delete_attribute_all_call_args += [args]
                
            def delete_attribute_value(self2, *args):
                self.fail("Should not have called delete_attribute_value")
                
        backend = TestStoreBackend()
        backend.delete_attribute("domain", "item", "attr1", set(["attr1val1", basicdb.AllAttributes, "attr1val2"]))
        self.assertEquals([("domain", "item", "attr1")], self.delete_attribute_all_call_args)

    def test_delete_attribute(self):
        self.delete_attribute_value_call_args = []
        class TestStoreBackend(basicdb.backends.StorageBackend):
            def delete_attribute_all(self2, *args):
                self.fail("Should not have called delete_attribute_all")
                
            def delete_attribute_value(self2, *args):
                self.delete_attribute_value_call_args += [args]
                
        backend = TestStoreBackend()
        backend.delete_attribute("domain", "item", "attr1", set(["attr1val1", "attr1val2"]))
        self.assertIn(("domain", "item", "attr1", "attr1val1"), self.delete_attribute_value_call_args)
        self.assertIn(("domain", "item", "attr1", "attr1val2"), self.delete_attribute_value_call_args)

    def test_select_raises_not_implemented(self):
        backend = basicdb.backends.StorageBackend()
        self.assertRaises(NotImplementedError, backend.select, "SELECT somethign FROM somewhere")
        
class _GenericBackendDriverTest(unittest2.TestCase):
    def test_create_list_delete_domain(self):
        self.assertEquals(self.backend.list_domains(), [])

        self.backend.create_domain("domain1")
        self.assertEquals(set(self.backend.list_domains()), set(["domain1"]))

        self.backend.create_domain("domain2")
        self.assertEquals(set(self.backend.list_domains()), set(["domain1", "domain2"]))

        self.backend.delete_domain("domain1")
        self.assertEquals(set(self.backend.list_domains()), set(["domain2"]))
        
        self.backend.delete_domain("domain2")
        self.assertEquals(self.backend.list_domains(), [])
        
    def test_domain_metadata(self):
        self.backend.create_domain("domain1")
        self.backend.domain_metadata("domain1")

    def test_put_get_attributes(self):
        self.backend.create_domain("domain1")
        self.backend.put_attributes("domain1", "item1",
                                    {"a": set(["b", "c"])},
                                    {"d": set(["e"])})

        self.assertEquals(self.backend.get_attributes("domain1", "item1"),
                          {"a": set(["b", "c"]), "d": set(["e"])})

        self.backend.put_attributes("domain1", "item1",
                                    {"d": set(["f", "g"])},
                                    {"a": set(["h"])})

        self.assertEquals(self.backend.get_attributes("domain1", "item1"),
                          {"a": set(["h"]), "d": set(["e", "f", "g"])})

    def test_delete_attributes_non_existant_item(self):
        self.backend.create_domain("domain1")
        self.backend.delete_attributes("domain1", "item1",
                                       {"a": set(["b"]),
                                        "b": set([basicdb.AllAttributes])})

    def test_delete_attributes_non_existant_domain(self):
        self.backend.delete_attributes("domain1", "item1",
                                       {"a": set(["b"]),
                                        "b": set([basicdb.AllAttributes])})

    def test_delete_attributes(self):
        self.backend.create_domain("domain1")
        self.backend.put_attributes("domain1", "item1",
                                    {"a": set(["b", "c"]),
                                     "d": set(["e"]),
                                     "f": set(["g"])},
                                     {})

        self.backend.delete_attributes("domain1", "item1",
                                       {"a": set([basicdb.AllAttributes]),
                                        "d": set(["f"]),
                                        "f": set(["g"])})

        self.assertEquals(self.backend.get_attributes("domain1", "item1"),
                          {"d": set(["e"])})

    def test_select(self):
        self.backend.create_domain("domain1")
        self.backend.put_attributes("domain1", "item1",
                                    {"shape": set(["square", "triangle"]),
                                     "colour": set(["Blue"])}, {})
        self.backend.put_attributes("domain1", "item2",
                                    {"colour": set(["Blue"])}, {})
        self.backend.put_attributes("domain1", "item3",
                                    {"shape": set(["round"]),
                                     "colour": set(["Red"])}, {})

        self.assertEquals(self.backend.select("SELECT * FROM domain1"),
                          {"item1": {"shape": set(["square", "triangle"]),
                                     "colour": set(["Blue"])},
                           "item2": {"colour": set(["Blue"])},
                           "item3": {"shape": set(["round"]),
                                     "colour": set(["Red"])}})

        self.assertEquals(self.backend.select("SELECT shape FROM domain1"),
                          {"item1": {"shape": set(["square", "triangle"])},
                           "item3": {"shape": set(["round"])}})

        self.assertEquals(self.backend.select("SELECT shape FROM domain1 WHERE colour LIKE 'Blue'"),
                          {"item1": {"shape": set(["square", "triangle"])}})

        self.assertEquals(self.backend.select("SELECT shape FROM domain1 WHERE shape = 'triangle'"),
                          {"item1": {"shape": set(["square", "triangle"])}})



class FakeBackendDriverTest(_GenericBackendDriverTest):
    def setUp(self):
        import basicdb.backends.fake
        self.backend = basicdb.backends.fake.driver
        self.backend._reset()


