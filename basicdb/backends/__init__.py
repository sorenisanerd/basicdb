import basicdb
import basicdb.exceptions

class StorageBackend(object):
    def create_domain(self, domain_name):
        """Create a new domain"""
        raise NotImplementedError()

    def delete_domain(self, domain_name):
        """Delete a domain"""
        raise NotImplementedError()

    def list_domains(self):
        """List domains"""
        raise NotImplementedError()

    def batch_put_attributes(self, domain_name, additions, replacements):
        """Update attributes on multiple items at a time
        
        replacements is a dict where keys are item names and the corresponding
        value is another dict where keys are attribute names and the
        corresponding value is a set of values that are to replace any current
        values associated with the given attribute.
        additions is a dict where keys are item names and the corresponding
        value is another dict where keys are attribute names and the
        corresponding value is a set of values that are to be added to the
        given attribute.
        
        If the backend does not have a quick mechanism for this, just leave
        this method alone and implement some of the more low-level methods"""
        for item_name in set(additions.keys() + replacements.keys()):
            self.put_attributes(domain_name, item_name,
                                additions.get(item_name, {}),
                                replacements.get(item_name, {}))

    def put_attributes(self, domain_name, item_name, additions, replacements,
                       expectations=None):
        """Update the set of attributes on the given item:
        
        replacements is a dict where keys are attribute names and the
        corresponding value is a set of values that are to replace any
        current values associated with the given attribute.
        additions is a dict where keys are attribute names and the
        corresponding value is a set of values that are to be added to
        the given attribute.
        expectations is an iterable of 2-tuples (attr_name, expected_value).
        If expected_value is a boolean, it denotes whether the value is expected
        to exist.
        
        If the backend does not have a quick mechanism for this, just leave
        this method alone and implement some of the more low-level methods"""
        if expectations and not self.check_expectations(domain_name, item_name, expectations):
            raise basicdb.exceptions.ConditionalCheckFailed()
        self.add_attributes(domain_name, item_name, additions)
        self.replace_attributes(domain_name, item_name, replacements)

    def add_attributes(self, domain_name, item_name, additions):
        """Add attributes to an item

        additions is a dict where keys are attribute names and the
        corresponding value is a set of values that are to be added to
        the given attribute."""
        for attr_name, attr_values in additions.iteritems():
            self.add_attribute(domain_name, item_name, attr_name, attr_values)
            
    def add_attribute(self, domain_name, item_name, attribute_name, attribute_values):
        """Adds an attribute to an item (preserving existing attributes
        of the same name

        attribute_value is an iterable of values to be added for the given attribute
        name."""
        for value in attribute_values:
            self.add_attribute_value(domain_name, item_name, attribute_name, value)

    def add_attribute_value(self, domain_name, item_name, attribute_name, attribute_value):
        """Adds a single value to an attribute (preserving existing values)"""
        raise NotImplementedError()

    def replace_attributes(self, domain_name, item_name, replacements):
        """Replaces attributes
        
        replacements is a dict where keys are attribute names and the
        corresponding value is a set of values that are to replace any
        current values associated with the given attribute."""
        for attr_name, values in replacements.iteritems():
            self.replace_attribute(domain_name, item_name, attr_name, values)

    def replace_attribute(self, domain_name, item_name, attr_name, attr_values):
        """Replaces that current set of values associated witha given attribute
        with a new set"""
        self.delete_attributes(domain_name, item_name, {attr_name: set([basicdb.AllAttributes])})
        self.add_attribute(domain_name, item_name, attr_name, attr_values)

    def delete_attributes(self, domain_name, item_name, deletions):
        for attr_name, attr_values in deletions.iteritems():
            self.delete_attribute(domain_name, item_name, attr_name, attr_values)

    def delete_attribute(self, domain_name, item_name, attr_name, attr_values):
        if basicdb.AllAttributes in attr_values:
            self.delete_attribute_all(domain_name, item_name, attr_name)
        else:
            for attr_value in attr_values:
                self.delete_attribute_value(domain_name, item_name, attr_name, attr_value)

    def delete_attribute_all(self, domain_name, item_name, attr_name):
        raise NotImplementedError()

    def delete_attribute_value(self, domain_name, item_name, attr_name, attr_value):
        raise NotImplementedError()

    def get_attributes(self, domain_name, item_name):
        raise NotImplementedError()

    def select(self, sql_expr):
        raise NotImplementedError()

    def domain_metadata(self, domain_name):
        raise NotImplementedError()

    def check_expectations(self, domain_name, item_name, expectations):
        return all([self.check_expectation(domain_name, item_name, expectation) for expectation in expectations])
        
    def check_expectation(self, domain_name, item_name, expectations):
        raise NotImplementedError()
