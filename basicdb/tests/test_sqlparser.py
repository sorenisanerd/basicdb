import unittest2

from basicdb import sqlparser, exceptions

class SQLParserTest(unittest2.TestCase):
    def test_simple_select_asterisk(self):
        expr = sqlparser.parse("SELECT * FROM foobar")
        self.assertEquals(expr.columns[0], "*")
        self.assertEquals(expr.table, "foobar")

    def test_simple_select_with_limit(self):
        expr = sqlparser.parse("SELECT * FROM foobar limit 1")
        self.assertEquals(expr.columns[0], "*")
        self.assertEquals(expr.table, "foobar")

    def test_simple_select_specific_column(self):
        expr = sqlparser.parse("SELECT something FROM foobar")
        self.assertEquals(expr.columns, ["something"])
        self.assertEquals(expr.table, "foobar")

    def test_simple_select_multiple_specific_column(self):
        expr = sqlparser.parse("SELECT something, somethingelse FROM foobar")
        self.assertEquals(expr.columns, ["something", "somethingelse"])
        self.assertEquals(expr.table, "foobar")

    def test_where_compares_two_identifiers_raises_exception(self):
        self.assertRaises(exceptions.InvalidQueryExpression,
                          sqlparser.parse, "SELECT * FROM foobar WHERE a = b")

    def test_where_compares_two_literals_raises_exception(self):
        self.assertRaises(exceptions.InvalidQueryExpression,
                          sqlparser.parse, "SELECT * FROM foobar WHERE 'a' = 'b'")

    def test_where_literal_integer_raises_exception(self):
        self.assertRaises(exceptions.InvalidQueryExpression,
                          sqlparser.parse, "SELECT * FROM foobar WHERE a = 10")

    def test_where_compares_literal_and_identifier(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a = 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['b'])}))

    def test_where_compares_identifier_and_literal(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE 'a' = b")
        self.assertTrue(expr.where_expr.match('item1', {'b': set(['a'])}))

    def test_select_with_multi_clause_where(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a = 'b' and c = 'd'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['b']),
                                                        'c': set(['d'])}))

    def test_where_equality(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a = 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['b'])}))

    def test_where_equality_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a = 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_where_inequality(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a != 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_where_inequality_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a != 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['b'])}))

    def test_where_less_than(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a < 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['a'])}))

    def test_where_less_than_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a < 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_where_greater_than(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a > 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_where_greater_than_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a > 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['a'])}))

    def test_where_less_than_or_equal(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a <= 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['b'])}))

    def test_where_less_than_or_equal_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a <= 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_where_between_and(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a between 'b' and 'f'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['d'])}))

    def test_where_every(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE every(a) in ('b','c')")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['b', 'c'])}))

    def test_where_every_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE every(a) in ('b','c')")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['b', 'c', 'd'])}))

    def test_where_greater_than_or_equal(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a >= 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['b'])}))

    def test_where_greater_than_or_equal_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a >= 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['a'])}))

    def test_where_in(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a in ('b', 'c', 'd', 'e')")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['d'])}))

    def test_where_in_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a in ('b', 'c', 'd', 'e')")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['a'])}))

    def test_select_with_both_and_and_or(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE m = 'a' or a > 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_select_with_both_and_and_or_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE m = 'a' or n > 'b'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_select_with_multiple_comparisons_against_same_identifier(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a < 'd' and a > 'b'")
        self.assertTrue(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_select_with_multiple_comparisons_against_same_identifier_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a < 'd' and a > 'd'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['c'])}))

    def test_select_with_multiple_comparisons_against_same_multivalued_identifier(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a > 'd' and a < 'd'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['c', 'e'])}))

    def test_select_with_multi_clause_where_negative(self):
        expr = sqlparser.parse("SELECT * FROM foobar WHERE a = 'b' and c = 'd'")
        self.assertFalse(expr.where_expr.match('item1', {'a': set(['b']),
                                                        'c': set(['e'])}))

    def test_nonsense_throws_exception(self):
        self.assertRaises(Exception, sqlparser.parse, "NOT a vAlID SQL expression")
