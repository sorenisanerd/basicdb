# coding=utf-8
# SQL parser based on select_parser.py from the pyparsing distribution
#
# Copyright © 2010, Paul McGuire
# Copyright © 2013, Soren Hansen
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import itertools
import re

from pyparsing import ParserElement, Forward, Suppress, CaselessKeyword, MatchFirst, \
                      alphas, alphanums, Regex, QuotedString, oneOf, Keyword, \
                      Word, Optional, delimitedList, operatorPrecedence, opAssoc, \
                      Group, ParseException

from basicdb import exceptions

ParserElement.enablePackrat()

UNARY,BINARY,TERNARY=1,2,3
LPAR,RPAR,COMMA = map(Suppress,"(),")

# keywords
(  UNION, ALL, AND, OR, INTERSECT, INTERSECTION, EXCEPT, COLLATE, ASC, DESC, ON,
 NOT, SELECT, DISTINCT, FROM, WHERE, BY, ORDER, BY, LIMIT, EVERY) = map(CaselessKeyword,
"""UNION, ALL, AND, OR, INTERSECT, INTERSECTION, EXCEPT, COLLATE, ASC, DESC, ON,
 NOT, SELECT, DISTINCT, FROM, WHERE, BY, ORDER, BY, LIMIT, EVERY""".replace(",","").split())

(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE,
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE,
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())

keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON,
 NOT, SELECT, DISTINCT, FROM, WHERE, BY, EVERY,
 ORDER, BY, LIMIT, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP))

def regex_from_like(s):
    return str(s).replace('*', '\*').replace('_', '.').replace('%', '.*')


def lookup(id):
    return

def lookup_every(id):
    return

class SqlParser(object):
    class BoolOperand(object):
        def __lt__(self, other):
            if isinstance(other, SqlParser.BoolOperand):
                return all(cmp(v1, v2) < 0 for v1, v2 in itertools.product(self.value, other.value))
            return NotImplemented

        def __le__(self, other):
            if isinstance(other, SqlParser.BoolOperand):
                return all(cmp(v1, v2) <= 0 for v1, v2 in itertools.product(self.value, other.value))
            return NotImplemented

        def __gt__(self, other):
            if isinstance(other, SqlParser.BoolOperand):
                return all(cmp(v1, v2) > 0 for v1, v2 in itertools.product(self.value, other.value))
            return NotImplemented

        def __ge__(self, other):
            if isinstance(other, SqlParser.BoolOperand):
                return all(cmp(v1, v2) >= 0 for v1, v2 in itertools.product(self.value, other.value))
            return NotImplemented

        def __eq__(self, other):
            if isinstance(other, SqlParser.BoolOperand):
                return all(cmp(v1, v2) == 0 for v1, v2 in itertools.product(self.value, other.value))
            return NotImplemented

        def __ne__(self, other):
            if isinstance(other, SqlParser.BoolOperand):
                return all(cmp(v1, v2) != 0 for v1, v2 in itertools.product(self.value, other.value))
            return NotImplemented

        def comparable(self):
            return True

        def get_single_value_or_raise(self):
            if len(self.value) > 1:
                raise Exception("Asked for singular value, but multiple values were available")
            else:
                return self.value.copy().pop()

    class OrderByTerms(object):
        def __init__(self, t):
            t = t[:]
            if len(t) <= 2:
                self.key = None
            else:
                terms = t[2][:]
                self.key = terms[0].reference
                if len(terms) > 1 and terms[1] == 'DESC':
                    self.reverse = True
                else:
                    self.reverse = False

    class ValueList(BoolOperand):
        def __init__(self, t):
            self.value = self
            self._value = t[:]

        def __str__(self):
            return str(self._value)

        def __contains__(self, item):
            for needle in item.value:
                if not any(needle == v.get_single_value_or_raise() for v in self._value):
                    return False
            return True

        def riak_js_expr(self):
            return '[' + ','.join([val.riak_js_expr() for val in self._value]) + ']'


        def identifiers(self):
            return []

    def MakeValueList(*args, **kwargs):
        return ValueList(*args, **kwargs)

    class Null(BoolOperand):
        def identifiers(self):
            return []

        def riak_js_expr(self):
            return 'undefined'

    class Literal(BoolOperand):
        def __init__(self, t):
            self.value = set([t[0]])

        def riak_js_expr(self):
            return repr(self.get_single_value_or_raise())

        def __str__(self):
            return str(self.get_single_value_or_raise())

        def identifiers(self):
            return []

    class Identifier(BoolOperand):
        def __init__(self, t):
            self.reference = t[0]

        @property
        def value(self):
            val = lookup(self.reference)
            if val is None:
                return val
            return set([lookup(self.reference)])

        def riak_js_expr(self):
            return '[vals[%r]]' % (self.reference,)

        def comparable(self):
            if self.value is None:
                return False
            return True

        def identifiers(self):
            return [self.reference]


    class ItemName(Identifier):
        def riak_js_expr(self):
            return '[riakObject.key]'


    class Count(Identifier):
        def riak_js_expr(self):
            return '[riakObject.key]'


    class EveryIdentifier(Identifier):
        def __init__(self, t):
            self.reference = t[1].reference

        @property
        def value(self):
            return lookup_every(self.reference)

        def riak_js_expr(self):
            return 'val[%r]' % (self.reference,)

    class BoolOperator(object):
        def __init__(self, t):
            self.args = t[0][0::2]
            if not all(isinstance(arg, (SqlParser.BinaryComparisonOperator, SqlParser.BoolOperator)) for arg in self.args):
                raise ParseException("Invalid query")

        def __nonzero__(self):
            return self.__bool__()

        def riak_js_expr(self):
            ident = filter(lambda x:isinstance(x, SqlParser.Identifier), self.args)
            if len(ident) > 0:
                ident = ident[0]
                return "((%s != undefined) && %s.filter(function l(x) { return !(%s %s %s); }).length == 0)" % (ident.riak_js_expr(), ident.riak_js_expr(),
                                                                                                                self.args[0] is ident and 'x' or self.args[0].riak_js_expr(),
                                                                                                                self.riak_js_oper,
                                                                                                                self.args[1] is ident and 'x' or self.args[1].riak_js_expr())
            else:
               sep = " %s " % self.riak_js_oper
               return "(" + sep.join(map(lambda a:a.riak_js_expr(),self.args)) + ")"

        def __str__(self):
            sep = " %s " % self.reprsymbol
            return "(" + sep.join(map(str,self.args)) + ")"

        def match(self, item_name, attrs):
            def set_iter(key, vals):
                return ((key, v) for v in vals)

            return any((self._match(item_name, dict(values), attrs) for values in itertools.product(*[set_iter(key, attrs[key]) for key in attrs.keys()])))

        def _match(self, item_name, attrs, raw_attrs):
            global lookup, lookup_every
            def _lookup(key):
                if key == 'itemName()':
                    return item_name
                return attrs.get(key, None)
            def _lookup_every(key):
                return raw_attrs.get(key, None)
            saved_lookup, saved_lookup_every = lookup, lookup_every
            try:
                lookup, lookup_every = _lookup, _lookup_every
                return self.__bool__()
            finally:
                lookup, lookup_every = saved_lookup, saved_lookup_every

        def identifiers(self):
            return sum([arg.identifiers() for arg in self.args], [])

    class Intersection(BoolOperator):
        reprsymbol = 'INTERSECTION'

        def __init__(self, t):
            self.args = t[0][::2]

        def match(self, item_name, attrs):
            return all(subexpr.match(item_name, attrs) for subexpr in self.args)

        def riak_js_expr(self):
            return [arg.riak_js_expr() for arg in self.args]

    class BoolAnd(BoolOperator):
        riak_js_oper = '&&'
        reprsymbol = '&'
        def __bool__(self):
            for a in self.args:
                if isinstance(a,str):
                    v = eval(a)
                else:
                    v = bool(a)
                if not v:
                    return False
            return True

    class BoolOr(BoolOperator):
        riak_js_oper = '||'
        reprsymbol = '|'
        def __bool__(self):
            for a in self.args:
                if isinstance(a,str):
                    v = eval(a)
                else:
                    v = bool(a)
                if v:
                    return True
            return False

    class BinaryComparisonOperator(BoolOperator):
        def riak_js_expr(self):
            if self.reprsymbol == 'LIKE':
                arg0 = self.args[0]
                arg1 = self.args[1]
                return '%s.filter(function(s) { return !s.match(/%s/g) }).length == 0' % (arg0.riak_js_expr(), regex_from_like(arg1.get_single_value_or_raise()),)
            elif self.reprsymbol == 'IN':
                return '(%s != undefined) && %s.filter(function(s) { return (%s.indexOf(s) < 0) }).length == 0' % (self.args[0].riak_js_expr(),
                                                                                                                   self.args[0].riak_js_expr(),
                                                                                                                   self.args[1].riak_js_expr())
            elif len(self.reprsymbol) == 2 and self.reprsymbol[0] == 'IS' and self.reprsymbol[1] == 'NOT':
                if isinstance(self.args[1], SqlParser.Null):
                    return '%s != undefined' % (self.args[0].riak_js_expr(),)
            else:
                return super(SqlParser.BinaryComparisonOperator, self).riak_js_expr()

        @property
        def riak_js_oper(self):
            if self.reprsymbol in ('<', '>', '==', '<=', '>='):
                return self.reprsymbol
            elif self.reprsymbol == '=':
                return '=='
            elif self.reprsymbol in ('!=', '<>'):
                return '!='
            elif self.reprsymbol == 'IS':
                return 'is'
            elif self.reprsymbol == 'IN':
                raise "Oh, shit"
            elif self.reprsymbol == 'LIKE':
                raise "Oh, shit"

        def __init__(self, t):
            self.reprsymbol = t[0][1]
            self.args = t[0][0::2]
            if type(self.args[0]) == type(self.args[1]):
                raise ParseException("We don't allow comparing two identifiers nor two literals!")

        def __bool__(self):
            arg0 = self.args[0]
            arg1 = self.args[1]
            if not arg0.comparable() or not arg1.comparable():
                return False

            if self.reprsymbol == '<':
                return arg0 < arg1
            elif self.reprsymbol == '>':
                return arg0 > arg1
            elif self.reprsymbol in ('=', '=='):
                return arg0 == arg1
            elif self.reprsymbol in ('!=', '<>'):
                return not (arg0 == arg1)
            elif self.reprsymbol == '<=':
                return arg0 <= arg1
            elif self.reprsymbol == '>=':
                return arg0 >= arg1
            elif self.reprsymbol == 'IN':
                return arg0 in arg1
            elif len(self.reprsymbol) == 2 and self.reprsymbol[0] == 'IS' and self.reprsymbol[1] == 'NOT':
                if isinstance(arg1, SqlParser.Null):
                    return arg0.value is not None
            elif self.reprsymbol == 'LIKE':
                if arg0:
                    regex = re.compile(regex_from_like(arg1))
                    return bool(regex.match(arg0.get_single_value_or_raise()))
                else:
                    return False

    class BetweenXAndY(BoolOperator):
        def __init__(self, t):
            self.reprsymbol = t[0][1]
            self.args = t[0][0::2]

        def __bool__(self):
            return self.args[0] > min(*self.args[1:]) and self.args[0] < max(*self.args[1:])

        def riak_js_expr(self):
            minval = min(*self.args[1:])
            maxval = max(*self.args[1:])
            return ("(%s < %s) && (%s < %s)" % (minval.riak_js_expr(), self.args[0].riak_js_expr(), self.args[0].riak_js_expr(), maxval.riak_js_expr()))

    class BoolNot(BoolOperator):
        def __init__(self,t):
            self.args = t[0][1:]

        def riak_js_expr(self):
            return '!(%s)' % (self.args[0].riak_js_expr())

        def __str__(self):
            return "~" + str(self.args[0])

        def __bool__(self):
            return not bool(self.args[0])

        def comparable(self):
            return True

    def __init__(self):
        self.select_stmt = Forward().setName("select statement")
        self.itemName = MatchFirst(Keyword("itemName()")).setParseAction(self.ItemName)
        self.count = MatchFirst(Keyword("count(*)")).setParseAction(self.Count)
        self.identifier = ((~keyword + Word(alphas, alphanums+"_")) | QuotedString("`"))
        self.column_name = (self.itemName | self.identifier.copy())
        self.table_name = self.identifier.copy()
        self.function_name = self.identifier.copy()

        # expression
        self.expr = Forward().setName("expression")

        self.integer = Regex(r"[+-]?\d+")
        self.string_literal = QuotedString("'")
        self.literal_value = self.string_literal


        self.expr_term = (
            self.itemName |
            self.function_name + LPAR + Optional(delimitedList(self.expr)) + RPAR |
            self.literal_value.setParseAction(self.Literal) |
            NULL.setParseAction(self.Null) |
            self.identifier.setParseAction(self.Identifier) |
            (EVERY + LPAR + self.identifier.setParseAction(self.Identifier) + RPAR).setParseAction(self.EveryIdentifier) |
            (LPAR + Optional(delimitedList(self.literal_value.setParseAction(self.Literal))) + RPAR).setParseAction(self.ValueList)
            )

        self.expr << (operatorPrecedence(self.expr_term,
            [
            (NOT, UNARY, opAssoc.RIGHT, self.BoolNot),
            (oneOf('< <= > >='), BINARY, opAssoc.LEFT, self.BinaryComparisonOperator),
            (oneOf('= == != <>') | Group(IS + NOT) | IS | IN | LIKE, BINARY, opAssoc.LEFT, self.BinaryComparisonOperator),
            ((BETWEEN,AND), TERNARY, opAssoc.LEFT, self.BetweenXAndY),
            (OR, BINARY, opAssoc.LEFT, self.BoolOr),
            (AND, BINARY, opAssoc.LEFT, self.BoolAnd),
            (INTERSECTION, BINARY, opAssoc.LEFT, self.Intersection),
            ])).setParseAction(self.dont_allow_non_comparing_terms)

        self.ordering_term = (self.itemName | self.identifier) + Optional(ASC | DESC)

        self.single_source = self.table_name("table")

        self.result_column = Group("*" | self.count | delimitedList(self.column_name))("columns")
        self.select_core = (SELECT + self.result_column + FROM + self.single_source + Optional(WHERE + self.expr("where_expr")))

        self.select_stmt << (self.select_core +
                        Optional(ORDER + BY + Group(delimitedList(self.ordering_term))).setParseAction(self.OrderByTerms)("order_by_terms") +
                        Optional(LIMIT + self.integer)("limit_terms"))

    def dont_allow_non_comparing_terms(self, s, loc, toks):
        if isinstance(toks[0], self.BoolOperand):
            raise ParseException("Failed")
        return toks

    def parse(self, s):
        try:
            ret = self.select_stmt.parseString(s, parseAll=True)
            ret.columns = ret.columns.asList()
            return ret
        except ParseException:
            raise exceptions.InvalidQueryExpression()
