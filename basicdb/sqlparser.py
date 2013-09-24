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
                      alphas, alphanums, Regex, QuotedString, oneOf, \
                      Word, Optional, delimitedList, operatorPrecedence, opAssoc, \
                      Group, ParseException

from basicdb import exceptions

ParserElement.enablePackrat()

def lookup(id):
    return

def lookup_every(id):
    return

def regex_from_like(s):
    return str(s).replace('*', '\*').replace('_', '.').replace('%', '.*')

class BoolOperand(object):
    def __lt__(self, other):
        if isinstance(other, BoolOperand):
            return all(cmp(v1, v2) < 0 for v1, v2 in itertools.product(self.value, other.value))
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, BoolOperand):
            return all(cmp(v1, v2) <= 0 for v1, v2 in itertools.product(self.value, other.value))
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, BoolOperand):
            return all(cmp(v1, v2) > 0 for v1, v2 in itertools.product(self.value, other.value))
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, BoolOperand):
            return all(cmp(v1, v2) >= 0 for v1, v2 in itertools.product(self.value, other.value))
        return NotImplemented

    def __eq__(self, other):
        if isinstance(other, BoolOperand):
            return all(cmp(v1, v2) == 0 for v1, v2 in itertools.product(self.value, other.value))
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, BoolOperand):
            return all(cmp(v1, v2) != 0 for v1, v2 in itertools.product(self.value, other.value))
        return NotImplemented

    def comparable(self):
        return True

    def get_single_value_or_raise(self):
        if len(self.value) > 1:
            raise Exception("Asked for singular value, but multiple values were available")
        else:
            return self.value.copy().pop()

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

class Literal(BoolOperand):
    def __init__(self, t):
        self.value = set([t[0]])

    def riak_js_expr(self):
        return repr(self.get_single_value_or_raise())

    def __str__(self):
        return str(self.get_single_value_or_raise())

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

class EveryIdentifier(Identifier):
    def __init__(self, t):
        self.identifier = t[1]

    @property
    def value(self):
        return lookup_every(self.identifier.reference)

    def riak_js_expr(self):
        return 'val[%r]' % (self.identifier.reference,)

class BoolOperator(object):
    def __init__(self, t):
        self.args = t[0][0::2]
        if not all(isinstance(arg, (BinaryComparisonOperator, BoolOperator)) for arg in self.args):
            raise ParseException("Invalid query")

    def __nonzero__(self):
        return self.__bool__()

    def riak_js_expr(self):
        ident = filter(lambda x:isinstance(x, Identifier), self.args)
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
            return attrs.get(key, None)
        def _lookup_every(key):
            return raw_attrs.get(key, None)
        saved_lookup, saved_lookup_every = lookup, lookup_every
        try:
            lookup, lookup_every = _lookup, _lookup_every
            return self.__bool__()
        finally:
            lookup, lookup_every = saved_lookup, saved_lookup_every


class Intersection(BoolOperator):
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
            print self.args[0]
            return '(%s != undefined) && %s.filter(function(s) { return (%s.indexOf(s) < 0) }).length == 0' % (self.args[0].riak_js_expr(),
                                                                                                               self.args[0].riak_js_expr(),
                                                                                                               self.args[1].riak_js_expr())
        else:
            return super(BinaryComparisonOperator, self).riak_js_expr()

    @property
    def riak_js_oper(self):
        if self.reprsymbol in ('<', '>', '==', '<=', '>='):
            return self.reprsymbol
        elif self.reprsymbol == '=':
            return '=='
        elif self.reprsymbol in ('!=', '<>'):
            return '!='
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

class BoolNot(BoolOperand):
    def __init__(self,t):
        self.arg = t[0][1]

    def __str__(self):
        return "~" + str(self.arg)

    def __bool__(self):
        return not bool(self.arg)

LPAR,RPAR,COMMA = map(Suppress,"(),")
select_stmt = Forward().setName("select statement")

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

identifier = ((~keyword + Word(alphas, alphanums+"_")) | QuotedString("`"))
column_name = identifier.copy()
table_name = identifier.copy()
function_name = identifier.copy()

# expression
expr = Forward().setName("expression")

integer = Regex(r"[+-]?\d+")
string_literal = QuotedString("'")
literal_value = ( string_literal | NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )

expr_term = (
    function_name + LPAR + Optional(delimitedList(expr)) + RPAR |
    literal_value.setParseAction(Literal) |
    identifier.setParseAction(Identifier) |
    (EVERY + LPAR + identifier.setParseAction(Identifier) + RPAR).setParseAction(EveryIdentifier) |
    (LPAR + Optional(delimitedList(literal_value.setParseAction(Literal))) + RPAR).setParseAction(ValueList)
    )

def dont_allow_non_comparing_terms(s, loc, toks):
    if isinstance(toks[0], BoolOperand):
        raise ParseException("Failed")
    return toks

UNARY,BINARY,TERNARY=1,2,3
expr << (operatorPrecedence(expr_term,
    [
    (NOT, UNARY, opAssoc.RIGHT, BoolNot),
    (oneOf('< <= > >='), BINARY, opAssoc.LEFT, BinaryComparisonOperator),
    (oneOf('= == != <>') | IS | IN | LIKE, BINARY, opAssoc.LEFT, BinaryComparisonOperator),
    ((BETWEEN,AND), TERNARY, opAssoc.LEFT, BetweenXAndY),
    (OR, BINARY, opAssoc.LEFT, BoolOr),
    (AND, BINARY, opAssoc.LEFT, BoolAnd),
    (INTERSECTION, BINARY, opAssoc.LEFT, Intersection),
    ])).setParseAction(dont_allow_non_comparing_terms)

ordering_term = expr + Optional(ASC | DESC)

single_source = table_name("table")

result_column = Group("*" | delimitedList(column_name))("columns")
select_core = (SELECT + result_column + FROM + single_source + Optional(WHERE + expr("where_expr")))

select_stmt << (select_core +
                Optional(ORDER + BY + Group(delimitedList(ordering_term))("order_by_terms")) +
                Optional(LIMIT + integer))

def parse(s):
    try:
        ret = select_stmt.parseString(s, parseAll=True)
        ret.columns = ret.columns.asList()
#        print ret.dump()
        return ret
    except ParseException:
        raise exceptions.InvalidQueryExpression()

if __name__ == "__main__":
    tests = """\
    select * from mydomain where Title = 'The Right Stuff'
    select * from mydomain where Year > '1985'
    select * from mydomain where Rating like '****%'
    select * from mydomain where Pages < '00320'
    select * from mydomain where Year > '1975' and Year < '2008'
    select * from mydomain where Year between '1975' and '2008'
    select * from mydomain where Rating = '***' or Rating = '*****'
    select * from mydomain where Rating = '4 stars' or Rating = '****'
    select * from mydomain where Keyword = 'Book' and Keyword = 'Hardcover'
    select * from mydomain where every(keyword) in ('Book', 'Paperback')
    select * from mydomain where Rating = '****'
    select * from mydomain where every(Rating) = '****'
    select * from mydomain where Keyword = 'Book' intersection Keyword = 'Hardcover'
    select * from mydomain where (Year > '1950' and Year < '1960') or Year like '193%' or Year = '2007'
    select * from mydomain where Year < '1980' order by Year asc
    select * from mydomain where Year < '1980' order by Year
    select * from mydomain where Year = '2007' intersection Author is not null order by Author desc
    select * from mydomain order by Year asc
    select * from mydomain where Year < '1980' order by Year limit 2
    select itemName() from mydomain where itemName() like 'B000%' order by itemName()
    select * from xyzzy where z > '100'
    select * from xyzzy where z > 100
    select * from xyzzy where z > 100 order by zz
    select * from xyzzy
    """.splitlines()
    tests = """\
    SELECT * FROM foobar WHERE colour == 'blue' and size > '5' or shape = 'triangular'
    """.splitlines()
    for t in tests:
        if t.strip() == '':
            continue
        print t
        try:
            tree = parse(t)
            import pdb;pdb.set_trace()
            print t.dump()
        except ParseException, pe:
            print 'Parsing %r failed' % (t,)
            print pe.msg
            print pe.line
            print ' '*(pe.col-1) + '^'
            raise
        print
