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

def regex_from_like(s):
    return s.replace('*', '\*').replace('_', '.').replace('%', '.*')

class BoolOperand(object):
    pass

class ValueList(BoolOperand):
    def __init__(self, t):
        self.value = self
        self._value = t[:]

    def __str__(self):
        return str(self._value)

    def __contains__(self, item):
        return any(item == val.value for val in self._value)

class Literal(BoolOperand):
    def __init__(self, t):
        self.value = t[0]

    def riak_js_expr(self):
        return repr(self.value)

    def __str__(self):
        return str(self.value)

    def __eq__(self, other):
        return other == self.value

class Identifier(BoolOperand):
    def __init__(self, t):
        self.reference = t[0]

    @property
    def value(self):
        return lookup(self.reference)

    def riak_js_expr(self):
        return 'vals[%r]' % (self.reference,)

class BoolOperator(object):
    def __init__(self, t):
        self.args = t[0][0::2]
        if not all(isinstance(arg, (BinaryComparisonOperator, BoolOperator)) for arg in self.args):
            raise ParseException("Invalid query")

    def __nonzero__(self):
        return self.__bool__()

    def riak_js_expr(self):
        sep = " %s " % self.riak_js_oper
        return "(" + sep.join(map(lambda a:a.riak_js_expr(),self.args)) + ")"

    def __str__(self):
        sep = " %s " % self.reprsymbol
        return "(" + sep.join(map(str,self.args)) + ")"

    def match(self, item_name, attrs):
        def set_iter(key, vals):
            return ((key, v) for v in vals)

        return any((self._match(item_name, dict(values)) for values in itertools.product(*[set_iter(key, attrs[key]) for key in attrs.keys()])))

    def _match(self, item_name, attrs):
        global lookup
        def _lookup(key):
            return attrs.get(key, None)
        saved_lookup = lookup
        try:
            lookup = _lookup
            return self.__bool__()
        finally:
            lookup = saved_lookup 


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
            arg1 = self.args[1].value
            return '%s.match(/%s/g)' % (arg0.riak_js_expr(), regex_from_like(arg1),)
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
        arg0 = self.args[0].value
        arg1 = self.args[1].value
        if None in (arg0, arg1):
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
                return regex.match(arg0)
            else:
                return False

class BetweenXAndY(BoolOperator):
    def __init__(self, t):
        self.reprsymbol = t[0][1]
        self.args = t[0][0::2]

    def __bool__(self):
        return self.args[0] > min(*self.args[1:]) and self.args[0] < max(*self.args[1:])


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
 NOT, SELECT, DISTINCT, FROM, WHERE, BY, ORDER, BY, LIMIT) = map(CaselessKeyword,
"""UNION, ALL, AND, OR, INTERSECT, INTERSECTION, EXCEPT, COLLATE, ASC, DESC, ON,
 NOT, SELECT, DISTINCT, FROM, WHERE, BY, ORDER, BY, LIMIT""".replace(",","").split())

(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE,
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE,
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE,
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())

keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON,
 NOT, SELECT, DISTINCT, FROM, WHERE, BY,
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
    (INTERSECTION, BINARY, opAssoc.LEFT),
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
    select * from xyzzy where z IN ('10', '11')
    select foo, bar from xyzzy where z IN ('10', '11')
    """.splitlines()
    for t in tests:
        if t.strip() == '':
            continue
        print t
        try:
            print parse(t).dump()
        except ParseException, pe:
            print 'Parsing %r failed' % (t,)
            print pe.msg
            print pe.line
            print ' '*(pe.col-1) + '^'
            raise
        print
