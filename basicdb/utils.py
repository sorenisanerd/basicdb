import re

PUT_ATTRIBUTE_QUERY_REGEX = re.compile(r'Attribute\.(\d+)\.(Name|Value|Replace)')
DELETE_QUERY_ARG_REGEX = re.compile(r'Attribute\.(\d+)\.(Name|Value)')
EXPECTED_QUERY_ARG_REGEX = re.compile(r'Expected\.(\d+)\.(Name|Value|Exists)')

def extract_numbered_args(regex, req):
    attrs = {}
    for (k, v) in req._params.iteritems():
        match = regex.match(k)
        if not match:
            continue
    
        idx, elem = match.groups()
        if idx not in attrs:
            attrs[idx] = {}
        attrs[idx][elem] = v
    return attrs
     
def extract_additions_and_replacements_from_query_params(req): 
    args = extract_numbered_args(PUT_ATTRIBUTE_QUERY_REGEX, req)
    
    additions = {}
    replacements = {}
    for idx, data in args.iteritems():
        if 'Name' in args[idx] and 'Value' in args[idx]:
            name = args[idx]['Name']
            value = args[idx]['Value']
            if 'Replace' in args[idx] and args[idx]['Replace'] == 'true':
                if name not in replacements:
                    replacements[name] = set()
                replacements[name].add(value)
            else:
                if name not in additions:
                    additions[name] = set()
                additions[name].add(value)
    return additions, replacements

def extract_expectations_from_query_params(req):
    args = extract_numbered_args(EXPECTED_QUERY_ARG_REGEX, req)

    expectations = set()
    for data in args.values():
        if 'Name' in data:
            if  'Value' in data:
                expected_value = data['Value']
            elif  'Exists' in data:
                val = data['Exists']
                expected_value = not (val == 'false')
            expectations.add((data['Name'], expected_value))
    
    return expectations

def extract_deletions_from_query_params(req):
    args = extract_numbered_args(DELETE_QUERY_ARG_REGEX, req)
    
    deletions = {}
    for data in args.values():
        if 'Name' not in data:
            continue
    
        attr_name = data['Name']
    
        if attr_name not in deletions:
            deletions[attr_name] = set()
    
        if 'Value' in data:
            deletions[attr_name].add(data['Value'])
        else:
            import basicdb
            deletions[attr_name].add(basicdb.AllAttributes)
    return deletions
