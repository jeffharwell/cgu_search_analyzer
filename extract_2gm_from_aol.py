#!/bin/python2
import re

gm_queries = {}

class QueryFilter:
    def __init__(self):
        self.start_www = re.compile('^www')
        self.end_domain_root = re.compile(r'(org|net|com|gov|mil)$')
        self.match_spaces = re.compile(r'\s{2,}')

    def sanitize(self, s):
        return self.match_spaces.sub(' ',s)
        
    def filtered(self, q):
        if self.start_www.match(q):
            return True
        if self.end_domain_root.search(q):
            return True
        return False

qf = QueryFilter()

f = open('all_queries.txt')
for line in f:
    query = qf.sanitize(line.strip())
    query_list = line.split(' ')
#    print "Length: %s" % len(query_list)
#    print line,
    if len(query_list) == 2:
        if query_list[0] != "www":
            if not query in gm_queries:
                if not qf.filtered(query):
                    gm_queries[query] = 1

f.close()

print "Extracted %s queries" % len(gm_queries)

f = open('2gram_queries.txt', 'w')
for k in gm_queries.keys():
    f.write("%s\n" % k)

f.close()

