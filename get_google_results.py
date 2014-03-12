#!/usr/bin/python2.7

"""
get_google_results.py
Author: Jeff Harwell
Version: 1.1

This program takes a set of terms, submits those to the Google search api,
and then writes the results JSON to a file.

Release History

Version 1.1
  Add the ability to just search the base, not including the expansion
  google_search(terms, file_to_write, True) will just search using
  the base term.
Version 1
  The first base release. Major improvements include the ability to start at 
  an offset in the file numbers and fall back and evertually retry when 
  hitting the quota limit.

"""

import csv
import urllib2
import urllib
import simplejson
import sys
import time
import os
import re
import random
from HTMLParser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def is_integer(var):
    try:
        int(var)
        return True
    except ValueError:
        return False


def find_continue_point(base_dir, expansion_type, offset):
    def is_valid_file(var):
        if re.match(r'^%s_[0-9]+' % expansion_type, var):
            return True
        else:
            return False

    c = os.listdir(base_dir)
    fn_list = filter(is_valid_file, c)
    if len(fn_list) == 0:
        return 0

    str_num_list = [x.split('_')[1] for x in c]
    num_list = [int(x) - offset for x in str_num_list] 
    return max(num_list)

def google_search(terms, file_to_write, just_base = False):

    query_string = ""
    if just_base:
        query_string = "%s" % (terms[0])
    else:
        query_string = "%s %s" % (terms[0],terms[1])

    query = urllib.urlencode({'q':query_string})
    url = ('https://ajax.googleapis.com/ajax/services/search/web'
   '?v=1.0&rsz=8&%s' % query)
    print url


    while 1:

        request = urllib2.Request(
                  url, None, {'Referer':'cisat.claremont.edu'})
        try:
            response = urllib2.urlopen(request)
            results = simplejson.load(response)

            if results['responseDetails'] == "Quota Exceeded.  Please see http://code.google.com/apis/websearch":
                print "Hit our quote, sleeping for a while"
                r = random.sample([60,120,180,240,300],1)
                time.sleep(300 + r[0])
            else:
                f = open(to_write, 'w')
                f.write(simplejson.dumps(results))
                f.close()
                break

        except urllib2.URLError, e:
            print "Could not open URL, error %s" % e
            print "Sleep and retry"
            time.sleep(45)


base_dir = "results"
query_number = 0
if len(sys.argv) == 1:
    print "Usage: get_google_results.py file_with_expansions expansion_type{c|s} file_number_offset(use zero if unsure) just_base{true|false}"
    sys.exit()
file_to_open = sys.argv[1]
expansion_type = sys.argv[2].strip()
offset = int(sys.argv[3].strip())
try: 
    expand_base = sys.argv[4].strip()
    base = True
except IndexError, e:
    print "You must specify true or false for just_base, to just search for the base query or the base query plus expansion"
    sys.exit()
if (expand_base == 'true'):
    base = True
elif expand_base == 'false':
    base = False
else:
    print "You must specify 'true' or 'false' for just_base when calling this program."
    sys.exit()

if expansion_type not in ['s','c']:
    raise RuntimeError, "Invalid expansion type %s" % expansion_type
if not file_to_open or file_to_open == "":
    raise RuntimeError, "No File passed to open %s" % file_to_open
if not offset >= 0:
    raise RuntimeError, "%s is not a valid offset" % offset

continue_point = find_continue_point(base_dir, expansion_type, offset)
print "Continuing from %s" % continue_point

do_only = []

with open(file_to_open, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        query_number += 1
        if query_number <= continue_point:
            continue
        if len(do_only) > 0 and not query_number in do_only:
            continue

        to_write = "./%s/%s_%s" % (base_dir, expansion_type, query_number+offset)
        google_search([row[5],row[0]],to_write,base)

        r = random.sample([5,10,15,20],1)
        time.sleep(25+r[0])

print "Done processing %s query expansions" % expansion_type
sys.exit()

