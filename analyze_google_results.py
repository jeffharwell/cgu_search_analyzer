#!/usr/bin/python2.7

import csv
import urllib2
import urllib
import simplejson
import sys
import time
import re
import os
from scipy import stats
from HTMLParser import HTMLParser
from nltk.tokenize import *
from nltk.corpus import wordnet as wn

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

class PartOfSpeech():
    """Returns a Part of Speach for a given term"""
    def __init__(self):
        self.possessive = re.compile(r"'s")
        self.punctuation = re.compile(r"^[\.\^\*\+\?\{\}\[\]\(\)\\\|\-,;:%@!&/_]+$")
        self.numeric = re.compile(r"\d+")

        ## http://grammar.yourdictionary.com/parts-of-speech/conjunctions/conjunctions.html
        c1 = ['and','but','or','nor','for','yet','so','after','although','as','because','before','even','if','inasmuch','in']
        c2 = ['just','lest','now','once','provided','rather','since','so','supposing','than','that','though']
        c3 = ['til','unless','until','when','whenever','where','whereas','where if','wherever','whether']
        c4 = ['which','while','who','whoever','why']
        c5 = ['both','either','neither','scarcely','rather']
        self.conjunction = c1 + c2 + c3 + c4 + c5
        
        ## http://en.wikipedia.org/wiki/List_of_English_prepositions
        p1 = ['a','abaft','aboard','about','above','absent','across','afore','after','against','along']
        p2 = ['alongside','amid','amidst','among','amongst','an','anenst','apropos','apud','around']
        p3 = ['as','aside','astride','at','athwart','atop','barring','before']
        p4 = ['behind','below','beneath','beside','besides','between','beyond','but','by']
        p5 = ['circa','concerning','despite','down','during','except','excluding','failing']
        p6 = ['following','for','forenenst','from','given','in','including','inside','into','lest','like']
        p7 = ['mid','midst','minus','modulo','near','nigh','next','notwithstanding','of','off','on']
        p8 = ['onto','opposite','out','outside','over','pace','past','per','plus','pro','qua']
        p9 = ['regarding','round','sans','save','since','than','through','thru','throughout']
        p10 = ['till','times','to','toward','towards','under','underneath','unlike','until','unto']
        p11 = ['up','upon','versus','via','vice','with','within','without','worth']
        self.preposition = p1+p2+p3+p4+p5+p6+p7+p8+p9+p10+p11
    def getPOS(self,term):
        if (self.possessive.match(term)):
            return "possessive"
        elif (self.punctuation.match(term)):
            return "punctuation"
        elif (self.numeric.match(term)):
            return "numeric"
        elif (term in self.preposition):
            return "preposition"
        elif (term in self.conjunction):
            return "conjunction"
        else:
            return ""

def skip_word_to_lower(pos_object, word):
    word_pos = pos_object.getPOS(word.lower())
    if word_pos == "" or word_pos == "numeric":
        return False
    else:
        return True


def get_wordnet_specificity(word):
    """ Given a word as a string, return the specificity"""    
    try:
        syn_set=wn.synsets(word)               # Get the synsets for the word
        hyperns=syn_set[0].hypernym_paths()    # Get the possible hypernym paths
        specificity=len(hyperns[0])            # Specificity = number of items in path                         
    except IndexError:
        specificity=''                         # Word not found means missing value

    return specificity

def get_wordnet_ambiguity(word):
    """Given a word as a string, return the ambiguity"""
    try:
        syn_set=wn.synsets(word)
        ambiguity=len(syn_set)
        if ambiguity==0:                       # If no synsets, an empty list is returned
            ambiguity=''
    except Exception as e:
        print "Ambiguity error: %r" % e        # Didn't see any errors, but printing them just in case
        ambiguity=''

    return ambiguity

def string_specificity_and_ambiguity(s,pos_object):
    sum_specificity = 0
    terms_specific = 0
    sum_ambiguity = 0
    terms_ambiguous = 0
    token_list = word_tokenize(s)
    for word in token_list:
        if not skip_word_to_lower(pos_object, word):
            spec = get_wordnet_specificity(word)
            amb = get_wordnet_ambiguity(word)
            if spec != '':
                sum_specificity += spec
                terms_specific += 1
                #print word,
            if amb != '':
                sum_ambiguity += amb
                terms_ambiguous += 1
    avg_ambiguous = 0
    avg_specific = 0
    if (terms_ambiguous != 0):
        avg_ambiguous = sum_ambiguity / terms_ambiguous
    if (terms_specific != 0):
        avg_specific = sum_specificity / terms_specific
    #print "Ambiguity: %s, %s, %s" % (sum_ambiguity, terms_ambiguous, avg_ambiguous)
    #print "Specificity: %s, %s, %s" % ( sum_specificity, terms_specific, avg_specific)
    #print "END TOKEN"
    return {'specificity':sum_specificity, 'terms_specific':terms_specific, 'ambiguity':sum_ambiguity, 'terms_ambiguous':terms_ambiguous}


#base_dir = "s_google_search_json"
base_dir = {'s':"s_google_search_json_2gm",'c':"c_google_search_json_2gm"}
expansion_types = ['s','c']

file_list = dict()
for expansion_type in expansion_types:
    ## If python supported currying this would be a bit more
    ## straightforward
    def is_valid_file(var):
        if re.match(r'^%s_[0-9]+' % expansion_type, var):
            return True
        else:
            return False

    c = os.listdir(base_dir[expansion_type])
    fn_list = filter(is_valid_file, c)
    file_list[expansion_type] = fn_list
    print "Will analyze %s expansions for type '%s'" % (len(file_list[expansion_type]), expansion_type)

pos_object = PartOfSpeech()
corpus_samples = []
search_samples = []
samples = {}
samples['c'] = corpus_samples
samples['s'] = search_samples

global_unique_domains = {'s':dict(), 'c':dict()}
global_unique_counts = {'s':[],'c':[]}

for expansion_type in expansion_types:

    wordnet_totals = {'specificity':0, 'terms_specific':0, 'ambiguity':0, 'terms_ambiguous':0}
    domain_totals = {'domains_returned':0, 'unique_to_query':0}
    queries_analyzed = 0

    for file_name in file_list[expansion_type]:
        #print "Working on Query %s" % query_number
        to_read = "%s/%s" % (base_dir[expansion_type], file_name)
        queries_analyzed += 1
       
        f = open(to_read, 'r')
        #print f.readline()
        sjr = simplejson.load(f)
        #print sjr
        f.close()
        
        #print "---\nTrying JSON"
        #print "Query: %s" % sjr['responseData']['cursor']['moreResultsUrl']
        domain_list = []
        unique_domain = []
        try:
            results = sjr['responseData']['results']
        except TypeError, e:
            print "Error getting results for file %s, error %s" % (file_name, e)
            sys.exit()

        for result in sjr['responseData']['results']:
            visableurl = result['visibleUrl'].encode('utf-8')
            url_list = visableurl.split('.')
            domain = "%s.%s" % (url_list[-2],url_list[-1])
            ## Looking at diversite of domains across all queries
            if not (domain in global_unique_domains[expansion_type]):
                global_unique_domains[expansion_type][domain] = 1
            else:
                global_unique_domains[expansion_type][domain] += 1

            if (domain not in unique_domain):
                ## Only unique domains here
                unique_domain.append(domain)
            ## Any domain here
            domain_list.append(domain)
            #print visableurl
            #print domain
            title = result['titleNoFormatting'].encode('utf-8')
            content = strip_tags(result['content'].encode('utf-8'))
            #print "Title: %s" % title
            #print "Content: %s" % content
            analysis = string_specificity_and_ambiguity("%s %s" % (title, content), pos_object)
            if (analysis['terms_specific'] != 0): ## i.e. we actually found something
                wordnet_totals['specificity'] += analysis['specificity']
                wordnet_totals['terms_specific'] += analysis['terms_specific']
            if (analysis['terms_ambiguous'] != 0): ## i.e., we actually found something
                wordnet_totals['ambiguity'] += analysis['ambiguity']
                wordnet_totals['terms_ambiguous'] += analysis['terms_ambiguous']
        domain_totals['domains_returned'] += len(domain_list)
        domain_totals['unique_to_query'] += len(unique_domain)
        global_unique_counts[expansion_type].append(len(unique_domain))
        
    ## Final Analysis
    print "Query Type: %s, Queries Analyzed: %s" % (expansion_type, queries_analyzed)
    print wordnet_totals
    print "Result Specificity: %s" % (float(wordnet_totals['specificity']) / wordnet_totals['terms_specific'])
    print "Result Ambiguity: %s" % (float(wordnet_totals['ambiguity']) / wordnet_totals['terms_ambiguous'])
    percent_unique = float(domain_totals['unique_to_query']) / domain_totals['domains_returned']
    print "Domain: %s Total, %s unique to query, %s percent unique" % (domain_totals['domains_returned'],domain_totals['unique_to_query'],percent_unique)



print "Global Unique Domains"
print "  Search: %s" % len(global_unique_domains['s'])
print "  Corpus: %s" % len(global_unique_domains['c'])

##global_unique_counts = {'s':[],'c':[]}
sud = global_unique_counts['s']
cud = global_unique_counts['c']
print "\nNumber of Unique Domains Per Search"
print "  Corpus:"
print "    Length: %s, Mean: %s, Stdev: %s" % (len(cud), numpy.mean(cud),numpy.std(cud))
print "  Search:"
print "    Length: %s, Mean: %s, Stdev: %s" % (len(sud), numpy.mean(sud),numpy.std(sud))
print "\n  Welch's T-Test:"
print stats.ttest_ind(cud, sud, equal_var=False)
print "\n Two-tailed T-Test:"
print stats.ttest_ind(cud, sud, equal_var=True)

## Effect Size - Cohen's D using Cohen's 1988 p. 44 Pool Standard
## Deviation Formula
## (http://www.polyu.edu.hk/mm/effectsizefaqs/effect_size_equations2.html)
sd_pooled = numpy.sqrt((numpy.std(cud)**2 + numpy.std(sud)**2)/2)
cohen_d = (numpy.mean(cud) - numpy.mean(sud))/sd_pooled

print "\n  Effect Size (Cohen's D)"
print "Cohen's D: %s" % cohen_d
print "Pooled Standard Deviation: %s" % sd_pooled

