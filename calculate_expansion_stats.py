#!/usr/bin/python2.7

import csv
import urllib2
import urllib
import simplejson
import sys
import time
import re
from scipy import stats
import numpy
from HTMLParser import HTMLParser
from nltk.tokenize import *
from nltk.corpus import wordnet as wn
import matplotlib.pyplot as plt

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


file_name = "sampled_text_2gm.csv"
expansion_types = ['s','c']
pos_object = PartOfSpeech()
specificity_list = {'s':[], 'c':[]}
ambiguity_list = {'s':[], 'c':[]}
queries_analyzed = {'s':0,'c':0,'total':0,'pre-filter':{'s':0,'c':0,'total':0},'raw':{'s':0,'c':0,'total':0}}
with open(file_name, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        expansion_type = row[1]
        expansion_term = row[0]
        specificity = row[6]
        ambiguity = row[7] 

        # Counters
        queries_analyzed['raw']['total'] += 1
        queries_analyzed['raw'][expansion_type] += 1
        if specificity != '' and ambiguity != '':
            ## This expansion had a hit in Wordnet 3.0
            queries_analyzed['pre-filter']['total'] += 1
            queries_analyzed['pre-filter'][expansion_type] += 1
            ## We are skipping all prepositions, conjunctions, etc
            if not skip_word_to_lower(pos_object, expansion_term) and specificity != '':
                queries_analyzed[expansion_type] += 1
                queries_analyzed['total'] += 1
                specificity_list[expansion_type].append(float(specificity))
                ambiguity_list[expansion_type].append(float(ambiguity))

print queries_analyzed
for et in expansion_types:
    print "\n---\n-- Type %s\n---" % et
    print "  Standard Deviation: "
    print "  Ambiguity: Length %s, Mean %s, Std %s" % (len(ambiguity_list[et]), numpy.mean(ambiguity_list[et]),numpy.std(ambiguity_list[et]))
    print "  Specificity: Length %s, Mean %s, Std %s" % (len(specificity_list[et]), numpy.mean(specificity_list[et]),numpy.std(specificity_list[et]))
    """
    print numpy.histogram(specificity_list)
    ##n, bins, patches = plt.hist(ambiguity_list, 50, normed=1, facecolor='g', alpha=0.75)
    n, bins, patches = plt.hist(ambiguity_list)
    plt.grid(True)
    plt.show()
    time.sleep(30)
    """

print "\n  Welch's T-Test: For Ambiguity"
print stats.ttest_ind(ambiguity_list['s'], ambiguity_list['c'],equal_var=False)
print "  Welch's T-Test: For Specificity"
print stats.ttest_ind(specificity_list['s'], specificity_list['c'],equal_var=False)

