import csv
import re
import time
import pyodbc
import datetime
import xml.parsers.expat
import random
from nltk.corpus import wordnet as wn

from xml.dom import minidom
from urllib import urlopen
from nltk.tokenize import *

"""
  Import the cql drivers for Cassandra
"""
from glob import glob
import sys
import os

CQL_LIB_PREFIX = 'cql-internal-only-'
ziplibdir = os.path.join(os.path.dirname(__file__), '.', 'lib')
ziplibdir = os.path.join('c:\\','Users','jharwell','apache-cassandra-2.0.1','lib')
print os.path.dirname(__file__)
zips = glob(os.path.join(ziplibdir, CQL_LIB_PREFIX + '*.zip'))
print zips
print zips[0]
ver = os.path.splitext(os.path.basename(zips[0]))[0][len(CQL_LIB_PREFIX):]
print ziplibdir
print ver
sys.path.insert(0, os.path.join(zips[0], 'cql-' + ver))
import cql

"""
  Define a few custom error classes
"""

class NotThreeGram(Exception):
    """ A custom Exception class that is thrown when the parser encounters a search
        query that is not 3 grams long
    """
    pass
class GoogleExpansionFailure(Exception):
    """ A custom exception class that is thrown when the Googl Search Query Expansion
        fails.
    """
    pass
class UnknownExpansionType(Exception):
    """ A custom exception class thrown when you try to write an expansion to Cassandra
        that is not a corpus or google search type
    """
    pass

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
        
def get_random_number_list(min, max, num_samples):
    l = dict()
    for i in range(1,num_samples+1):
        attempt = random.randint(min,max)
        while 1:
            if attempt in l:
                attempt = random.randint(min,max)
            else:
                l[attempt] = 1
                break

    return l


def print_list(l, filename):
    opened=open(filename,'wb')
    writer=csv.writer(opened)
    for r in l:
        writer.writerow(r)
    opened.close()

def main():
    num_grams = 2

    filename = ""
    corpus_sample_write_file = ""
    search_sample_write_file = ""
    corpus_rnum_list = []
    search_rnum_list = []

    if (num_grams == 3):
        filename = "sampled_text_3gm_postfilter.csv"
        corpus_sample_write_file = 'random_corpus_sample_3gm.csv'
        search_sample_write_file = 'random_search_sample_3gm.csv'

        ## Ok, create the random number lists
        corpus_rnum_list = get_random_number_list(1,151746,3170)
        search_rnum_list = get_random_number_list(1,482068,3170)
    elif (num_grams == 2):
        filename = "sampled_text_2gm_postfilter.csv"
        corpus_sample_write_file = 'random_corpus_sample_2gm.csv'
        search_sample_write_file = 'random_search_sample_2gm.csv'

        ## Ok, create the random number lists
        corpus_rnum_list = get_random_number_list(1,438148,7478)
        search_rnum_list = get_random_number_list(1,1057297,7478)

    rnum_list = dict()
    rnum_list['s'] = search_rnum_list
    rnum_list['c'] = corpus_rnum_list
    ##print corpus_rnum_list
    print "Corpus rnum length %s Search rnum length %s" % (len(corpus_rnum_list), len(search_rnum_list))

    random_sample = dict()
    random_sample['c'] = []
    random_sample['s'] = []

    sum_counter = dict()
    sum_counter['c'] = dict()
    sum_counter['s'] = dict()
    sum_counter['c']['specificity'] = [0.0,0]
    sum_counter['c']['ambiguity'] = [0.0,0]
    sum_counter['s']['specificity'] = [0.0,0]
    sum_counter['s']['ambiguity'] = [0.0,0]

    totals = dict()
    totals['c'] = {'totals':0, 'misses':0}
    totals['s'] = {'totals':0, 'misses':0}
    totals_hit = dict()
    totals_hit['c'] = 0
    totals_hit['s'] = 0

    pos = PartOfSpeech()

    with open(filename, 'rb') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in csvreader:
            p = row[4]
            if p == "" or p == "numeric":
                totals[row[1]]['totals'] += 1
                if row[6] != "":
                    ## Wordnet Hit
                    totals_hit[row[1]] += 1
                    if (totals_hit[row[1]] in rnum_list[row[1]]):
                        random_sample[row[1]].append(list(row))

                        sum_counter[row[1]]['specificity'][0] += float(row[6])
                        sum_counter[row[1]]['specificity'][1] += 1
                        sum_counter[row[1]]['ambiguity'][0] += float(row[7])
                        sum_counter[row[1]]['ambiguity'][1] += 1
                else:
                    ## No Wordnet Results
                    totals[row[1]]['misses'] += 1
            

    corpus_specificity = sum_counter['c']['specificity'][0] / sum_counter['c']['specificity'][1]
    corpus_ambiguity = sum_counter['c']['ambiguity'][0] / sum_counter['c']['ambiguity'][1]
    search_specificity = sum_counter['s']['specificity'][0] / sum_counter['s']['specificity'][1]
    search_ambiguity = sum_counter['s']['ambiguity'][0] / sum_counter['s']['ambiguity'][1]
    
    print "Corpus: Specificity %s, Ambiguity %s" % (corpus_specificity, corpus_ambiguity)
    print "Search: Specificity %s, Ambiguity %s" % (search_specificity, search_ambiguity)
    print "Corpus: Total %s, Misses %s, Percent Misses %s" % (totals['c']['totals'], totals['c']['misses'], (float(totals['c']['misses'])/totals['c']['totals'])*100)
    print "Search: Total %s, Misses %s Percent Misses %s" % (totals['s']['totals'], totals['s']['misses'], (float(totals['s']['misses'])/totals['s']['totals'])*100)
    print "Corpus Wordnet Hits %s" % totals_hit['c']
    print "Search Wordnet Hits %s" % totals_hit['s']

    print "Corpus Random Samples = %s" % len(random_sample['c'])
    print "Search Random Samples = %s" % len(random_sample['s'])

    print_list(random_sample['c'],corpus_sample_write_file)
    print_list(random_sample['s'],search_sample_write_file)

if __name__=="__main__":
    main()
