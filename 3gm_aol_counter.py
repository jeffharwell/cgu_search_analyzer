import csv
import re
import time
import pyodbc
import datetime
import xml.parsers.expat

from xml.dom import minidom
from urllib import urlopen
from nltk.tokenize import *

"""
  Import the cql drivers for Cassandra
"""
from glob import glob
import sys
import os


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
        
        

class Expander():
    """Given a file that contains queries on separate lines,
    this class finds terms that may be used to expand those queries
    from the Ngram Corpus (our database) and Google Search.

    Additionally, the UMLS is queried to get_ambiguity and get_specificity
    of each term

    Skip to main() to edit input and output filenames and directories.
    """
    def __init__(self,filename):
        self.start=datetime.datetime.now()
        opened=open(filename,'rb')
        reader=csv.reader(opened)
        self.terms=[]
        self.total_count=0
        self.pos = PartOfSpeech()
        for term in reader:
            self.terms.append(term[0])
        
    def top_index_expand(self,term,count):
	## Use the cassandra database to get the Google Corpus terms
	## as opposed to the SQL server database
	return self.top_index_expand_count(term,count)

    def top_index_expand_count(self,term,count):
        """ Method which expands the query using the Google Web Corpus parsed and stored
            in a Cassandra database
        """
        terms = term.split()
        ## We are only set up to do expansions on 3-gram searches ... so check for this
        if not len(terms) == 3:
            raise NotThreeGram()
    	return True

def main():
    ## The first argument is the directory with the data file. This allows the program
    ## to be run easily on multiple data sets simultaneously.
    datadirectory = sys.argv[1]
    print "Data Directory %s" % datadirectory
    data=[]
    filename="./%s/sampled3.txt" % datadirectory # File to be read
    expander=Expander(filename)
    c=0
    submitted_searches = 0
    total_searches = 0
    l=len(expander.terms)
    for term in expander.terms: 
        c+=1
        ##print "%d / %d" % (c,l)
        try:
            ##print "Top Index Expand"
            total_searches += 1
            d=expander.top_index_expand(term.lower(),10)
            submitted_searches += 1
            ##print "Got %s" % d
        except NotThreeGram:
            ## expander will throw a NotThreeGram exception if it is asked to
            ## parse a search that does not contain exactly three grams.
            ## Catch that error, note it, then continue.
            pass
            #print "%s doesn't appear to be a three gram search" % term
        except:
            ## Print out the problem term to aid troubleshooting
            print "Unable to Cassandra expand term %s" % term
            ## Hmm, something unexpected happened. Re-raised the error and kill the
            ## program so that we can find and fix the problem.
            raise
    print "Would Submit: %s searches of %s total searches" % (submitted_searches, total_searches)

if __name__=="__main__":
    main()
