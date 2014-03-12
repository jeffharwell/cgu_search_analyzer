import csv
import re
import time
import pyodbc
import datetime
import xml.parsers.expat
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
        
    def search_expand(self,term):
        """Uses minidom to parse XML returned by Google search.
        Returns suggested terms (Up to 10, I think)
        """
        print "Search Expanding %s" % term
        term_words=[]
        new_terms=[]
        url="http://clients1.google.com/complete/search?hl=en&output=toolbar&q="
        space="%27"
        pattern=re.compile('(\w*)\s*')
        matches=pattern.findall(term)
        count=0
        for found in matches:
            if len(found) > 0:
                term_words.append(found)
        if len(term_words)==3:
            for t in term_words:
                if count<2:
                    url=url+t+space
                    count+=1
                else:
                    url=url+t
            ## Content comes in with charset ISO-8859-1, need to
            ## encode that to unicode or the minidom parser will fail
            ## at the first non-ascii character

            ## First figure out what character set we were sent by Google
            req = urlopen(url)
            encoding=req.headers['content-type'].split('charset=')[-1]

            ## This does the encoding
            google_result = unicode(req.read(), encoding)

            ## Now Parse, note you must .encode() because minidom doesn't like
            ## the raw UTF-8 bit stream
            dom = minidom.parseString(google_result.encode('utf-8'))
            suggestions=dom.getElementsByTagName('suggestion')
            for sug in suggestions:
                matches=pattern.findall(sug.getAttribute('data'))
                for found in matches:
                    if found not in term_words and len(found) > 0:
                        newt=str(found)
                        
                        amb=self.get_ambiguity(newt)
                        spe=self.get_specificity(newt)
                        new_terms.append({newt.lower():{'type':'s','amb':amb,'spe':spe,'term':term}})

        return self.insert_data(new_terms,term,"search")

    def write_results_to_cassandra(self,dic):
        for word_key in dic.keys():
            terms = dic[word_key]['term'].split()
            etype = dic[word_key]['type']
            ambiguity = dic[word_key]['amb']
            specificity = dic[word_key]['spe']
            row=[word_key,dic[word_key]['type'],dic[word_key]['amb'],dic[word_key]['spe'],dic[word_key]['term']]
            pos = self.pos.getPOS(word_key)
            
            table_name = None
            if (etype == 's'):
                table_name = 'fourgram_search_expansion'
            elif (etype == 'c'):
                table_name = 'fourgram_corpus_expansion'
            else:
                print "Unknown exansion type %s for term %s" % (term, etype)
                raise UnknownExpansionType()
            query = """
	    insert into fourgm.%s (gramone, gramtwo, gramthree, expansion, specificity, ambiguity, pos)
	    values (:gramone, :gramtwo, :gramthree, :expansion, :specificity, :ambiguity, :pos)
            """ % (table_name)
            values = {"gramone":terms[0], "gramtwo":terms[1], "gramthree":terms[2], "expansion":word_key, "specificity":specificity, "ambiguity":ambiguity, "pos":pos}
            for i in range(20):
                ## The Database might time out, if so back off and try again
                try:
                    self.cursor.execute(query, values)
                except cql.apivalues.OperationalError:
                    print "Timeout, backoff and retry"
                    time.sleep(20)
                    continue
                else:
                    break
            

    def top_index_expand(self,term,count):
	## Use the cassandra database to get the Google Corpus terms
	## as opposed to the SQL server database
	return self.top_index_expand_cassandra(term,count)

    def top_index_expand_cassandra(self,term,count):
        """ Method which expands the query using the Google Web Corpus parsed and stored
            in a Cassandra database
        """
        print "Corpus Expanding %s" % term
        terms = term.split()
        ## We are only set up to do expansions on 3-gram searches ... so check for this
        if not len(terms) == 3:
            raise NotThreeGram()
        expansion_terms=[]
        good_queries=[]
	query = """
	select expansion,ambiguity,specificity,gramone,gramtwo,gramthree from fourgram_corpus_expansion
	"""
	for i in range(20):
            ## The Database might time out, if so back off and try again
            try:
                self.cursor.execute(query)
            except cql.apivalues.OperationalError:
                print "Timeout, backoff and retry"
                time.sleep(20)
                continue
            else:
                break
                    
        results=self.cursor.fetchall()

        ## Need a different data structure, move results into a dictionary where the
        ## key is the term and the value is frequency that the term occured in the result set
        term_freq = dict()
        for r in results:
            term_freq[r[0]] = r[1]

        ## Now sort that data structure by descending frequency
        sorted_terms = []
        for w in sorted(term_freq, key=term_freq.get, reverse=True):
            sorted_terms.append(w)
        
        for i in range(count):
            try:
                ##result=str(results[i][0])
                result = sorted_terms[i]
                print "RESULT: %s" % result
                tokenizer=word_tokenize(result)
                for word in tokenizer:
                    if word.lower() not in term.lower() and len(word)>0:
                        try:
                            amb=self.get_ambiguity(word)
                            spe=self.get_specificity(word)
                        except Exception as inst:
                            print type(inst)
                            print inst.args
                        expansion_terms.append({word.lower():{'type':'c','amb':amb,'spe':spe,'term':term}})

                        if term not in good_queries:
                            good_queries.append(term)
            except:
                pass
        s = None    
        for q in good_queries:
            ## Google might time out, retry a few times
            for i in range(5):
                try:
                    s=self.search_expand(q)
                    time.sleep(2)
                except xml.parsers.expat.ExpatError:
                    print "Error returned from Google ... retrying"
                    raise
                    time.sleep(20)
                    continue
                else:
                    break
            ## We didn't get anything, go to the next term
            if not s:
                continue

            try:
                for res in s:
                    expansion_terms.append(res)
            except:
                pass
        return self.insert_data(expansion_terms,term,"top")

    def top_index_expand_sql(self,term,count):
        print "Corpus Expanding %s" % term
        expansion_terms=[]
        good_queries=[]
        self.cursor.execute("""SELECT term,termcount FROM GoogleCorpus.dbo.Ngram
                               WHERE term LIKE '%s'
                               ORDER BY termcount DESC"""
                            % (term+'%'))
        results=self.cursor.fetchall()
        for i in range(count):
            try:
                result=str(results[i][0])
                #print "RESULT: %s" % result
                tokenizer=word_tokenize(result)
                for word in tokenizer:
                    if word.lower() not in term.lower() and len(word)>0:
                        try:
                            amb=self.get_ambiguity(word)
                            spe=self.get_specificity(word)
                        except Exception as inst:
                            print type(inst)
                            print inst.args
                        expansion_terms.append({word.lower():{'type':'c','amb':amb,'spe':spe,'term':term}})

                        if term not in good_queries:
                            good_queries.append(term)
            except:
                pass
        for q in good_queries:
            s=self.search_expand(q)
            time.sleep(2)
            try:
                for res in s:
                    expansion_terms.append(res)
            except:
                pass
        return self.insert_data(expansion_terms,term,"top")

    def bottom_index_expand(self,term,count):
        # UNUSED METHOD
        expansion_terms=[]
        self.cursor.execute("""SELECT term,termcount FROM GoogleCorpus.dbo.Ngram
                               WHERE term LIKE '%s'
                               ORDER BY termcount ASC"""
                            % (term+'%'))
        results=self.cursor.fetchall()
        for i in range(count):
            try:
                expansion_terms.append(results[i])
            except Exception as inst:
                print type(inst)

        self.insert_data(expansion_terms,term,"bot")
        
    def insert_data(self,expansion_terms,term,indextype):
        """Placeholder method for inserting into database.
        Currently just returns the terms it was passed.
        """
        if len(expansion_terms) > 0:
            self.total_count+=1
            return expansion_terms

    def connect(self):
        """Connect to the two databases
        self.connection is for NGram Corpus on Cassandra
        self.connection2 is for UMLS on SQLServer 2008
        """
        ## This is the cassandra database with the 4-gram Google Web Corpus
        
        self.connection = cql.connect('127.0.0.1', 9160, "fourgm", cql_version = '3.0.0')
        self.cursor = self.connection.cursor()

    def write_to_file(self,filename,data):
        """Write to csv file in format:
        expansion term, ambiguity, specificity, term
        per line
        """
        opened=open(filename,'wb')
        writer=csv.writer(opened)
        for dictionary in data:
            for word_key in dictionary.keys():
                row=[word_key,dictionary[word_key]['type'],dictionary[word_key]['amb'],dictionary[word_key]['spe'],dictionary[word_key]['term']]
                writer.writerow(row)
        opened.close()

    def get_specificity(self,term):
        sql = """SELECT TOP 15 str, treeNumber as specificityCount FROM mesh 
                 where LOWER(str) = ?
        """

        ## I switched to parameter binding rather than use string interpolation (%)
        ## to keep the query from failing whene there is certain punctuation in the term
        self.cursor2.execute(sql, term.lower())

        results=self.cursor2.fetchall()
        return len(results)
    
    def get_ambiguity(self,term):
        sql = """SELECT DISTINCT(cui) as ambigTermCount FROM mrconso 
                 WHERE lower(str) like ?
              """

        ## I switched to parameter binding rather than use string interpolation (%)
        ## to keep the query from failing whene there is certain punctuation in the term
        self.cursor2.execute(sql, term.lower())
        results=self.cursor2.fetchall()
        return len(results)

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


def write_to_file(filename):
    """Write to csv file in format:
    expansion term, ambiguity, specificity, term
    per line
    """
    connection = cql.connect('127.0.0.1', 9160, "fourgm", cql_version = '3.0.0')
    cursor = connection.cursor()
    query = """
	select expansion,ambiguity,specificity,pos,gramone,gramtwo,gramthree from fourgram_corpus_expansion limit 1000000000
    """

    for i in range(20):
        ## The Database might time out, if so back off and try again
        try:
            cursor.execute(query)
        except cql.apivalues.OperationalError:
            print "Timeout, backoff and retry"
            time.sleep(20)
            continue
        else:
            break
                    
    results=cursor.fetchall()

    opened=open(filename,'wb')
    writer=csv.writer(opened)
    for r in results:
	items = list()
        items.append(r[0].encode('utf-8'))
	items.append(r[1])
	items.append(r[2])
	items.append(r[3])
	items.append(r[4].encode('utf-8'))
	items.append(r[5].encode('utf-8'))
	items.append(r[6].encode('utf-8'))
	wn_spec = get_wordnet_specificity(items[0])
	wn_ambig = get_wordnet_ambiguity(items[0])
        row = [items[0],'c',items[1],items[2],items[3],"%s %s %s" % (items[4],items[5],items[6]),wn_spec, wn_ambig]	
	writer.writerow(row)
    opened.close()
    results = ""
    print "Done writing initial corpus expansion results"
    cursor.close()
    connection.close()
    time.sleep(20)
    connection = cql.connect('127.0.0.1', 9160, "fourgm", cql_version = '3.0.0')
    cursor = connection.cursor()
    print "Starting Search expansion results"

    query = """
	select expansion,ambiguity,specificity,pos,gramone,gramtwo,gramthree from fourgram_search_expansion limit 1000000000
    """

    cursor.execute(query)
    """
    for i in range(20):
        ## The Database might time out, if so back off and try again
	cursor.execute(query)
        try:
            cursor.execute(query)
        except cql.apivalues.OperationalError:
            print "Timeout, backoff and retry"
            time.sleep(20)
            continue
        else:
            break
    """
                    
    results=cursor.fetchall()

    opened=open(filename,'ab')
    writer=csv.writer(opened)
    for r in results:
	items = list()
        items.append(r[0].encode('utf-8'))
	items.append(r[1])
	items.append(r[2])
	items.append(r[3])
	items.append(r[4].encode('utf-8'))
	items.append(r[5].encode('utf-8'))
	items.append(r[6].encode('utf-8'))
	wn_spec = get_wordnet_specificity(items[0])
	wn_ambig = get_wordnet_ambiguity(items[0])
        row = [items[0],"s",items[1],items[2],items[3],"%s %s %s" % (items[4],items[5],items[6]),wn_spec,wn_ambig]	
	writer.writerow(row)
    opened.close()

def main():
    filename = "sampled_text.csv"
    filename = "random_corpus_sample.csv"
    #filename = "random_search_sample.csv"
    try:
        filename = sys.argv[1]
    except IndexError:
        print "Put the name of the file you want to analyze as a command line argument"
        sys.exit()
    
    sum_counter = dict()
    sum_counter['c'] = dict()
    sum_counter['s'] = dict()
    sum_counter['c']['specificity'] = [0.0,0]
    sum_counter['c']['ambiguity'] = [0.0,0]
    sum_counter['s']['specificity'] = [0.0,0]
    sum_counter['s']['ambiguity'] = [0.0,0]

    prefilter_totals = dict()
    prefilter_totals['c'] = {'totals':0, 'misses':0}
    prefilter_totals['s'] = {'totals':0, 'misses':0}

    totals = dict()
    totals['c'] = {'totals':0, 'misses':0}
    totals['s'] = {'totals':0, 'misses':0}

    try:
        with open(filename, 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in csvreader:
                ## Count pre-filter results
                prefilter_totals[row[1]]['totals'] += 1 
                if row[6] == "":
                    prefilter_totals[row[1]]['misses'] += 1

                ## Apply the filter and count post-filter results
                if row[4] == "" or row[4] == "numeric":
                    totals[row[1]]['totals'] += 1
                    if row[6] != "":
                        sum_counter[row[1]]['specificity'][0] += float(row[6])
                        sum_counter[row[1]]['specificity'][1] += 1
                        sum_counter[row[1]]['ambiguity'][0] += float(row[7])
                        sum_counter[row[1]]['ambiguity'][1] += 1
                    else:
                        totals[row[1]]['misses'] += 1
    except IOError, e:
        print "Problem opening file %s with error: %s"  % (filename,e)
        sys.exit()
		    
    if sum_counter['c']['specificity'][1] != 0:
        corpus_specificity = sum_counter['c']['specificity'][0] / sum_counter['c']['specificity'][1]
        corpus_ambiguity = sum_counter['c']['ambiguity'][0] / sum_counter['c']['ambiguity'][1]
    else:
        corpus_specificity = 0
        corpus_ambiguity = 0

    if sum_counter['s']['specificity'][1] != 0:
        search_specificity = sum_counter['s']['specificity'][0] / sum_counter['s']['specificity'][1]
        search_ambiguity = sum_counter['s']['ambiguity'][0] / sum_counter['s']['ambiguity'][1]
    else:
        search_specificity = 0
        search_ambiguity = 0
    
    print "Corpus: Specificity %s, Ambiguity %s" % (corpus_specificity, corpus_ambiguity)
    print "Search: Specificity %s, Ambiguity %s" % (search_specificity, search_ambiguity)
    if totals['c']['totals'] != 0:
        print "Corpus: Total %s, Misses %s, Percent Misses %s" % (totals['c']['totals'], totals['c']['misses'], (float(totals['c']['misses'])/totals['c']['totals'])*100)
    else:
        print "No corpus data on Wordnet hits and misses"

    if totals['s']['totals'] != 0:
        print "Search: Total %s, Misses %s Percent Misses %s" % (totals['s']['totals'], totals['s']['misses'], (float(totals['s']['misses'])/totals['s']['totals'])*100)
    else:
        print "No search data on Wordnet hits and misses"

    if prefilter_totals['s']['totals'] != 0:
        print "Prefilter Search: Total %s, Misses %s Percent Misses %s" % (prefilter_totals['s']['totals'], prefilter_totals['s']['misses'], (float(prefilter_totals['s']['misses'])/prefilter_totals['s']['totals'])*100)
    else:
        print "No prefilter search data on Wordnet hits and misses ... very odd"

    if prefilter_totals['c']['totals'] != 0:
        print "Prefilter Corpus: Total %s, Misses %s Percent Misses %s" % (prefilter_totals['c']['totals'], prefilter_totals['c']['misses'], (float(prefilter_totals['c']['misses'])/prefilter_totals['c']['totals'])*100)
    else:
        print "No prefilter corpus data on Wordnet hits and misses ... very odd"

if __name__=="__main__":
    main()
