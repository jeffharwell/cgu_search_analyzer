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

    def top_index_expand(self,term,count):
	## Use the cassandra database to get the Google Corpus terms
	## as opposed to the SQL server database
	self.top_index_expand_cassandra(term,count)

    def top_index_expand_cassandra(self,term,count):
        """ Method which expands the query using the Google Web Corpus parsed and stored
            in a Cassandra database
        """"
        print "Corpus Expanding %s" % term
        terms = term.split()
        ## We are only set up to do expansions on 3-gram searches ... so check for this
        if not len(terms) == 3:
            raise NotThreeGram()
        expansion_terms=[]
        good_queries=[]
	query = """
	select gramfour, frequency from fourgm.fourgram_lower where gramone = :gramone and gramtwo = :gramtwo and gramthree = :gramthree
        """
	for i in range(20):
            ## The Database might time out, if so back off and try again
            try:
                self.cursor.execute(query, {"gramone":terms[0], "gramtwo":terms[1], "gramthree":terms[2]})
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
        ##self.connection = pyodbc.connect('DRIVER={SQL Server};Trusted_Connection=yes;SERVER=WIN-20IR78KE8UV\SQLSERVER2012;DATABASE=GoogleCorpus;UID=XXXXXXXX')
        ##self.cursor = self.connection.cursor()
        ## This is the cassandra database with the 4-gram Google Web Corpus
        
        self.connection = cql.connect('127.0.0.1', 9160, "fourgm", cql_version = '3.0.0')
        self.cursor = self.connection.cursor()

        self.connection2 = pyodbc.connect('DRIVER={SQL Server};SERVER=127.0.0.1;DATABASE=umlsSmall;UID=XXXXXXXX;PWD=XXXXXXXX')
        self.cursor2=self.connection2.cursor()

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

def main():
    ## The first argument is the directory with the data file. This allows the program
    ## to be run easily on multiple data sets simultaneously.
    datadirectory = sys.argv[1]
    print "Data Directory %s" % datadirectory
    data=[]
    newfile="./%s/new_200k.csv" % datadirectory # File to be written to
    filename="./%s/sampled3.txt" % datadirectory # File to be read
    print "begin"
    expander=Expander(filename)
    expander.connect()
    c=0
    l=len(expander.terms)
    for term in expander.terms: 
        c+=1
        print "%d / %d" % (c,l)
        try:
            d=expander.top_index_expand(term.lower(),10)
            if d:
                for dic in d:
                    data.append(dic)
        except NotThreeGram:
            ## expander will throw a NotThreeGram exception if it is asked to
            ## parse a search that does not contain exactly three grams.
            ## Catch that error, note it, then continue.
            print "%s doesn't appear to be a three gram search" % term
        except:
            ## Print out the problem term to aid troubleshooting
            print "Unable to expand term %s" % term
            ## Hmm, something unexpected happened. Re-raised the error and kill the
            ## program so that we can find and fix the problem.
            raise
    print "writing"
    expander.write_to_file(newfile,data)
    print "done"


if __name__=="__main__":
    main()
