import csv
import re
import time
import pyodbc
import datetime
import xml.parsers.expat
import urllib

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
class NotTwoGram(Exception):
    """ A custom Exception class that is thrown when the parser encounters a search
        query that is not 2 grams long
    """
    pass
class UnsupportedNGram(Exception):
    """ A custom Exception class that is throw when the parser encounters a query 
        that is the wrong length
    """
    pass
class WrongGramSize(Exception):
    """ A custom exception class that the Expander class will throw when it is configured
        for a certain gram size and then receives a query that is not the correct size.
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
    def __init__(self,filename,num_grams):
        ## Right now the code is only written to support 2 gram 
        ## or 3 gram queries. Other than that it will throw an
        ## error.
        if num_grams != 2 and num_grams != 3:
            raise UnsupportedNGram()

        self.start=datetime.datetime.now()
        self.match_spaces = re.compile(r'\s{2,}')	
        self.terms=[]
        self.total_count=0
        self.pos = PartOfSpeech()
        self.num_grams = num_grams
        self.load_file(filename)

    def get_cassandra_expansion_query(self):
        """ This function returns the cassandra query needed to get a corpus
            expansion for a given size of query.
            It will throw an UnsupportedNGram error if it is asked to return
            a query for a size of search that it does not support.
        """
        
        ## This is some UGLY code
        query = ''
        if self.num_grams == 2:
            query = """
            select gramthree, frequency from fourgm.threegram_lower where gramone = :gramone and gramtwo = :gramtwo
            """
        elif self.num_grams == 3:
            query = """
            select gramfour, frequency from fourgm.fourgram_lower where gramone = :gramone and gramtwo = :gramtwo and gramthree = :gramthree
            """
        else:
            raise UnsupportedNGram()
        return query

    def get_cassandra_bind_variables(self,term_list):
        if len(term_list) != self.num_grams:
            raise WrongGramSize()
        if self.num_grams == 2:
            return {"gramone":term_list[0], "gramtwo":term_list[1]}
        elif self.num_grams == 3:
            return {"gramone":term_list[0], "gramtwo":term_list[1], "gramthree":term_list[2]}
        else:
            raise UnsupportedNGram()




    def load_file(self,filename):
        ## Load the file into the terms list
        opened=open(filename,'rb')
        reader=csv.reader(opened)

        for term in reader:
            self.terms.append(term[0])

        if len(self.terms) == 0:
            raise RuntimeError, "No terms were loaded from %s" % filename


    def verify_search_length(self,term):
        """ Verify that our search string has the right number of terms
        """
        term_list = self.term_to_list(term)
        if len(term_list) != self.num_grams:
            raise WrongGramSize("Expander Object configured for %s gram search terms but '%s' is a %s gram search term." % (self.num_grams, term, len(term_list)))
        return True

    def term_to_list(self,term):
        """ Convent a search query into a list of terms. Go ahead and 
            trim off any whitespace characters and collapse multiple white
            space characters between terms while we are at it
        """
        ##term_list = split(strip(self.match_spaces.sub(' ',term)))
        term_list = self.match_spaces.sub(' ',term).strip().split()
        return term_list

    def encode_url_with_query(self,term):
        ##url="http://clients1.google.com/complete/search?hl=en&output=toolbar&q="
        urlbase = "http://clients1.google.com/complete/search?"
        arguments = {'h1':'en', 'output':'toolbar', 'q':term}
        url = urlbase + urllib.urlencode(arguments)
        return url

    def search_expand(self,term):
        """Uses minidom to parse XML returned by Google search.
        Returns suggested terms (Up to 10, I think)
        """
        print "Search Expanding %s" % term
        pattern=re.compile('(\w*)\s*')
        matches=pattern.findall(term)

        ## Make sure it is the right length
        self.verify_search_length(term)

        term_words=self.term_to_list(term)
        new_terms=[]
        url = self.encode_url_with_query(term)

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
        already_found = []
        for sug in suggestions:
            matches=pattern.findall(sug.getAttribute('data'))
            for found in matches:
                if found not in term_words and len(found) > 0 and found not in already_found:
                    newt=str(found)
                    already_found.append(newt)
                    
                    amb=self.get_ambiguity(newt)
                    spe=self.get_specificity(newt)
                    new_terms.append({newt.lower():{'type':'s','amb':amb,'spe':spe,'term':term}})

        return self.insert_data(new_terms,term,"search")

    def get_write_table_name(self,expansion_type):
        prefix = ''
        if self.num_grams == 2:
            prefix = 'threegram'
        elif self.num_grams == 3:
            prefix = 'fourgram'
        else:
            raise UnsupportedNGram
        table_type = ''
        if expansion_type == 's':
            table_type = 'search'
        elif expansion_type == 'c':
            table_type = 'corpus'
        else:
            raise RuntimeError("Unknown Expansion Type %s" % expansion_type)

        return "%s_%s_expansion" % (prefix, table_type)
            
    def get_write_query(self,expansion_type):
        ## get the table name, varies according to the expansion type
        table_name = self.get_write_table_name(expansion_type)
        bind_base = ':gramone'
        col_base = 'gramone'
        if self.num_grams >= 2:
            col_base = "%s, gramtwo" % col_base
            bind_base = "%s, :gramtwo" % bind_base
        if self.num_grams >= 3:
            col_base = "%s, gramthree" % col_base
            bind_base = "%s, :gramthree" % bind_base
        if self.num_grams >= 4:
            raise UnsupportedNGram()

        query = """
	    insert into fourgm.%s (%s, expansion, specificity, ambiguity, pos)
	    values (%s, :expansion, :specificity, :ambiguity, :pos)
            """ % (table_name, col_base, bind_base)
        return query

    def get_write_bind_variables(self,terms,expansion_term,spec,amb,pos):
        ## This will give us the base, a dictionary with the grams encoded
        base = self.get_cassandra_bind_variables(terms)

        ## Now we build the rest by hand
        r = {"expansion":expansion_term, "specificity":spec, "ambiguity":amb, "pos":pos}

        ## Now put it together and throw it back
        return dict(base.items() + r.items())


    def write_results_to_cassandra(self,dic):
        for word_key in dic.keys():
            ## hmm, use objects much?
            terms = dic[word_key]['term'].split()
            etype = dic[word_key]['type']
            ambiguity = dic[word_key]['amb']
            specificity = dic[word_key]['spe']
            row=[word_key,dic[word_key]['type'],dic[word_key]['amb'],dic[word_key]['spe'],dic[word_key]['term']]
            pos = self.pos.getPOS(word_key)
          
            ## Put together our Cassandra write query
            query = self.get_write_query(etype)
            ## word_key is the expansion term
            values = self.get_write_bind_variables(terms,word_key,specificity,ambiguity,pos)
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

        ## This will throw "WrongGramSize" if the search query we are 
        ## being passed is the wrong size
        self.verify_search_length(term)
        terms = self.term_to_list(term)

        expansion_terms=[]
        good_queries=[]
        query = self.get_cassandra_expansion_query()
        bind_vars = self.get_cassandra_bind_variables(terms)

        for i in range(20):
            ## The Database might time out, if so back off and try again
            try:
                self.cursor.execute(query, bind_vars)
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
        
        for i in range(len(sorted_terms)):
            if i > count:
                ## We are just interested in the top ${count} changes
                break
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
                    break ## got a result, we are done
                except xml.parsers.expat.ExpatError:
                    print "Error returned from Google ... retrying"
                    if i >= 4:
                        ## Ok, of Google bombs out four times then
                        ## re-throw the exception, this will kill the
                        ## program
                        raise
                    time.sleep(20)
                    continue
            ## We didn't get anything, go to the next term
            if not s:
                continue

            for res in s:
                expansion_terms.append(res)
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
        for i in range(len(results)):
            if i > count:
            ## We are just interested in the top ${count} results
                break
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
        self.connection = cql.connect('127.0.0.1', 9160, "fourgm", cql_version = '3.0.0')
        self.cursor = self.connection.cursor()

        self.connection2 = pyodbc.connect('DRIVER={SQL Server};SERVER=XXX.XXX.XXX.XXX;DATABASE=umlsSmall;UID=XXXXXX;PWD=XXXXXXXXX')
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
                row=[word_key.encode('utf-8'),dictionary[word_key]['type'],dictionary[word_key]['amb'],dictionary[word_key]['spe'],dictionary[word_key]['term']]
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
    number_of_grams = int(sys.argv[2])

    if not number_of_grams:
        sys.exit("You must tell the program the number of grams it should expand.")
        
    print "Data Directory %s" % datadirectory
    data=[]
    newfile="./%s/sampled2_test_results.csv" % datadirectory # File to be written to
    filename="./%s/sampled2_test" % datadirectory # File to be read
    print "begin"
    expander=Expander(filename,number_of_grams)
    expander.connect()
    c=0
    l=len(expander.terms)
    print "Processing %s terms" % l
    for term in expander.terms: 
        c+=1
        print "%d / %d" % (c,l)
        try:
            print "Top Index Expand"
            d=expander.top_index_expand(term.lower(),10)
            print "Got %s" % d
            if d:
                for dic in d:
                    print "Processing Result %s" % dic
                    data.append(dic)
                    expander.write_results_to_cassandra(dic)
        except WrongGramSize():
            ## expander will throw a WrongGramSize exception if it is asked to
            ## parse a search that does not contain exactly three grams.
            ## Catch that error, note it, then continue.
            print "%s doesn't appear to be the proper length search" % term
        except:
            ## Print out the problem term to aid troubleshooting
            print "Unable to Cassandra expand term %s" % term
            ## Hmm, something unexpected happened. Re-raised the error and kill the
            ## program so that we can find and fix the problem.
            raise
    print "writing"
    expander.write_to_file(newfile,data)
    print "done"


if __name__=="__main__":
    main()
