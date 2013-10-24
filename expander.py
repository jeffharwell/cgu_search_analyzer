import csv
import re
import time
import pyodbc
import datetime

from xml.dom import minidom
from urllib import urlopen
from nltk.tokenize import *

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
            dom = minidom.parse(urlopen(url))
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
        """Connect to the two databases on separate SQLServer installs.
        self.connection is for NGram Corpus on SQLServer 2012
        self.connection2 is for UMLS on SQLServer 2008
        """
        self.connection = pyodbc.connect('DRIVER={SQL Server};Trusted_Connection=yes;SERVER=WIN-20IR78KE8UV\SQLSERVER2012;DATABASE=GoogleCorpus;UID=XXXXXXXXX')
        self.cursor = self.connection.cursor()

        self.connection2 = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost;DATABASE=umlsSmall;UID=XXXXXXXXX;PWD=XXXXXXXXX')
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
        self.cursor2.execute("""
                        SELECT TOP 15 str, treeNumber as specificityCount FROM mesh 
                        where LOWER(str) = '%s'
                        """ % term.lower())
        results=self.cursor2.fetchall()
        return len(results)
    
    def get_ambiguity(self,term):
        self.cursor2.execute("""
                        SELECT DISTINCT(cui) as ambigTermCount FROM mrconso 
                        WHERE lower(str) like '%s'
                        """ % term.lower())
        results=self.cursor2.fetchall()
        return len(results)

def main():
    data=[]
    newfile="../data/new_200k.csv" # File to be written to
    filename="../data/sampled3.txt" # File to be read
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
        except:
            print "Unable to expand term" 
    print "writing"
    expander.write_to_file(newfile,data)
    print "done"


if __name__=="__main__":
    main()
