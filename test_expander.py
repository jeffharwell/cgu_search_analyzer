import unittest
import expander as e
import mock

class TestExpanderClass(unittest.TestCase):

    def mock_load_file(self):
        self.terms = ['this is testing','hello again world']

    @mock.patch.object(e.Expander, 'load_file', mock_load_file)
    def setUp(self):
        self.expanderobj = e.Expander('dummyfile',3)

    def test_term_to_list(self):
        t = "a b c d"
        l = ["a","b","c","d"]
        self.assertEquals(self.expanderobj.term_to_list(t), l)
        t = "   testing    4  2 1  "
        l = ["testing","4","2","1"]
        self.assertEquals(self.expanderobj.term_to_list(t), l)

    def test_verify_search_length(self):
        with self.assertRaises(e.WrongGramSize):
            self.expanderobj.verify_search_length("a b c d")
        self.assertTrue(self.expanderobj.verify_search_length("a b c"))

    def test_encode_url_with_query(self):
        """ This is a horrible test. In the code being testing the 
            arguments are passed into urlencode as a dictionary, so
            the order of the arguments in the URL string is not 
            guaranteed to be the same, so testing this as a simple
            string comparison is error prone.
        """
        term = "this is a test"
        my_url = "http://clients1.google.com/complete/search?q=this+is+a+test&output=toolbar&h1=en"
        self.assertEquals(self.expanderobj.encode_url_with_query(term) ,my_url)

    def test_get_write_table_name(self):
        eo = self.expanderobj
        eo.num_grams = 2
        self.assertEquals(eo.get_write_table_name('s'),'threegram_search_expansion')
        self.assertEquals(eo.get_write_table_name('c'),'threegram_corpus_expansion')
        eo.num_grams = 3
        self.assertEquals(eo.get_write_table_name('s'),'fourgram_search_expansion')
        self.assertEquals(eo.get_write_table_name('c'),'fourgram_corpus_expansion')
        with self.assertRaises(RuntimeError):
            eo.get_write_table_name('never a valid type')
        eo.num_grams = 4
        with self.assertRaises(e.UnsupportedNGram):
            eo.get_write_table_name('c')

    def test_get_write_query(self):
        ## Big, ugly function, replicates the functionality of the 
        ## thing it is testing to a great degree, uugh, lets see
        ## this is going to complicated to make concise, this is all
        ## list comprehension
        eo = self.expanderobj
        with self.assertRaises(e.UnsupportedNGram):
            eo.num_grams = 50
            eo.get_write_query('c')

        etypes = ['c','s']
        valid_ngrams = [2, 3]
        def n_to_bind(n):
            if (n == 3):
                return ":gramone, :gramtwo, :gramthree"
            if (n == 2):
                return ":gramone, :gramtwo"

        def n_to_col(n):
            if (n == 3):
                return "gramone, gramtwo, gramthree"
            if (n == 2):
                return "gramone, gramtwo"

        def get_table_name(n,t):
            eo.num_grams = n
            return eo.get_write_table_name(t)

        templates = [ (n, t, n_to_bind(n), get_table_name(n,t), n_to_col(n)) for n in valid_ngrams for t in etypes ]

        for t in templates:
            query = """
	    insert into fourgm.%s (%s, expansion, specificity, ambiguity, pos)
	    values (%s, :expansion, :specificity, :ambiguity, :pos)
            """ % (t[3], t[4], t[2]) ## sorry
            eo.num_grams = t[0] ## again, sorry about that
            self.assertEquals(eo.get_write_query(t[1]),query) ## yup, that will do it

    def test_get_write_bind_variables(self):
        eo = self.expanderobj
        amb = 1
        spec = 2
        expansion = 'program'
        pos = 'N'
        base_dict = {"expansion":expansion, "specificity":spec, "ambiguity":amb, "pos":pos}
        terms = ['hello','again','world']
        base_dict['gramone'] = terms[0]
        base_dict['gramtwo'] = terms[1]

        ## Test three gram
        eo.num_grams = 2
        self.assertEquals(eo.get_write_bind_variables(terms[:2],expansion,spec,amb,pos),base_dict)

        ## Test four gram
        eo.num_grams = 3
        base_dict['gramthree'] = terms[2]
        self.assertEquals(eo.get_write_bind_variables(terms,expansion,spec,amb,pos),base_dict)

        ## Test fifty gram
        terms = range(50)
        eo.num_grams = 50
        with self.assertRaises(e.UnsupportedNGram):
            eo.get_write_bind_variables(terms,expansion,amb,spec,pos),

    def test_get_cassandra_bind_variables(self):
        eo = self.expanderobj
        two_terms = ["hello","world"]
        three_terms = ["hello","again","world"]
        two_term_bind = {"gramone":two_terms[0], "gramtwo":two_terms[1]}
        three_term_bind = {"gramone":three_terms[0], "gramtwo":three_terms[1], "gramthree":three_terms[2]}
        eo.num_grams = 3
        self.assertEquals(eo.get_cassandra_bind_variables(three_terms),three_term_bind)
        eo.num_grams = 2
        self.assertEquals(eo.get_cassandra_bind_variables(two_terms),two_term_bind)

        fifty_terms = range(50)
        with self.assertRaises(e.WrongGramSize):
            eo.get_cassandra_bind_variables(fifty_terms)

        eo.num_grams = 50
        with self.assertRaises(e.UnsupportedNGram):
            eo.get_cassandra_bind_variables(fifty_terms)

    def test_cassendra_expansion_query(self):
        ## you have got to be kidding ... I suck at this
        two_gram_query = """
            select gramthree, frequency from fourgm.threegram_lower where gramone = :gramone and gramtwo = :gramtwo
            """
        three_gram_query = """
            select gramfour, frequency from fourgm.fourgram_lower where gramone = :gramone and gramtwo = :gramtwo and gramthree = :gramthree
            """
        eo = self.expanderobj
        eo.num_grams = 2
        self.assertEquals(eo.get_cassandra_expansion_query(),two_gram_query)
        eo.num_grams = 3
        self.assertEquals(eo.get_cassandra_expansion_query(),three_gram_query)


if __name__ == '__main__':
    unittest.main()
