from collections import defaultdict
import numpy as np
import random
import sys
import time
import logging
from xpinyin import Pinyin

from portrait.DB.MongodbClient import MongodbClient
from portrait.utils.LogHandler import LogHandler

class LSHCache:

    def __init__(self, db, n=100, b=20, r=5, shingle_size=3):
        # assign it
        self._n = n
        self._b = b
        self._r = r
        self._max_shingle = shingle_size
        self._shingle_size = shingle_size

        # check it
        assert self._b*self._r == self._n, 'Minhash bands/rows/length mismatch: _b*_r != _n, _b=%d, _r=%d, _n=%d' % (self._b,self._r,self._n)
        assert self._shingle_size > 0, '_shingle_size must be greater than 0.  Current _shingle_size=%d' % (self._shingle_size)

        # make it
        self._shingles = {} # maps from words or sequences of words to integers
        self._counter = 0 # the global counter for word indicies in _shingles
        self._memomask = [] # stores the random 32 bit sequences for each hash function
        self._most_recent_insert = 0
        self._num_docs = 0
        self._init_hash_masks(self._n)
        self._reset_cache(self._b)

        self.db = db

        self.pinyin = Pinyin()
        self.logger = LogHandler('LSH_Cache'+db.__db_name__+'.'+db.__collect_name__)

        if not isinstance(db, MongodbClient):
            self.logger.error(u'Set MongodbClient Object Ahead!')

    def _init_hash_masks(self,num_hash):
        """
        This initializes the instance variable _memomask which is a list of the 
        random 32 bits associated with each hash function
        """
        for i in range(num_hash):
            random.seed(i)
            self._memomask.append(int(random.getrandbits(32)))

    def _reset_cache(self, band):
        self._seen = set()  # the set of doc ids which have already been hashed
        self._cache = [defaultdict(list) for i in range(band)]

    def _xor_hash(self,mask,x):
        """
        This is a simple hash function which returns the result of a bitwise XOR
        on the input x and the 32-bit random mask
        """
        return int(x ^ mask)
        
    def _get_shingle_vec(self, doc):
        """
        Takes a sequence of tokenized words and maps each shingle to a unique id.
        These unique ids, are then added to the shingle_vec object which is just a sparse
        vector implemented as a dict with v[id]=1 when a shingle id is present
        """
        logging.debug('entering with len(doc)=%d', len(doc))
        v = {}
        # print doc
        if isinstance(doc, unicode):
            # for n in range(1,self._max_shingle):
            #     for j in range(max(1,len(doc) - n)):
            #         s = doc[j:j+n]
            #         if not self._shingles.has_key(s):
            #             self._shingles[s] = self._counter
            #             self._counter += 1
            #         v[self._shingles[s]] = 1
            for i in range(max(1,len(doc)-self._shingle_size)):
                s = doc[i:min(len(doc),i+self._shingle_size)]
                if not self._shingles.has_key(s):
                    self._shingles[s] = self._counter
                    self._counter += 1
                v[self._shingles[s]] = 1
        if isinstance(doc, list):
            for n in range(1,self._shingle_size):
                for i in range(len(doc) - n):
                    s = doc[i:i+n]
                    if not self._shingles.has_key(tuple(s)):
                        self._shingles[tuple(s)] = self._counter
                        self._counter += 1
                    v[self._shingles[tuple(s)]] = 1
        return v

    def _get_sig(self,shingle_vec,num_perms):
        """
        Takes a shingle vec and computes the minhash signature of length n using
        approximate permutations.  This method is explained in Mining Massive
        Datasets by Rajaraman and Ullman (http://infolab.stanford.edu/~ullman/mmds.html)
        in section 3.3.4.
        """
        mhash = [{} for i in range(num_perms)]
        keys = sorted(shingle_vec.keys())
        for r in keys:
            #logging.debug('r=%d', r)
            h = np.array([self._xor_hash(mask,r) for mask in self._memomask])
            for i in range(num_perms):
                if (h[i] < mhash[i]):
                    mhash[i] = h[i]
        return mhash

    def _get_lsh(self,sig,b,r):
        """
        Takes an n-dimensional minhash signature and computes b hashes for each of
        b bands of r rows in the signature.  These hashes can take on any value that
        can be stored in the 32bit integer.
        """
        lsh = []
        for i,band in enumerate(range(b)):
            lsh.append(hash(tuple(sig[i*r:i*r+r])))
        #logging.debug('hashed signature: %s\n[get_lsh]\tto bins: %s', (sig,lsh)
        return lsh
    
    def _get_lsh_from_doc(self, doc):
        """
        given an iterable of hashable items, returns a list of bucket ids
        """
        logging.debug('got tokenized doc: len(doc)=%d', len(doc))
        shingle_vec = self._get_shingle_vec(doc)
        # print "shingle_vec:",
        # print shingle_vec
        # print "keys of shingle_vec:",
        # print sorted(self._shingles.keys())
        logging.debug('got shingle_vec: len(shingle_vec)=%d', len(shingle_vec))
        sig = self._get_sig(shingle_vec,self._n) # n-dimensional min-hash signiture
        # print "sig:",
        # print sig
        logging.debug('got minhash sig: len(sig)=%d', len(sig))
        lsh = self._get_lsh(sig,self._b,self._r) # r-dimensional list of bucket ids
        # print "lsh",
        # print lsh
        return lsh

    def _insert_lsh(self,lsh,doc_id,date_added):
        """
        Given an LSH vector of bucket indices, this method inserts the current doc
        id in the corresponding bucket for each of the _b tables
        """
        if (doc_id in self._seen):
            logging.info(u'The %s is already processed', doc_id)
            return False
        else:
            self._num_docs += 1
            if (date_added > self._most_recent_insert):
                self._most_recent_insert = date_added
            self._seen.add(doc_id)
            for i,band_bucket in enumerate(lsh):
                if doc_id not in self._cache[i][band_bucket]:
                    self._cache[i][band_bucket].append(doc_id)
            #     print i,band_bucket
            # print self._cache
            # print len(self._cache)
            return True

    def _insert(self, doc, id, date_added=int(time.time()), passive=True):
        lsh = self._get_lsh_from_doc(doc)
        logging.debug('id: %d lsh: %s', id, lsh)
        # print doc, lsh
        if self._insert_lsh(lsh, id, date_added):
            return True
        else:
            return False

    # public methods

    def insert_batch(self, doc_tuples, id_tag, content_tag):
        """Batch method for adding db docs to cache"""
        for doc_tuple in doc_tuples:
            if id_tag not in doc_tuple or content_tag not in doc_tuple:
                continue
            doc_id = doc_tuple[id_tag]
            doc = self.pinyin.get_pinyin(doc_tuple[content_tag]).replace('-','')
            # print doc
            if not self._insert(doc, doc_id):
                logging.info(u'The hashing %s is failed', doc_tuple)

    def store_in_mongo(self, db_coll_name, doc_tuples, id_tag, content_tag):
        self.insert_batch(doc_tuples, id_tag, content_tag)
        base_coll_name = db_coll_name
        for i in range(self._b):
            cur_coll_name = '{}_{}'.format(base_coll_name,i)
            self.db.changeTable(cur_coll_name)
            records = list()
            for k,v in self._cache[i].iteritems():
                for doc_id in v:
                    records.append({'hash':k,'doc_id':doc_id})
            # print "====="
            # print doc_tuples
            # print i, self._cache[i]
            # print records
            if records:
                self.db.put_many(records)
        self._reset_cache(self._b)

    def cache(self):
        return self._cacheupdate

    def num_docs(self):
        return self._num_docs

    def most_recent_insert(self):
        return self._most_recent_insert

    def num_shingles(self):
        return self._counter

