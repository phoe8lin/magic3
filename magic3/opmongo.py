# -*- coding:utf-8 -*-
## author : cypro666
## note   : python3.4+
import sys
from copy import deepcopy
from urllib.parse import quote
import pymongo
from pymongo import MongoClient, ReadPreference
from bson.code import Code
from magic3.utils import timeMeter, printException, loadJson

# default mongodb host and port
DefaultAddress = ('localhost', 27017)
DefaultHost, DefaultPort = DefaultAddress
MongoErrors = pymongo.errors.PyMongoError

""" OpMongo's constructor kwargs like:
{
    "host"     : "192.168.1.33",
    "port"     : 27017,
    "username" : "root"
    "password" : "bingo321"
    "dbname"   : "spider"
} """

class OpMongo(object):
    """ wrapper of mongodb operations """
    def __init__(self, **kwargs):
        super(OpMongo, self).__init__()
        host = kwargs['host']
        port = kwargs['port']
        username = kwargs['username']
        password = kwargs['password']
        dbname = kwargs['dbname']
        if not password:
            uri = host
        else:
            uri = 'mongodb://%s:%s@%s:%d/%s' % (username,quote(password),host,port,dbname)
        self._mc = MongoClient(uri)
        self._db = self._mc[dbname]
        self._collection = None

    def __del__(self):
        """ close on exit """
        if self._mc:
            self._mc.close()
    
    def listCollections(self):
        """ return collection names current """
        assert self._db
        return deepcopy(self._db.collection_names())
    
    def choose(self, collname):
        """ select another collection """
        assert isinstance(collname, str)
        self._collection = self._db[str(collname)]
    
    def info(self):
        """ get infos of collection """
        s = 'connected to: %s\nchosen collection: %s\n'
        if self._collection.count(): 
            s += 'Warning : %s is Not Empty !!!' % self._collection.full_name
        return s % (str(self._db), self._collection.full_name)
    
    def remove(self):
        """ remove collection, this is dangerous!!! """
        if self._collection.count():
            self._collection.remove()
            sys.stderr.write('warning: %s removed!\n' % self._collection.full_name) 
    
    def execute(self, javascript, *args):
        """ execute `javascript` with optional `args`, low level """
        result = self._db.command('$eval', 
                                  Code(javascript),
                                  read_preference = ReadPreference.PRIMARY,
                                  args = args)
        return result.get('retval', None)
    
    def update(self, spec, doc, upsert = False):
        """ update or insert(upsert) specified by `spec` with doc """
        return self._collection.update(spec, doc, upsert = upsert)
    
    def insert(self, docs, fsync = False)->object:
        """ insert doc, if fsync is True, will be very very slow! """
        objects_id = self._collection.insert(docs, fsync)
        return objects_id
    
    def insertNoException(self, docs, fsync = False)->object:
        """ insert doc, if fsync is True, will be very very slow! """
        try:
            objects_id = self._collection.insert(docs, fsync)
            return objects_id
        except Exception as e:
            return e
    
    def count(self)->int:
        """ count of current collection """
        return self._collection.count()
    
    def createIndexes(self, indexlist, unique=False)->list:
        """ create indexes(means not only one) on current collection if enable """ 
        if not isinstance(indexlist, (list, tuple, set)):
            raise TypeError('indexlist shoule be list/tuple/set')
        return [self._collection.create_index(i, unique=unique) for i in indexlist]
    
    def findOne(self, query)->'cursor':
        """ query and return only one doc if found """
        cursor = self._collection.find_one(query)
        return cursor
    
    def explain(self, cursor)->str:
        """ explain """
        return str(cursor.explain())
    
    def find(self, query = {}, sortkey = None, reverse = True, explain = False):
        """ find doc specified by `query`, if `sortkey`, sort result """
        cursor = self._collection.find(query)
        if explain:
            self.explain(cursor)
        if sortkey:
            cursor.sort(sortkey, pymongo.DESCENDING if reverse else pymongo.ASCENDING)
        return cursor
    
    def findFields(self, query, fields:list):
        """ find doc specified by `query`, if `sortkey`, sort result """
        cursor = self._collection.find(query, fields)
        return cursor

    def distinct(self, key):
        return self._collection.distinct(key)


@timeMeter(__name__)
def test():
    ''' Simple tester for opmongo '''
    cfg = {'host':DefaultHost, 'port':DefaultPort, 'username':'root', 'password':'', 'dbname':'test'}
    db = OpMongo(**cfg)
    colls = db.listCollections()
    print(colls)
    db.choose(colls[0])
    print(list(db.find({}, explain=True)))


if __name__ == '__main__':
    test()

