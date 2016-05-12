# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
import sys, threading
from copy import deepcopy
from urllib.parse import quote
import pymongo, bson

# default mongodb host and port
DefaultAddress = ('localhost', 27017)
DefaultHost, DefaultPort = DefaultAddress
MongoErrors = pymongo.errors.PyMongoError


class EasyMongo(object):
    """ wrapper of mongodb operations """
    def __init__(self, **kwargs):
        """ kwargs is a dict or json like:
        {
            "host"     : "192.168.1.33",
            "port"     : 27017,
            "username" : "root",
            "password" : "bingo321",
            "dbname"   : "spider"
        } 
        """
        super().__init__()
        host = kwargs['host']
        port = kwargs['port']
        username = kwargs['username']
        password = kwargs['password']
        dbname = kwargs['dbname']
        if not password:
            uri = host
        else:
            uri = 'mongodb://%s:%s@%s:%d/%s' % (username,quote(password),host,port,dbname)
        if 'pool_size' in kwargs:
            self._mc = pymongo.MongoClient(uri, maxPoolSize=kwargs['pool_size'])
        else:
            self._mc = pymongo.MongoClient(uri, maxPoolSize=16)
        self._db = self._mc[dbname]
        self._collection = None

    def __del__(self):
        """ close on exit """
        if self._mc:
            self._mc.close()
    
    def list_collections(self):
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
                                  bson.code.Code(javascript),
                                  read_preference = pymongo.ReadPreference.PRIMARY,
                                  args = args)
        return result.get('retval', None)
    
    def update(self, spec, doc, upsert = False):
        """ update or insert(upsert) specified by `spec` with doc """
        return self._collection.update(spec, doc, upsert = upsert)
    
    def insert(self, docs, fsync = False)->object:
        """ insert doc, if fsync is True, will be very very slow! """
        objects_id = self._collection.insert(docs, fsync)
        return objects_id
    
    def safe_insert(self, docs, fsync = False)->object:
        """ insert doc, if fsync is True, will be very very slow! """
        try:
            objects_id = self._collection.insert(docs, fsync)
            return objects_id
        except Exception as e:
            return e
    
    def count(self)->int:
        """ count of current collection """
        return self._collection.count()
    
    def create_index(self, indexlist, unique=False)->list:
        """ create indexes(means not only one) on current collection if enable """ 
        if not isinstance(indexlist, (list, tuple, set)):
            raise TypeError('indexlist shoule be list/tuple/set')
        return [self._collection.create_index(i, unique=unique) for i in indexlist]
    
    def find_one(self, query)->'cursor':
        """ query and return only one doc if found """
        cursor = self._collection.find_one(query)
        return cursor
    
    def explain(self, cursor)->str:
        """ explain """
        return cursor.explain()
    
    def find(self, query = {}, sortkey = None, reverse = True, explain = False):
        """ find doc specified by `query`, if `sortkey`, sort result """
        cursor = self._collection.find(query)
        if explain:
            return self.explain(cursor)
        if sortkey:
            cursor.sort(sortkey, pymongo.DESCENDING if reverse else pymongo.ASCENDING)
        return cursor
    
    def find_field(self, query, fields:list):
        """ find doc specified by `query`, if `sortkey`, sort result """
        cursor = self._collection.find(query, fields)
        return cursor

    def distinct(self, key):
        return self._collection.distinct(key)


class EasyMongoDict(object):
    """ a dict to store more than one EasyMongo objects """
    def __init__(self, **kwargs):
        """ kwargs is same as EasyMongo's kwargs """
        super().__init__()
        self.__cfg = kwargs
        self.__mcd = dict()
    
    def remove(self, name):
        """ name is the key for dict """
        mc = self.__mcd.pop(name)
        del mc
    
    def add(self, name):
        """ add a new client """
        if name in self.__mcd:
            raise ValueError(name + ' is already existed!')
        self.__mcd[name] = EasyMongo(**self.__cfg)
    
    def __getitem__(self, name):
        """ get client by name """
        return self.__mcd[name]



def test():
    """ Simple tester for opmongo """
    cfg = {
        'host' : DefaultHost, 
        'port' : DefaultPort, 
        'username' : 'root', 
        'password' : '', 
        'dbname' : 'test',
        'pool_size' : 1
    }
    
    mcd = EasyMongoDict(**cfg)
    mcd.add('mydb1')
    mcd.add('mydb2')
    mcd.add('mydb3')
    colls = mcd['mydb1'].list_collections()
    print(colls)
    
    mcd['mydb2'].choose(colls[0])
    for doc in mcd['mydb2'].find({}):
        print(doc)
    print(mcd['mydb3'].execute("db.getName()"))


if __name__ == '__main__':
    test()


