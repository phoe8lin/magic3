# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.4+
from threading import Lock
from pymysql import connections
from getpass import getpass
from magic3.utils import DummyLock, debug

""" OpMysql constructor kwargs like:
{
    "host"     : "192.168.1.24",
    "port"     : 3306,
    "username" : "root",
    "password" : "bingo321",
    "database" : "mytest"
}
"""

class OpMysql(object):
    """ simple wrapper of mysql operations, based on pymysql """
    def __init__(self, **kwargs):
        self._con = None
        if 'lock' in kwargs and kwargs['lock']:
            self._lock = Lock()
        else:
            self._lock = DummyLock()
        try:
            user = kwargs['username']
            passwd = kwargs['password']
            host, port, dbname= kwargs['host'], kwargs['port'], kwargs['database']
            if 'show' in kwargs and kwargs['show']:
                debug('Try to connect '+user+'@'+host+':'+str(port)+'/'+dbname)
            self._con = connections.Connection(host = str(host), port = int(port), 
                                                user = str(user), passwd = str(passwd), 
                                                database = str(dbname), charset = 'utf8')
        except Exception:
            raise RuntimeError('OpMysql: connect failed!')
        self._cur = self._con.cursor()
    
    
    def __del__(self):
        """ close in release """
        if self._con:
            self._con.commit()
            self._cur.close()
            self._con.close()
    
    def switchDatabase(self, dbname):
        """ change database """
        with self._lock:
            self._con.select_db(dbname)
    
    def execute(self, sql, args = None):
        """ execute SQL on selected database with commit """
        with self._lock:
            cur = self._con.cursor()
            cur.execute(sql, args = args)
            self._con.commit()  
            cur.close()          
            
    def executeNoCommit(self, sql, args = None):
        """ execute SQL on selected database without commit """
        with self._lock:
            self._cur.execute(sql, args = args)
    
    def commit(self):
        """ COMMIT """
        with self._lock:
            self._con.commit()
    
    def fetch(self, sql, args = None, size = 0):
        """ fetch data using SQL, always for SELECT tasks """
        with self._lock:
            cur = self._con.cursor()
            cur.execute(sql, args = args)
            try:
                if size <= 0 : 
                    items = cur.fetchall()
                else:
                    items = cur.fetchmany(size)
                cur.close()
                return items
            except Exception as e:
                cur.close()
                raise e
    
    def query(self, sql, buffered = True):
        """ only for simple query, return raw results """
        with self._lock:
            return self._con.query(sql, unbuffered = not buffered)


def test():
    mydb = OpMysql(username='root', password=getpass(),
                  host='localhost', port='3306', database='mysql')
    mydb.switchDatabase('mysql')
    
    debug(mydb.query("""SELECT * FROM `user` LIMIT %d;""" % (10,), True))

    sql = """SELECT * FROM `user` WHERE host=%s;"""
    cur = mydb.fetch(sql, ("localhost",))
    for i in cur:
        debug(i)
    
    result = mydb.fetch(sql = """SELECT COUNT(*) FROM `user`""", args = None, size = -1)
    debug(result)


if __name__ == '__main__':
    test()


