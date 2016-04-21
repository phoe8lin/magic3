# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.5+
import threading, getpass
import asyncio, aiomysql, aioredis
from magic3.Utils import *

if PythonVersion <= (3, 4):
    raise ImportError("python version should be >= 3.5.0")


class AioDBClient(object):
    """ As base of asyncio database wrappers below """
    def __init__(self):
        self._tasks = []
    
    def Addtask(self, task, done_callback=None):
        """ add a async task from execute or ExecuteWithCallback """
        task = aio_to_future(task)
        if done_callback:
            task.add_done_callback(lambda f : done_callback(f.result()))
        self._tasks.append(task)
        return self
    
    def Addtasks(self, *tasks, done_callback=None):
        """ add multi tasks """
        for t in tasks:
            self.Addtask(t, done_callback)
        return self

    def ClearTasks(self):
        """ clear all tasks """
        self._tasks.clear()
        return self

    @property
    def tasks(self):
        """ return tasks as futures """
        assert self.size
        return AioTasks(*self._tasks) 

    @property
    def size(self):
        """ length of tasks list """
        return len(self._tasks)

    def RunAllTasks(self):
        """ blocking call, run all tasks """
        assert self.size
        try:
            AioRun(self.tasks)
        except Exception as e:
            raise e
        finally:
            self.ClearTasks()
        return self


class AioMysql(AioDBClient):
    """ A wrapper of aiomysql for simple and easy using """
    def __init__(self, **kwargs):
        """ kwargs is the options for connecting like:
            {
                "host"     : "127.0.0.1",
                "port"     : 3306,
                "user"     : "root",
                "password" : "fucker123",
                "db"       : "test"
            } """
        super().__init__()
        self._opt = dict(kwargs)

    def Pool(self):
        """ internal using for make aiomysql pool """
        return aiomysql.create_pool(**self._opt)
    
    def Connect(self):
        """ internal using """
        return aiomysql.connect(**self._opt)

    async def __Execute(self, sql, *args):
        """ internal using """
        async with self.Connect() as con:
            async with con.cursor() as cur:
                await cur.execute(sql, *args)
                await con.commit()
                return cur

    async def __ExecuteMany(self, sql, vargs):
        """ internal using """
        async with self.Connect() as con:
            async with con.cursor() as cur:
                await cur.executemany(sql, vargs)
                await con.commit()
                return cur

    async def Execute(self, sql, *args):
        """ coroutine of execute a SQL with args like pymysql, return fetched results """
        cur = await self.__Execute(sql, *args)
        return await cur.fetchall()

    async def ExecuteWithCallback(self, callback, sql, *args):
        """ coroutine of execute a SQL with args like pymysql, return cur_callback(results) """
        cur = await self.__Execute(sql, *args)
        return callback(await cur.fetchall())

    async def ExecuteMany(self, sql, argslist):
        """ coroutine of execute a SQL with args like pymysql, return fetched results """
        cur = await self.__ExecuteMany(sql, argslist)
        return await cur.fetchall()
 
    async def ExecuteManyWithCallback(self, callback, sql, argslist):
        """ coroutine of execute a SQL with args like pymysql, return cur_callback(results) """
        cur = await self.__ExecuteMany(sql, argslist)
        return callback(await cur.fetchall())

    @staticmethod
    def DefaultConnectArgs():
        """ get default connection arguments """
        return {
                "host" : '127.0.0.1', 
                "port" : 3306, 
                "db"   : 'test', 
                "user" : 'root', 
                "password" : ''
        }


class AioRedis(AioDBClient):
    """ A wrapper of aiomysql for simple and easy using """
    def __init__(self, host='localhost', port=6379):
        self._host = host
        self._port = port
        self._tasks = []
        self._poolsize = (4, 16)
        
    def SetPoolSize(self, minsize, maxsize):
        """ set min and max size of pool """
        if minsize <= 0:
            minsize = 1
        if maxsize <= 0:
            maxsize = 1
        self._poolsize = (minsize, maxsize)
    
    def Pool(self):
        """ internal using """
        return aioredis.create_pool((self._host, self._port), minsize=self._poolsize[0], maxsize=self._poolsize[1])
    
    def connect(self):
        """ internal using """
        return aioredis.create_connection((self._host, self._port), encoding='utf-8')
    
    async def execute(self, cmd:str):
        """ execute one command """
        con = await self.Connect()
        try:
            if ' ' in cmd:
                method, args = cmd.split(' ', 1)
                return await con.execute(method.upper(), *args.strip().split())
            else:
                return await con.execute(cmd)
        finally:
            con.close()
    
    async def ExecuteMany(self, cmds:list):
        """ execute more commands in the list """
        pool = await self.Pool()
        try:
            ret = []
            async with pool.get() as con:
                for cmd, args in map(lambda _: _.split(' ', 1), cmds):
                    method = getattr(con, cmd.lower())
                    ret.append(await method(*args.split()))
            return ret
        finally:
            await pool.clear()
    
    async def DBSize(self):
        """ DBSIZE command is a special command, so makes it a method """
        con = await self.Connect()
        try:
            return await con.execute('DBSIZE')
        finally:
            con.close()
    
    @staticmethod 
    def DefaultConnectArgs():
        return ('localhost', 6379)


def InputPassword():
    """ help tester to get the password from console input """
    return str(getpass.getpass('Enter mysql password:'))


def TestAioMysql():
    """ simple test for this AioMysql """
    show = lambda r: Debug(list(r))
    options = AioMysql.DefaultConnectArgs()
    options['password'] = InputPassword()
    
    db = AioMysql(**options)
    
    # for looking clearly
    Addtask = db.Addtask
    Addtasks = db.Addtasks
    Execute = db.Execute
    ExecuteMany = db.ExecuteMany
    ExecuteWithCallback = db.ExecuteWithCallback
    ExecuteManyWithCallback = db.ExecuteManyWithCallback

    Debug('Test create table')

    SQL_DROP = "DROP TABLE IF EXISTS `test_aiodb`"
    SQL_CREATE = """
    CREATE TABLE `test_aiodb` (
        `id` INT NOT NULL AUTO_INCREMENT,
        `name` VARCHAR(30) NOT NULL,
        PRIMARY KEY (id)); 
    SHOW TABLES;
    """

    Addtask(Execute(SQL_DROP)).RunAllTasks()
    Addtask(Execute(SQL_CREATE)).RunAllTasks()
    Addtask(Execute("SHOW TABLES"), lambda r : Debug(list(sorted(r))[0])).RunAllTasks()

    Debug('Test execute many, insert 10000 rows!!!')
    names_to_insert = ['Python', 'Cpp', 'Java', 'Ruby', 'Scala', 'Delphi'] * 10000

    Addtask(
        ExecuteMany('INSERT INTO `test_aiodb` (`name`) VALUES (%s)', names_to_insert),
    ).RunAllTasks()

    Addtask(
        ExecuteWithCallback(show, 'SELECT COUNT(*) FROM `test_aiodb`'),
    ).RunAllTasks()

    Debug('Test singel execute')
    Addtask(
        ExecuteWithCallback(lambda _:debug(_[-1]), 'SELECT * FROM `test_aiodb`')
    ).RunAllTasks()
    
    Debug('Test multi execute')
    Addtasks(
        Execute('SELECT * FROM `test_aiodb` WHERE `id`=1'),
        Execute('SELECT * FROM `test_aiodb` WHERE `id`=2'),
        Execute('SELECT * FROM `test_aiodb` WHERE `id`=3'),
        Execute('SELECT * FROM `test_aiodb` WHERE `id`=4'),
        Execute('SELECT * FROM `test_aiodb` WHERE `id`=5'),
        done_callback = show
    ).RunAllTasks()

    Debug('Test double callback')
    Addtasks(
        ExecuteWithCallback(lambda r : r[:1], 'SELECT 1 FROM `test_aiodb`'),
        ExecuteWithCallback(lambda r : r[:2], 'SELECT 2 FROM `test_aiodb`'),
        ExecuteWithCallback(lambda r : r[:3], 'SELECT 3 FROM `test_aiodb`'),
        done_callback = show
    ).RunAllTasks()


def TestAioRedis():
    """ simple test for this AioRedis """
    n = 0
    def cb(x):
        nonlocal n
        n += len(x)
    ar = AioRedis()
    ar.Addtask(ar.DBSize(), lambda n: Debug('DBSIZE:', n)).RunAllTasks()
    ar.Addtask(ar.ExecuteMany(['SET my-key-for-test my-val', 'SET QQ-for-test 123'] * 500), cb).RunAllTasks()
    ar.Addtask(ar.ExecuteMany(['GET my-key-for-test', 'GET QQ-for-test'] * 500), cb).RunAllTasks()
    ar.Addtask(ar.DBSize(), lambda n: Debug('DBSIZE:', n)).RunAllTasks()
    ar.Addtasks(
        ar.execute('DEL my-key-for-test'), 
        ar.execute('DEL QQ'),
    ).RunAllTasks()
    ar.Addtask(ar.DBSize(), lambda n: Debug('DBSIZE:', n)).RunAllTasks()
    Debug(n)


if __name__ == '__main__':
    Debug('Test AioMysql')
    TestAioMysql()
    Debug('Test AioRedis')
    TestAioRedis()
    Debug('Test OK')


