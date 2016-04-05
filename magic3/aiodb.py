# -*- coding:utf-8 -*-
# author : cypro666
# note   : python3.5+
import threading, getpass
import asyncio, aiomysql, aioredis
from magic3.utils import *

if PythonVersion <= (3, 4):
    raise ImportError("python version should be >= 3.5.0")


class AioDBClient(object):
    """ As base of asyncio database wrappers below """
    def __init__(self):
        self._tasks = []
    
    def add_task(self, task, done_callback=None):
        """ add a async task from execute or execute_with_callback """
        task = aio_to_future(task)
        if done_callback:
            task.add_done_callback(lambda f : done_callback(f.result()))
        self._tasks.append(task)
        return self
    
    def add_tasks(self, *tasks, done_callback=None):
        """ add multi tasks """
        for t in tasks:
            self.add_task(t, done_callback)
        return self

    def clear_tasks(self):
        """ clear all tasks """
        self._tasks.clear()
        return self

    @property
    def tasks(self):
        """ return tasks as futures """
        assert self.tasks_size
        return aio_tasks(*self._tasks) 

    @property
    def tasks_size(self):
        """ length of tasks list """
        return len(self._tasks)

    def run_all_task(self):
        """ blocking call, run all tasks """
        assert self.tasks_size
        try:
            aio_run(self.tasks)
        except Exception as e:
            raise e
        finally:
            self.clear_tasks()
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

    def pool(self):
        """ internal using for make aiomysql pool """
        return aiomysql.create_pool(**self._opt)
    
    def connect(self):
        """ internal using """
        return aiomysql.connect(**self._opt)

    async def __execute(self, sql, *args):
        """ internal using """
        async with self.connect() as con:
            async with con.cursor() as cur:
                await cur.execute(sql, *args)
                await con.commit()
                return cur

    async def __execute_many(self, sql, vargs):
        """ internal using """
        async with self.connect() as con:
            async with con.cursor() as cur:
                await cur.executemany(sql, vargs)
                await con.commit()
                return cur

    async def execute(self, sql, *args):
        """ coroutine of execute a SQL with args like pymysql, return fetched results """
        cur = await self.__execute(sql, *args)
        return await cur.fetchall()

    async def execute_with_callback(self, callback, sql, *args):
        """ coroutine of execute a SQL with args like pymysql, return cur_callback(results) """
        cur = await self.__execute(sql, *args)
        return callback(await cur.fetchall())

    async def execute_many(self, sql, argslist):
        """ coroutine of execute a SQL with args like pymysql, return fetched results """
        cur = await self.__execute_many(sql, argslist)
        return await cur.fetchall()
 
    async def execute_many_with_callback(self, callback, sql, argslist):
        """ coroutine of execute a SQL with args like pymysql, return cur_callback(results) """
        cur = await self.__execute_many(sql, argslist)
        return callback(await cur.fetchall())

    @staticmethod
    def default_connect_args():
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
        
    def set_pool_size(self, minsize, maxsize):
        """ set min and max size of pool """
        if minsize <= 0:
            minsize = 1
        if maxsize <= 0:
            maxsize = 1
        self._poolsize = (minsize, maxsize)
    
    def pool(self):
        """ internal using """
        return aioredis.create_pool((self._host, self._port), minsize=self._poolsize[0], maxsize=self._poolsize[1])
    
    def connect(self):
        """ internal using """
        return aioredis.create_connection((self._host, self._port), encoding='utf-8')
    
    async def execute(self, cmd:str):
        """ execute one command """
        con = await self.connect()
        try:
            if ' ' in cmd:
                method, args = cmd.split(' ', 1)
                return await con.execute(method.upper(), *args.strip().split())
            else:
                return await con.execute(cmd)
        finally:
            con.close()
    
    async def execute_many(self, cmds:list):
        """ execute more commands in the list """
        pool = await self.pool()
        try:
            ret = []
            async with pool.get() as con:
                for cmd, args in map(lambda _: _.split(' ', 1), cmds):
                    method = getattr(con, cmd.lower())
                    ret.append(await method(*args.split()))
            return ret
        finally:
            await pool.clear()
    
    async def dbsize(self):
        """ DBSIZE command is a special command, so makes it a method """
        con = await self.connect()
        try:
            return await con.execute('DBSIZE')
        finally:
            con.close()
    
    @staticmethod 
    def default_connect_args():
        return ('localhost', 6379)


def input_password():
    """ help tester to get the password from console input """
    return str(getpass.getpass('Enter mysql password:'))


def test_aiomysql():
    """ simple test for this AioMysql """
    show = lambda r: debug(list(r))
    options = AioMysql.default_connect_args()
    options['password'] = input_password()
    
    db = AioMysql(**options)
    
    # for looking clearly
    add_task = db.add_task
    add_tasks = db.add_tasks
    execute = db.execute
    execute_many = db.execute_many
    execute_with_callback = db.execute_with_callback
    execute_many_with_callback = db.execute_many_with_callback

    debug('Test create table')

    SQL_DROP = "DROP TABLE IF EXISTS `test_aiodb`"
    SQL_CREATE = """
    CREATE TABLE `test_aiodb` (
        `id` INT NOT NULL AUTO_INCREMENT,
        `name` VARCHAR(30) NOT NULL,
        PRIMARY KEY (id)); 
    SHOW TABLES;
    """

    add_task(execute(SQL_DROP)).run_all_task()
    add_task(execute(SQL_CREATE)).run_all_task()
    add_task(execute("SHOW TABLES"), lambda r : debug(list(sorted(r))[0])).run_all_task()

    debug('Test execute many, insert 10000 rows!!!')
    names_to_insert = ['Python', 'Cpp', 'Java', 'Ruby', 'Scala', 'Delphi'] * 10000

    add_task(
        execute_many('INSERT INTO `test_aiodb` (`name`) VALUES (%s)', names_to_insert),
    ).run_all_task()

    add_task(
        execute_with_callback(show, 'SELECT COUNT(*) FROM `test_aiodb`'),
    ).run_all_task()

    debug('Test singel execute')
    add_task(
        execute_with_callback(lambda _:debug(_[-1]), 'SELECT * FROM `test_aiodb`')
    ).run_all_task()
    
    debug('Test multi execute')
    add_tasks(
        execute('SELECT * FROM `test_aiodb` WHERE `id`=1'),
        execute('SELECT * FROM `test_aiodb` WHERE `id`=2'),
        execute('SELECT * FROM `test_aiodb` WHERE `id`=3'),
        execute('SELECT * FROM `test_aiodb` WHERE `id`=4'),
        execute('SELECT * FROM `test_aiodb` WHERE `id`=5'),
        done_callback = show
    ).run_all_task()

    debug('Test double callback')
    add_tasks(
        execute_with_callback(lambda r : r[:1], 'SELECT 1 FROM `test_aiodb`'),
        execute_with_callback(lambda r : r[:2], 'SELECT 2 FROM `test_aiodb`'),
        execute_with_callback(lambda r : r[:3], 'SELECT 3 FROM `test_aiodb`'),
        done_callback = show
    ).run_all_task()


def test_aioredis():
    """ simple test for this AioRedis """
    n = 0
    def cb(x):
        nonlocal n
        n += len(x)
    ar = AioRedis()
    ar.add_task(ar.dbsize(), lambda n: debug('DBSIZE:', n)).run_all_task()
    ar.add_task(ar.execute_many(['SET my-key-for-test my-val', 'SET QQ-for-test 123'] * 500), cb).run_all_task()
    ar.add_task(ar.execute_many(['GET my-key-for-test', 'GET QQ-for-test'] * 500), cb).run_all_task()
    ar.add_task(ar.dbsize(), lambda n: debug('DBSIZE:', n)).run_all_task()
    ar.add_tasks(
        ar.execute('DEL my-key-for-test'), 
        ar.execute('DEL QQ'),
    ).run_all_task()
    ar.add_task(ar.dbsize(), lambda n: debug('DBSIZE:', n)).run_all_task()
    debug(n)


if __name__ == '__main__':
    debug('Test AioMysql')
    test_aiomysql()
    debug('Test AioRedis')
    test_aioredis()
    debug('Test OK')


