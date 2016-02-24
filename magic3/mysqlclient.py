#!/usr/bin/env python3
# -*- coding:utf-8 -*-
## author : cypro666
## date   : 2015.07.06
import sys, os
import asyncio
import re, struct
import socket
import warnings
import configparser
from functools import partial
from _collections import deque
from pymysql.err import (Warning, Error, 
                         InterfaceError, DataError,
                         DatabaseError, OperationalError, 
                         IntegrityError, InternalError,
                         NotSupportedError, ProgrammingError)
from pymysql.constants import SERVER_STATUS
from pymysql.constants.CLIENT import *
from pymysql.constants.COMMAND import *
from pymysql.charset import charset_by_name, charset_by_id
from pymysql.util import byte2int, int2byte
from pymysql.converters import escape_item, encoders, decoders, escape_string
from pymysql.connections import TEXT_TYPES, MAX_PACKET_LEN, DEFAULT_CHARSET
from pymysql.connections import _scramble, _scramble_323
from pymysql.connections import pack_int24
from pymysql.connections import MysqlPacket
from pymysql.connections import FieldDescriptorPacket
from pymysql.connections import EOFPacketWrapper, OKPacketWrapper

DEFAULT_USER = "root"

# regex for `Cursor.executemany`
# executemany only suports simple bulk insert
RE_INSERT_VALUES = re.compile(r"""(INSERT\s.+\sVALUES\s+)(\(\s*%s\s*(?:,\s*%s\s*)*\))""" +
                              """(\s*(?:ON DUPLICATE.*)?)\Z""",
                              re.IGNORECASE | re.DOTALL)

class Cursor:
    """Cursor is used to interact with the database
    Max stetement size which :meth:`executemany` generates.
    Max size of allowed statement is max_allowed_packet -
    packet_header_size.
    Default value of max_allowed_packet is 1048576."""
    max_stmt_length = 1024000
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, connection, echo=False):
        """ Do not create an instance of a Cursor yourself. Call
            connections.Connection.cursor(). """
        self._connection = connection
        self._loop = self._connection.loop
        self._description = None
        self._rownumber = 0
        self._rowcount = -1
        self._arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None
        self._lastrowid = None
        self._echo = echo

    @property
    def connection(self):
        """ This read-only attribute return a reference to the Connection
            object on which the cursor was created """
        return self._connection

    @property
    def description(self):
        """ This read-only attribute is a sequence of 7-item sequences.
            Each of these sequences is a collections.namedtuple containing
            information describing one result column:
        0.  name: the name of the column returned.
        1.  type_code: the type of the column.
        2.  display_size: the actual length of the column in bytes.
        3.  internal_size: the size in bytes of the column associated to
            this column on the server.
        4.  precision: total number of significant digits in columns of
            type NUMERIC. None for other types.
        5.  scale: count of decimal digits in the fractional part in
            columns of type NUMERIC. None for other types.
        6.  null_ok: always None as not easy to retrieve from the libpq.
        """
        return self._description

    @property
    def rowcount(self):
        """ Returns the number of rows that has been produced of affected. """
        return self._rowcount

    @property
    def rownumber(self):
        """ Row index """
        return self._rownumber

    @property
    def arraysize(self):
        """ How many rows will be returned by fetchmany() call. """
        return self._arraysize

    @arraysize.setter
    def arraysize(self, val):
        """ How many rows will be returned by fetchmany() call. """
        self._arraysize = val

    @property
    def lastrowid(self):
        """ This read-only property returns the value generated for an
            AUTO_INCREMENT column by the previous INSERT or UPDATE statement
            or None when there is no such value available. """
        return self._lastrowid

    @property
    def echo(self):
        """ Return echo mode status. """
        return self._echo

    @property
    def closed(self):
        """ The readonly property that returns ``True`` if connections was
            detached from current cursor """
        return True if not self._connection else False

    @asyncio.coroutine
    def close(self):
        """Closing a cursor just exhausts all remaining data."""
        conn = self._connection
        if conn is None:
            return
        try:
            while (yield from self.nextset()):
                pass
        finally:
            self._connection = None

    def _get_db(self):
        if not self._connection:
            raise ProgrammingError("Cursor closed")
        return self._connection

    def _check_executed(self):
        if not self._executed:
            raise ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """ Does nothing, required by DB API. """

    def setoutputsizes(self, *args):
        """ Does nothing, required by DB API. """

    @asyncio.coroutine
    def nextset(self):
        """Get the next query set"""
        conn = self._get_db()
        current_result = self._result
        if current_result is None or current_result is not conn._result:
            return
        if not current_result.has_next:
            return
        yield from conn.next_result()
        self._do_get_result()
        return True

    def _escape_args(self, args, conn):
        if isinstance(args, (tuple, list)):
            return tuple(conn.escape(arg) for arg in args)
        elif isinstance(args, dict):
            return dict((key, conn.escape(val)) for (key, val) in args.items())
        else:
            return conn.escape(args)

    @asyncio.coroutine
    def execute(self, query, args=None):
        """ Executes the given operation
            Executes the given operation substituting any markers with the given parameters.
        """
        assert query
        conn = self._get_db()
        while (yield from self.nextset()):
            pass
        if args is not None:
            query = query % self._escape_args(args, conn)
        yield from self._query(query)
        self._executed = query
        if self._echo:
            print(query)
            print("%r" % args)
        return self._rowcount

    @asyncio.coroutine
    def executemany(self, query, args):
        """ Execute the given operation multiple times
            The executemany() method will execute the operation iterating
            over the list of parameters in seq_params.
            Example: Inserting 3 new employees and their phone number
            data = [ ('Jane','555-001'),
                     ('Joe', '555-001'),
                     ('John', '555-003') ]
            stmt = "INSERT INTO employees (name, phone) VALUES ('%s','%s')"
            yield from cursor.executemany(stmt, data)
            INSERT statements are optimized by batching the data, that is
            using the MySQL multiple rows syntax.
            query: `str`, sql statement
            args: ``tuple`` or ``list`` of arguments for sql query """
        if not args:
            return
        if self._echo:
            print("CALL %s" % query)
            print("%r" % args)
        m = RE_INSERT_VALUES.match(query)
        if m:
            q_prefix = m.group(1)
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ''
            assert q_values[0] == '(' and q_values[-1] == ')'
            return (yield from self._do_execute_many(q_prefix, q_values, q_postfix, args, self.max_stmt_length,
                                                     self._get_db().encoding))
        else:
            rows = 0
            for arg in args:
                yield from self.execute(query, arg)
                rows += self._rowcount
            self._rowcount = rows
        return self._rowcount

    @asyncio.coroutine
    def _do_execute_many(self, prefix, values, postfix, args, max_stmt_length,
                         encoding):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, str):
            prefix = prefix.encode(encoding)
        if isinstance(postfix, str):
            postfix = postfix.encode(encoding)
        sql = bytearray(prefix)
        args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, str):
            v = v.encode(encoding, 'surrogateescape')
        sql += v
        rows = 0
        for arg in args:
            v = values % escape(arg, conn)
            if isinstance(v, str):
                v = v.encode(encoding, 'surrogateescape')
            if len(sql) + len(v) + len(postfix) + 1 > max_stmt_length:
                r = yield from self.execute(sql + postfix)
                rows += r
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        r = yield from self.execute(sql + postfix)
        rows += r
        self._rowcount = rows
        return rows

    @asyncio.coroutine
    def callproc(self, procname, args=()):
        """ Execute stored procedure procname with args
            procname: ``str``, name of procedure to execute on server
            args: `sequence of parameters to use with procedure """
        conn = self._get_db()
        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            yield from self._query(q)
            yield from self.nextset()
        _args = ','.join('@_%s_%d' % (procname, i) for i in range(len(args)))
        q = "CALL %s(%s)" % (procname, _args)
        yield from self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        """Fetch the next row """
        self._check_executed()
        fut = asyncio.Future(loop=self._loop)

        if self._rows is None or self._rownumber >= len(self._rows):
            fut.set_result(None)
            return fut
        result = self._rows[self._rownumber]
        self._rownumber += 1

        fut = asyncio.Future(loop=self._loop)
        fut.set_result(result)
        return fut

    def fetchmany(self, size=None):
        """Returns the next set of rows of a query result, returning a
        list of tuples. When no more rows are available, it returns an
        empty list.
        The number of rows returned can be specified using the size argument,
        which defaults to one
        size: ``int`` number of rows to return
        return ``list`` of fetched rows
        """
        self._check_executed()
        fut = asyncio.Future(loop=self._loop)
        if self._rows is None:
            fut.set_result([])
            return fut
        end = self._rownumber + (size or self._arraysize)
        result = self._rows[self._rownumber:end]
        self._rownumber = min(end, len(self._rows))

        fut.set_result(result)
        return fut

    def fetchall(self):
        """Returns all rows of a query result set"""
        self._check_executed()
        fut = asyncio.Future(loop=self._loop)
        if self._rows is None:
            fut.set_result([])
            return fut

        if self._rownumber:
            result = self._rows[self._rownumber:]
        else:
            result = self._rows
        self._rownumber = len(self._rows)

        fut.set_result(result)
        return fut

    def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position according to mode"""
        self._check_executed()
        if mode == 'relative':
            r = self._rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self._rownumber = r

        fut = asyncio.Future(loop=self._loop)
        fut.set_result(None)
        return fut

    @asyncio.coroutine
    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        yield from conn.query(q)
        self._do_get_result()

    def _do_get_result(self):
        conn = self._get_db()
        self._rownumber = 0
        self._result = result = conn._result
        self._rowcount = result.affected_rows
        self._description = result.description
        self._lastrowid = result.insert_id
        self._rows = result.rows


class _DictCursorMixin:
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super()._do_get_result()
        fields = []
        if self._description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class DictCursor(_DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""


class SSCursor(Cursor):
    """Unbuffered Cursor, mainly useful for queries that return a lot of
    data, or for connections to remote servers over a slow network.
    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """
    @asyncio.coroutine
    def close(self):
        conn = self._connection
        if conn is None:
            return

        if self._result is not None and self._result is conn._result:
            yield from self._result._finish_unbuffered_query()

        try:
            while (yield from self.nextset()):
                pass
        finally:
            self._connection = None

    @asyncio.coroutine
    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        yield from conn.query(q, unbuffered=True)
        self._do_get_result()
        return self._rowcount

    @asyncio.coroutine
    def _read_next(self):
        """Read next row """
        row = yield from self._result._read_rowdata_packet_unbuffered()
        row = self._conv_row(row)
        return row

    @asyncio.coroutine
    def fetchone(self):
        """ Fetch next row """
        self._check_executed()
        row = yield from self._read_next()
        if row is None:
            return
        self._rownumber += 1
        return row

    @asyncio.coroutine
    def fetchall(self):
        """Fetch all, as per MySQLdb. Pretty useless for large queries, as
        it is buffered.
        """
        rows = []
        while True:
            row = yield from self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    @asyncio.coroutine
    def fetchmany(self, size=None):
        """Returns the next set of rows of a query result, returning a
        list of tuples. When no more rows are available, it returns an
        empty list.
        The number of rows returned can be specified using the size argument,
        which defaults to one
        size: ``int`` number of rows to return
        return ``list`` of fetched rows
        """
        self._check_executed()
        if size is None:
            size = self._arraysize
        rows = []
        for i in range(size):
            row = yield from self._read_next()
            if row is None:
                break
            rows.append(row)
            self._rownumber += 1
        return rows

    @asyncio.coroutine
    def scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position
        according to mode . Same as :meth:`Cursor.scroll`, but move cursor
        on server side one by one row. If you want to move 20 rows forward
        scroll will make 20 queries to move cursor. Currently only forward
        scrolling is supported.
        int value: move cursor to next position according to mode.
        str mode: scroll mode, possible modes: `relative` and `absolute`
        """
        self._check_executed()
        if mode == 'relative':
            if value < 0:
                raise NotSupportedError("Backwards scrolling not supported "
                                        "by this cursor")

            for _ in range(value):
                yield from self._read_next()
            self._rownumber += value
        elif mode == 'absolute':
            if value < self._rownumber:
                raise NotSupportedError(
                    "Backwards scrolling not supported by this cursor")

            end = value - self._rownumber
            for _ in range(end):
                yield from self._read_next()
            self._rownumber = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)


class SSDictCursor(_DictCursorMixin, SSCursor):
    """An unbuffered cursor, which returns results as a dictionary """
    pass



@asyncio.coroutine
def _connect(host="localhost", user=None, password="",
            db=None, port=3306, unix_socket=None,
            charset='', sql_mode=None,
            read_default_file=None, conv=decoders, use_unicode=None,
            client_flag=0, cursorclass=Cursor, init_command=None,
            connect_timeout=None, read_default_group=None,
            no_delay=False, autocommit=False, echo=False, loop=None):
    """See connections.Connection.__init__() for information about
    defaults."""
    conn = Connection(host=host, user=user, password=password,
                      db=db, port=port, unix_socket=unix_socket,
                      charset=charset, sql_mode=sql_mode,
                      read_default_file=read_default_file, conv=conv,
                      use_unicode=use_unicode, client_flag=client_flag,
                      cursorclass=cursorclass, init_command=init_command,
                      connect_timeout=connect_timeout,
                      read_default_group=read_default_group, no_delay=no_delay,
                      autocommit=autocommit, echo=echo, loop=loop)
    yield from conn._connect()
    return conn


class Connection:
    """Representation of a socket with a mysql server.
    The proper way to get an instance of this class is to call
    connect().
    """
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, host="localhost", user=None, password="",
                 db=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, read_default_group=None,
                 no_delay=False, autocommit=False, echo=False, loop=None):
        """
        Establish a connection to the MySQL database. Accepts several
        arguments:
        host: Host where the database server is located
        user: Username to log in as
        password: Password to use.
        db: Database to use, None to not use a particular one.
        port: MySQL port to use, default is usually OK.
        unix_socket: Optionally, you can use a unix socket rather
        than TCP/IP.
        charset: Charset you want to use.
        sql_mode: Default SQL_MODE to use.
        read_default_file: Specifies  my.cnf file to read these
            parameters from under the [client] section.
        conv: Decoders dictionary to use instead of the default one.
            This is used to provide custom marshalling of types.
            See converters.
        use_unicode: Whether or not to default to unicode strings.
         client_flag: Custom flags to send to MySQL. Find
            potential values in constants.CLIENT.
        cursorclass: Custom cursor class to use.
        init_command: Initial SQL statement to run when connection is
            established.
        connect_timeout: Timeout before throwing an exception
            when connecting.
        read_default_group: Group to read from in the configuration
            file.
        no_delay: Disable Nagle's algorithm on the socket
        autocommit: Autocommit mode. None means use server default.
            (default: False)
        loop: asyncio loop
        """
        self._loop = loop or asyncio.get_event_loop()

        if use_unicode is None and sys.version_info[0] > 2:
            use_unicode = True

        if read_default_file:
            if not read_default_group:
                read_default_group = "client"
            cfg = configparser.RawConfigParser()
            cfg.read(os.path.expanduser(read_default_file))
            _config = partial(cfg.get, read_default_group)

            user = _config("user", fallback=user)
            password = _config("password", fallback=password)
            host = _config("host", fallback=host)
            db = _config("database", fallback=db)
            unix_socket = _config("socket", fallback=unix_socket)
            port = int(_config("port", fallback=port))
            charset = _config("default-character-set", fallback=charset)

        self._host = host
        self._port = port
        self._user = user or DEFAULT_USER
        self._password = password or ""
        self._db = db
        self._no_delay = no_delay
        self._echo = echo

        self._unix_socket = unix_socket
        if charset:
            self._charset = charset
            self.use_unicode = True
        else:
            self._charset = DEFAULT_CHARSET
            self.use_unicode = False

        if use_unicode is not None:
            self.use_unicode = use_unicode

        self._encoding = charset_by_name(self._charset).encoding

        client_flag |= CAPABILITIES
        client_flag |= MULTI_STATEMENTS
        if self._db:
            client_flag |= CONNECT_WITH_DB
        self.client_flag = client_flag

        self.cursorclass = cursorclass
        self.connect_timeout = connect_timeout

        self._result = None
        self._affected_rows = 0
        self.host_info = "Not connected"

        #: specified autocommit mode. None means use server default.
        self.autocommit_mode = autocommit

        self.encoders = encoders  # Need for MySQLdb compatibility.
        self.decoders = conv
        self.sql_mode = sql_mode
        self.init_command = init_command

        # asyncio StreamReader, StreamWriter
        self._reader = None
        self._writer = None

    @property
    def host(self):
        """MySQL server IP address or name"""
        return self._host

    @property
    def port(self):
        """MySQL server TCP/IP port"""
        return self._port

    @property
    def unix_socket(self):
        """MySQL Unix socket file location"""
        return self._unix_socket

    @property
    def db(self):
        """Current database name."""
        return self._db

    @property
    def user(self):
        """User used while connecting to MySQL"""
        return self._user

    @property
    def echo(self):
        """ Return echo mode status. """
        return self._echo

    @property
    def loop(self):
        """  """
        return self._loop

    @property
    def closed(self):
        """ The readonly property that returns ``True`` if connections is closed. """
        return self._writer is None

    @property
    def encoding(self):
        """ Encoding employed for this connection. """
        return self._encoding

    @property
    def charset(self):
        """ Returns the character set for current connection. """
        return self._charset

    def close(self):
        """ Close socket connection """
        if self._writer:
            self._writer.transport.close()
        self._writer = None
        self._reader = None

    @asyncio.coroutine
    def ensure_closed(self):
        """ Send quit command and then close socket connection """
        if self._writer is None:
            return
        send_data = struct.pack('<i', 1) + int2byte(COM_QUIT)
        self._writer.write(send_data)
        yield from self._writer.drain()
        self.close()

    @asyncio.coroutine
    def autocommit(self, value):
        """ Enable/disable autocommit mode for current MySQL session.
            value: ``bool``, toggle autocommit """
        self.autocommit_mode = bool(value)
        current = self.get_autocommit()
        if value != current:
            yield from self._send_autocommit_mode()

    def get_autocommit(self)->bool:
        """ Returns autocommit status for current MySQL session """
        status = self.server_status & SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT
        return bool(status)

    @asyncio.coroutine
    def _read_ok_packet(self):
        pkt = yield from self._read_packet()
        if not pkt.is_ok_packet():
            raise OperationalError(2014, "Command Out of Sync")
        ok = OKPacketWrapper(pkt)
        self.server_status = ok.server_status
        return True

    @asyncio.coroutine
    def _send_autocommit_mode(self):
        """ Set whether or not to commit after every execute() """
        yield from self._execute_command(COM_QUERY, "SET AUTOCOMMIT = %s" % self.escape(self.autocommit_mode))
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def begin(self):
        """ Begin transaction. """
        yield from self._execute_command(COM_QUERY, "BEGIN")
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def commit(self):
        """ Commit changes to stable storage. """
        yield from self._execute_command(COM_QUERY, "COMMIT")
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def rollback(self):
        """ Roll back the current transaction. """
        yield from self._execute_command(COM_QUERY, "ROLLBACK")
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def select_db(self, db):
        """ Set current db """
        yield from self._execute_command(COM_INIT_DB, db)
        yield from self._read_ok_packet()

    def escape(self, obj):
        """ Escape whatever value you pass to it"""
        if isinstance(obj, str):
            return "'" + self.escape_string(obj) + "'"
        return escape_item(obj, self._charset)

    def literal(self, obj):
        """ Alias for escape() """
        return self.escape(obj)

    def escape_string(self, s):
        if (self.server_status &
                SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES):
            return s.replace("'", "''")
        return escape_string(s)

    def cursor(self, cursor=None):
        """ Instantiates and returns a cursor """
        if cursor is not None and not issubclass(cursor, Cursor):
            raise TypeError('Custom cursor must be subclass of Cursor')

        cur = cursor(self, self._echo) if cursor else self.cursorclass(self)
        fut = asyncio.Future(loop=self._loop)
        fut.set_result(cur)
        return fut

    @asyncio.coroutine
    def query(self, sql, unbuffered=False):
        if isinstance(sql, str):
            sql = sql.encode(self.encoding, 'surrogateescape')
        yield from self._execute_command(COM_QUERY, sql)
        yield from self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    @asyncio.coroutine
    def next_result(self):
        yield from self._read_query_result()
        return self._affected_rows

    def affected_rows(self):
        return self._affected_rows

    @asyncio.coroutine
    def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        yield from self._execute_command(COM_PROCESS_KILL, arg)
        yield from self._read_ok_packet()

    @asyncio.coroutine
    def ping(self, reconnect=True):
        """Check if the server is alive"""
        if self._writer is None and self._reader is None:
            if reconnect:
                yield from self._connect()
                reconnect = False
            else:
                raise Error("Already closed")
        try:
            yield from self._execute_command(COM_PING, "")
            yield from self._read_ok_packet()
        except Exception:
            if reconnect:
                yield from self._connect()
                yield from self.ping(False)
            else:
                raise

    @asyncio.coroutine
    def set_charset(self, charset):
        """Sets the character set for the current connection"""
        encoding = charset_by_name(charset).encoding
        yield from self._execute_command(COM_QUERY, "SET NAMES %s"
                                         % self.escape(charset))
        yield from self._read_packet()
        self._charset = charset
        self._encoding = encoding

    @asyncio.coroutine
    def _connect(self):
        """ raise OperationalError(2006, MySQL server has gone away (%r)" % (e,)) """
        try:
            if self._unix_socket and self._host in ('localhost', '127.0.0.1'):
                self._reader, self._writer = yield from \
                    asyncio.open_unix_connection(self._unix_socket, loop=self._loop)
                self.host_info = "Localhost via UNIX socket: " + self._unix_socket
            else:
                self._reader, self._writer = yield from asyncio.open_connection(self._host, self._port, loop=self._loop)
                self.host_info = "socket %s:%d" % (self._host, self._port)
            if self._no_delay:
                self._set_nodelay(True)
            yield from self._get_server_information()
            yield from self._request_authentication()
            self.connected_time = self._loop.time()
            if self.sql_mode is not None:
                yield from self.query("SET sql_mode=%s" % (self.sql_mode,))
            if self.init_command is not None:
                yield from self.query(self.init_command)
                yield from self.commit()
            if self.autocommit_mode is not None:
                yield from self.autocommit(self.autocommit_mode)
        except Exception as e:
            if self._writer:
                self._writer.transport.close()
            self._reader = None
            self._writer = None
            raise OperationalError(2003, "Can't connect to MySQL server on %r" % self._host) from e

    def _set_nodelay(self, value):
        flag = int(bool(value))
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info('socket', default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, flag)
        transport.resume_reading()

    @asyncio.coroutine
    def _read_packet(self, packet_type=MysqlPacket):
        """Read an entire "mysql packet" in its entirety from the network
            and return a MysqlPacket type that represents the results."""
        buff = bytearray()
        try:
            while True:
                packet_header = yield from self._reader.readexactly(4)
                btrl, btrh, packet_number = struct.unpack('<HBB', packet_header) 
                bytes_to_read = btrl + (btrh << 16)
                # TODO: check sequence id
                recv_data = yield from self._reader.readexactly(bytes_to_read)
                buff += recv_data
                if bytes_to_read < MAX_PACKET_LEN:
                    break
        except (OSError, EOFError) as exc:
            msg = "MySQL server has gone away (%s)"
            raise OperationalError(2006, msg % (exc,)) from exc
        packet = packet_type(buff, self._encoding)
        packet.check_error()
        return packet

    def _write_bytes(self, data):
        return self._writer.write(data)

    @asyncio.coroutine
    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = MySQLResult(self)
                yield from result.init_unbuffered_query()
            except:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = MySQLResult(self)
            yield from result.read()
        self._result = result
        self._affected_rows = result.affected_rows
        if result.server_status is not None:
            self.server_status = result.server_status

    def insert_id(self):
        if self._result:
            return self._result.insert_id
        else:
            return 0

    @asyncio.coroutine
    def _execute_command(self, command, sql):
        if not self._writer:
            raise InterfaceError("(0, 'Not connected')")
        if self._result is not None and self._result.unbuffered_active:
            yield from self._result._finish_unbuffered_query()
        if isinstance(sql, str):
            sql = sql.encode(self._encoding)
        chunk_size = min(MAX_PACKET_LEN, len(sql) + 1)  # +1 is for command
        prelude = struct.pack('<i', chunk_size) + int2byte(command)
        self._write_bytes(prelude + sql[:chunk_size - 1])
        if chunk_size < MAX_PACKET_LEN:
            return
        seq_id = 1
        sql = sql[chunk_size - 1:]
        while True:
            chunk_size = min(MAX_PACKET_LEN, len(sql))
            prelude = struct.pack('<i', chunk_size)[:3]
            data = prelude + int2byte(seq_id % 256) + sql[:chunk_size]
            self._write_bytes(data)
            sql = sql[chunk_size:]
            if not sql and chunk_size < MAX_PACKET_LEN:
                break
            seq_id += 1

    @asyncio.coroutine
    def _request_authentication(self):
        self.client_flag |= CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= MULTI_RESULTS
        if self._user is None:
            raise ValueError("Did not specify a username")
        charset_id = charset_by_name(self._charset).id
        user = self._user
        if isinstance(self._user, str):
            user = self._user.encode(self._encoding)
        data_init = struct.pack('<iIB23s', self.client_flag, 1, charset_id, b'')
        next_packet = 1
        data = data_init + user + b'\0' + _scramble(
            self._password.encode('latin1'), self.salt)
        if self._db:
            db = self._db
            if isinstance(self._db, str):
                db = self._db.encode(self._encoding)
            data += db + int2byte(0)
        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2
        self._write_bytes(data)
        auth_packet = yield from self._read_packet()
        if auth_packet.is_eof_packet():
            data = _scramble_323(self._password.encode('latin1'),
                                 self.salt) + b'\0'
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            self._write_bytes(data)
            auth_packet = self._read_packet()

    def thread_id(self):
        return self.server_thread_id[0]

    def character_set_name(self):
        return self._charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self.protocol_version

    @asyncio.coroutine
    def _get_server_information(self):
        i = 0
        packet = yield from self._read_packet()
        data = packet.get_all_data()
        self.protocol_version = byte2int(data[i:i + 1])
        i += 1
        server_end = data.find(int2byte(0), i)
        self.server_version = data[i:server_end].decode('latin1')
        i = server_end + 1
        self.server_thread_id = struct.unpack('<I', data[i:i + 4])
        i += 4
        self.salt = data[i:i + 8]
        i += 9  # 8 + 1(filler)
        self.server_capabilities = struct.unpack('<H', data[i:i + 2])[0]
        i += 2
        if len(data) >= i + 6:
            lang, stat, cap_h, salt_len = struct.unpack('<BHHB', data[i:i + 6])
            i += 6
            self.server_language = lang
            self.server_charset = charset_by_id(lang).name
            self.server_status = stat
            self.server_capabilities |= cap_h << 16
            salt_len = max(12, salt_len - 9)
        i += 10
        if len(data) >= i + salt_len:
            # salt_len includes auth_plugin_data_part_1 and filler
            # TODO: AUTH PLUGIN NAME may appeare here.
            self.salt += data[i:i + salt_len]

    def get_transaction_status(self):
        return bool(self.server_status & SERVER_STATUS.SERVER_STATUS_IN_TRANS)

    def get_server_info(self):
        return self.server_version

    def __del__(self):
        if not self._writer:
            warnings.warn("Unclosed connection {!r}".format(self), ResourceWarning)
            self.close()


class MySQLResult(object):
    """ """
    def __init__(self, connection):
        self.connection = connection
        self.affected_rows = None
        self.insert_id = None
        self.server_status = None
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.rows = None
        self.has_next = None
        self.unbuffered_active = False

    # TODO: use classes for different packet types?
    @asyncio.coroutine
    def read(self):
        try:
            first_packet = yield from self.connection._read_packet()
            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            else:
                yield from self._read_result_packet(first_packet)
        finally:
            self.connection = None

    @asyncio.coroutine
    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = yield from self.connection._read_packet()
        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            yield from self._get_descriptions()
            self.affected_rows = 18446744073709551615  # Max val of 64bit int

    def _read_ok_packet(self, first_packet):
        ok_packet = OKPacketWrapper(first_packet)
        self.affected_rows = ok_packet.affected_rows
        self.insert_id = ok_packet.insert_id
        self.server_status = ok_packet.server_status
        self.warning_count = ok_packet.warning_count
        self.message = ok_packet.message
        self.has_next = ok_packet.has_next

    def _check_packet_is_eof(self, packet):
        if packet.is_eof_packet():
            eof_packet = EOFPacketWrapper(packet)
            self.warning_count = eof_packet.warning_count
            self.has_next = eof_packet.has_next
            return True
        return False

    @asyncio.coroutine
    def _read_result_packet(self, first_packet):
        self.field_count = first_packet.read_length_encoded_integer()
        yield from self._get_descriptions()
        yield from self._read_rowdata_packet()

    @asyncio.coroutine
    def _read_rowdata_packet_unbuffered(self):
        if not self.unbuffered_active:
            return
        packet = yield from self.connection._read_packet()
        if self._check_packet_is_eof(packet):
            self.unbuffered_active = False
            self.connection = None
            self.rows = None
            return
        row = self._read_row_from_packet(packet)
        self.affected_rows = 1
        self.rows = (row,)
        return row

    @asyncio.coroutine
    def _finish_unbuffered_query(self):
        # After much reading on the MySQL protocol, it appears that there is,
        # in fact, no way to stop MySQL from sending all the data after
        # executing a query, so we just spin, and wait for an EOF packet.
        while self.unbuffered_active:
            packet = yield from self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                self.unbuffered_active = False
                # release reference to kill cyclic reference.
                self.connection = None

    @asyncio.coroutine
    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        while True:
            packet = yield from self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                # release reference to kill cyclic reference.
                self.connection = None
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    def _read_row_from_packet(self, packet):
        use_unicode = self.connection.use_unicode
        row = []
        for field in self.fields:
            data = packet.read_length_coded_string()
            if data is not None:
                field_type = field.type_code
                if use_unicode:
                    if field_type in TEXT_TYPES:
                        charset = charset_by_id(field.charsetnr)
                        if use_unicode and not charset.is_binary:
                            data = data.decode(charset.encoding)
                    else:
                        data = data.decode()
                converter = self.connection.decoders.get(field_type)
                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

    @asyncio.coroutine
    def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        description = []
        for _ in range(self.field_count):
            field = yield from self.connection._read_packet(
                FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())

        eof_packet = yield from self.connection._read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)


class Pool(asyncio.AbstractServer):
    """ Connection Pool implements """
    def __init__(self, minsize, maxsize, echo, loop, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = deque(maxlen=maxsize)
        self._cond = asyncio.Condition(loop=loop)
        self._used = set()
        self._terminated = set()
        self._closing = False
        self._closed = False
        self._echo = echo

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    @asyncio.coroutine
    def clear(self):
        """ Close all free connections in pool """
        with (yield from self._cond):
            while self._free:
                conn = self._free.popleft()
                yield from conn.ensure_closed()
            self._cond.notify()

    def close(self):
        """ Close pool """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        """Terminate pool.
        Close pool with instantly closing all acquired connections also.
        """
        self.close()
        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)
        self._used.clear()

    @asyncio.coroutine
    def wait_closed(self):
        """Wait for closing all pool's connections."""
        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called after .close()")
        while self._free:
            conn = self._free.popleft()
            conn.close()
        with (yield from self._cond):
            while self.size > self.freesize:
                yield from self._cond.wait()

        self._closed = True

    @asyncio.coroutine
    def acquire(self):
        """Acquire free connection from the pool."""
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        with (yield from self._cond):
            while True:
                yield from self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    yield from self._cond.wait()

    @asyncio.coroutine
    def _fill_free_pool(self, override_min):
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = yield from _connect(echo=self._echo, loop=self._loop, **self._conn_kwargs)
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return
        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = yield from _connect(echo=self._echo, loop=self._loop, **self._conn_kwargs)
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    @asyncio.coroutine
    def _wakeup(self):
        with (yield from self._cond):
            self._cond.notify()

    def release(self, conn):
        """ Release free connection back to the connection pool, this is NOT a coroutine """
        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return
            if self._closing:
                conn.close()
            else:
                self._free.append(conn)
            asyncio.Task(self._wakeup(), loop=self._loop)

    def __enter__(self):
        raise RuntimeError('"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        """ This must exist because __enter__ exists, even though that
            always raises; that's how the with-statement works."""
        pass

    def __iter__(self):
        """ This is not a coroutine.  It is meant to enable the idiom:
            with (yield from pool) as conn:
                <block>
            as an alternative to:
            conn = yield from pool.acquire()
            try:     <block>
            finally: conn.release() """
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

@asyncio.coroutine
def create_pool(minsize=10, maxsize=10, echo=False, loop=None, **kwargs)->Pool:
    """ Pool factory """
    if loop is None:
        loop = asyncio.get_event_loop()
    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo, loop=loop, **kwargs)
    if minsize > 0:
        with (yield from pool._cond):
            yield from pool._fill_free_pool(False)
    return pool

class _ConnectionContextManager(object):
    __slots__ = ('_pool', '_conn')
    """ Context manager:
        with (yield from pool) as conn:
            cur = yield from conn.cursor()
        while failing loudly when accidentally using:
            with pool:
                <block> """
    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *args):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


def example():
    from getpass import getpass
    loop = asyncio.get_event_loop()
    @asyncio.coroutine
    def _user_work():
        pool = yield from create_pool(host='127.0.0.1', port=3306, 
                                      user=DEFAULT_USER, password=getpass(),
                                      db='mysql', loop=loop)
        with (yield from pool) as conn:
            cur = yield from conn.cursor()
            yield from cur.execute("SELECT 10")
            print(cur.description)
            (r,) = yield from cur.fetchone()
            print(r)
        pool.close()
        yield from pool.wait_closed()
    loop.run_until_complete(_user_work())


if __name__ == '__main__':
    example()




