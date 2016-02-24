#!/usr/bin/env python3
# -*- coding:utf-8 -*-
## author : cypro666
## date   : 2015.07.16
import struct
import asyncio
from collections import defaultdict
import bson
from bson import SON, ObjectId, Code
from pymongo import errors, auth

_ONE = b"\x01\x00\x00\x00"
_ZERO = b"\x00\x00\x00\x00"


class _Query(object):
    __slots__ = ('id', 'limit', 'collection', 'documents', 'future')
    def __init__(self, id_, collection, limit):
        """ init won't check the limit arg validate or not """
        self.id = id_
        self.limit = limit
        self.collection = collection
        self.documents = []
        self.future = asyncio.Future()


class _Protocol(asyncio.Protocol):
    __slots__ = ('__id', '__buffer', '__queries', '__datalen', '__response', 
                 '__waiting_header', '_pipelined_calls', 'transport', '_is_connected')
    def __init__(self):
        self.__id = 0
        self.__buffer = b""
        self.__queries = {}
        self.__datalen = None
        self.__response = 0
        self.__waiting_header = True
        self._pipelined_calls = set() # Set of all the pipelined calls.
        self.transport = None
        self._is_connected = False

    def connection_made(self, transport):
        self.transport = transport
        self._is_connected = True

    def connection_lost(self, exc):
        self._is_connected = False
        self.transport = None

        # Raise exception on all waiting futures.
        for f in self.__queries:
            f.set_exception(errors.ConnectionFailure(exc))

    @property
    def is_connected(self):
        """ True when the underlying transport is connected. """
        return self._is_connected

    def data_received(self, data):
        while self.__waiting_header:
            self.__buffer += data
            if len(self.__buffer) < 16:
                break

            # got full header, 16 bytes (or more)
            header, extra = self.__buffer[:16], self.__buffer[16:]
            self.__buffer = b""
            self.__waiting_header = False
            datalen, request, response, operation = struct.unpack("<iiii", header)
            self.__datalen = datalen - 16
            self.__response = response
            if extra:
                self.data_received(extra)
            break
        else:
            if self.__datalen is not None:
                data, extra = data[:self.__datalen], data[self.__datalen:]
                self.__datalen -= len(data)
            else:
                extra = b""

            self.__buffer += data
            if self.__datalen == 0:
                self.message_received(self.__response, self.__buffer)
                self.__datalen = None
                self.__waiting_header = True
                self.__buffer = b""
                if extra:
                    self.data_received(extra)
    

    def message_received(self, request_id, packet):
        """ Response Flags:
            bit 0:    Cursor Not Found
            bit 1:    Query Failure
            bit 2:    Shard Config Stale
            bit 3:    Await Capable
            bit 4-31: Reserved """
        QUERY_FAILURE = 1 << 1
        response_flag, cursor_id, start, length = struct.unpack("<iqii", packet[:20])
        if response_flag == QUERY_FAILURE:
            self.query_failure(request_id, cursor_id, response_flag,  bson.BSON(packet[20:]).decode())
            return
        self.query_success(request_id, cursor_id, bson.decode_all(packet[20:]))

    def send_message(self, operation, collection, message, query_opts=_ZERO):
        #print "sending %d to %s" % (operation, self)
        fullname = collection and bson._make_c_string(collection) or b""
        message = query_opts + fullname + message
        # 16 is the size of the header in bytes
        header = struct.pack("<iiii", 16 + len(message), self.__id, 0, operation)
        self.transport.write(header + message)
        self.__id += 1

    def OP_INSERT(self, collection, docs):
        docs = [bson.BSON.encode(doc) for doc in docs]
        self.send_message(2002, collection, b"".join(docs))

    def OP_UPDATE(self, collection, spec, document, upsert=False, multi=False):
        options = 0
        if upsert:
            options += 1
        if multi:
            options += 2

        message = struct.pack("<i", options) + \
            bson.BSON.encode(spec) + bson.BSON.encode(document)
        self.send_message(2001, collection, message)

    def OP_DELETE(self, collection, spec):
        self.send_message(2006, collection, _ZERO + bson.BSON.encode(spec))

    def OP_KILL_CURSORS(self, cursors):
        message = struct.pack("<i", len(cursors))
        for cursor_id in cursors:
            message += struct.pack("<q", cursor_id)
        self.send_message(2007, None, message)

    def OP_GET_MORE(self, collection, limit, cursor_id):
        message = struct.pack("<iq", limit, cursor_id)
        self.send_message(2005, collection, message)

    def OP_QUERY(self, collection, spec, skip, limit, fields=None):
        message = struct.pack("<ii", skip, limit) + bson.BSON.encode(spec)
        if fields:
            message += bson.BSON.encode(fields)

        query = _Query(self.__id, collection, limit)
        self.__queries[self.__id] = query
        self.send_message(2004, collection, message)
        return query.future

    def query_failure(self, request_id, cursor_id, response, raw_error):
        query = self.__queries.pop(request_id, None)
        if query:
            query.future.set_exception(ValueError("mongo error=%s" % repr(raw_error)))
            del query

    def query_success(self, request_id, cursor_id, documents):
        try:
            query = self.__queries.pop(request_id)
        except KeyError:
            return
        if isinstance(documents, list):
            query.documents += documents
        else:
            query.documents.append(documents)
        if cursor_id:
            query.id = self.__id
            next_batch = 0
            if query.limit:
                next_batch = query.limit - len(query.documents)
                # Assert, because according to the protocol spec and my observations
                # there should be no problems with this, but who knows? At least it will
                # be noticed, if something unexpected happens. And it is definitely
                # better, than silently returning a wrong number of documents
                assert next_batch >= 0, "Unexpected number of documents received!"
                if not next_batch:
                    self.OP_KILL_CURSORS([cursor_id])
                    query.future.set_result(query.documents)
                    return
            self.__queries[self.__id] = query
            self.OP_GET_MORE(query.collection, next_batch, cursor_id)
        else:
            query.future.set_result(query.documents)


def _DIRECTION(keys, direction):
    if isinstance(keys, str):
        return (keys, direction),
    elif isinstance(keys, (list, tuple)):
        return tuple([(k, direction) for k in keys])

def ASCENDING(keys):
    """Ascending sort order"""
    return _DIRECTION(keys, 1)


def DESCENDING(keys):
    """Descending sort order"""
    return _DIRECTION(keys, -1)


def GEO2D(keys):
    """
    Two-dimensional geospatial index
    """
    return _DIRECTION(keys, "2d")


def GEOHAYSTACK(keys):
    """
    Bucket-based geospatial index
    """
    return _DIRECTION(keys, "geoHaystack")


class _QueryFilter(defaultdict):
    def __init__(self):
        defaultdict.__init__(self, lambda:())

    def __add__(self, obj):
        for k, v in obj.items():
            if isinstance(v, tuple):
                self[k] += v
            else:
                self[k] = v
        return self

    def _index_document(self, operation, index_list):
        name = self.__class__.__name__
        try:
            assert isinstance(index_list, (list, tuple))
            for key, direction in index_list:
                if not isinstance(key, str):
                    raise TypeError("Invalid %sing key: %s" % (name, repr(key)))
                if direction not in (1, -1, "2d", "geoHaystack"):
                    raise TypeError("Invalid %sing direction: %s" % (name, direction))
                self[operation] += tuple(((key, direction),))
        except Exception:
            raise TypeError("Invalid list of keys for %s: %s" % (name, repr(index_list)))

    def __repr__(self):
        return "<mongodb QueryFilter: %s>" % dict.__repr__(self)


class _Sort(_QueryFilter):
    """Sorts the results of a query."""

    def __init__(self, key_list):
        _QueryFilter.__init__(self)
        try:
            assert isinstance(key_list[0], (list, tuple))
        except:
            key_list = (key_list,)
        self._index_document("orderby", key_list)


class _Hint(_QueryFilter):
    """Adds a `hint`, telling Mongo the proper index to use for the query."""

    def __init__(self, index_list):
        _QueryFilter.__init__(self)
        try:
            assert isinstance(index_list[0], (list, tuple))
        except:
            index_list = (index_list,)
        self._index_document("$hint", index_list)


class _Explain(_QueryFilter):
    """Returns an explain plan for the query."""

    def __init__(self):
        _QueryFilter.__init__(self)
        self["explain"] = True


class _Snapshot(_QueryFilter):
    def __init__(self):
        _QueryFilter.__init__(self)
        self["snapshot"] = True



class Collection(object):
    """ Wrapper of all operations on mongo collections """
    def __init__(self, database, name):
        if not isinstance(name, str):
            raise TypeError("name must be an instance of str")
        if not name or ".." in name:
            raise errors.InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith("oplog.$main") or
                                name.startswith("$cmd")):
            raise errors.InvalidName("collection names must not contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise errors.InvalidName("collection names must not start or end with '.': %r" % name)
        if "\x00" in name:
            raise errors.InvalidName("collection names must not contain the null character")

        self._database = database
        self._collection_name = name

    def __str__(self):
        return "%s.%s" % (str(self._database), self._collection_name)

    def __repr__(self):
        return "<mongodb Collection: %s>" % str(self)

    def __getitem__(self, collection_name):
        return Collection(self._database, "%s.%s" % (self._collection_name, collection_name))

    def __eq__(self, other):
        if isinstance(other, Collection):
            return (self._database, self._collection_name) == (other._database, other._collection_name)
        return NotImplemented

    def __hash__(self):
        return self._collection_name.__hash__()

    def __getattr__(self, collection_name):
        return self[collection_name]

    def __call__(self, collection_name):
        return self[collection_name]

    def _fields_list_to_dict(self, fields):
        """
        transform a list of fields from ["a", "b"] to {"a":1, "b":1}
        """
        as_dict = {}
        for field in fields:
            if not isinstance(field, str):
                raise TypeError("fields must be a list of key names")
            as_dict[field] = 1
        return as_dict

    def _gen_index_name(self, keys):
        return u"_".join([u"%s_%s" % item for item in keys])

    @asyncio.coroutine
    def options(self):
        result = yield from self._database.system.namespaces.find_one({"name": str(self)})
        if result:
            options = result.get("options", {})
            if "create" in options:
                del options["create"]
            return options
        return {}

    @asyncio.coroutine
    def find(self, spec=None, skip=0, limit=0, fields=None, filter=None, _proto=None):
        if spec is None:
            spec = SON()

        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if fields is not None and not isinstance(fields, (dict, list)):
            raise TypeError("fields must be an instance of dict or list")
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")

        if fields is not None:
            if not isinstance(fields, dict):
                if not fields:
                    fields = ["_id"]
                fields = self._fields_list_to_dict(fields)

        if isinstance(filter, (_Sort, _Hint, _Explain, _Snapshot)):
            spec = SON(dict(query=spec))
            for k, v in filter.items():
                spec[k] = isinstance(v, tuple) and SON(v) or v

        # send the command through a specific connection
        # this is required for the connection pool to work
        # when safe=True
        if _proto is None:
            proto = self._database._protocol
        else:
            proto = _proto
        return (yield from proto.OP_QUERY(str(self), spec, skip, limit, fields))

    @asyncio.coroutine
    def find_one(self, spec=None, fields=None, _proto=None):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))

        docs = yield from self.find(spec, limit=-1, fields=fields, _proto=_proto)
        doc = docs and docs[0] or {}
        if doc.get("err") is not None:
            if doc.get("code") == 11000:
                raise errors.DuplicateKeyError
            else:
                raise errors.OperationFailure(doc)
        else:
            return doc

    @asyncio.coroutine
    def count(self, spec=None, fields=None):
        if fields is not None:
            if not fields:
                fields = ["_id"]
            fields = self._fields_list_to_dict(fields)
        spec = SON([("count", self._collection_name),
                    ("query", spec or SON()),
                    ("fields", fields)])
        result = yield from self._database["$cmd"].find_one(spec)
        return result["n"]

    @asyncio.coroutine
    def group(self, keys, initial, reduce, condition=None, finalize=None):
        body = {
            "ns": self._collection_name,
            "key": self._fields_list_to_dict(keys),
            "initial": initial,
            "$reduce": Code(reduce),
        }
        if condition:
            body["cond"] = condition
        if finalize:
            body["finalize"] = Code(finalize)

        return (yield from self._database["$cmd"].find_one({"group": body}))

    @asyncio.coroutine
    def filemd5(self, spec):
        if not isinstance(spec, ObjectId):
            raise ValueError(_("filemd5 expected an objectid for its "
                               "on-keyword argument"))

        spec = SON([("filemd5", spec),
                    ("root", self._collection_name)])

        result = yield from self._database['$cmd'].find_one(spec)
        return result.get('md5')

    @asyncio.coroutine
    def __safe_operation(self, proto, safe=False, ids=None):
        callit = False
        result = None
        if safe is True:
            result = yield from self._database["$cmd"].find_one({"getlasterror": 1}, _proto=proto)
        else:
            callit = True
        if ids is not None:
            return ids
        if callit is True:
            return None
        return result

    @asyncio.coroutine
    def insert(self, docs, safe=False):
        if isinstance(docs, dict):
            ids = docs.get('_id', ObjectId())
            docs["_id"] = ids
            docs = [docs]
        elif isinstance(docs, list):
            ids = []
            for doc in docs:
                if isinstance(doc, dict):
                    id = doc.get('_id', ObjectId())
                    ids.append(id)
                    doc["_id"] = id
                else:
                    raise TypeError("insert takes a document or a list of documents")
        else:
            raise TypeError("insert takes a document or a list of documents")
        proto = self._database._protocol
        proto.OP_INSERT(str(self), docs)
        result = yield from self.__safe_operation(proto, safe, ids)
        return result

    @asyncio.coroutine
    def update(self, spec, document, upsert=False, multi=False, safe=False):
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict")
        if not isinstance(document, dict):
            raise TypeError("document must be an instance of dict")
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be an instance of bool")
        proto = self._database._protocol
        proto.OP_UPDATE(str(self), spec, document, upsert, multi)
        return (yield from self.__safe_operation(proto, safe))

    @asyncio.coroutine
    def save(self, doc, safe=False):
        if not isinstance(doc, dict):
            raise TypeError("cannot save objects of type %s" % type(doc))

        objid = doc.get("_id")
        if objid:
            return (yield from self.update({"_id": objid}, doc, safe=safe, upsert=True))
        else:
            return (yield from self.insert(doc, safe=safe))

    @asyncio.coroutine
    def remove(self, spec, safe=False):
        if isinstance(spec, ObjectId):
            spec = SON(dict(_id=spec))
        if not isinstance(spec, dict):
            raise TypeError("spec must be an instance of dict, not %s" % type(spec))

        proto = self._database._protocol
        proto.OP_DELETE(str(self), spec)
        return (yield from self.__safe_operation(proto, safe))

    @asyncio.coroutine
    def drop(self, safe=False):
        return (yield from self.remove({}, safe))

    @asyncio.coroutine
    def create_index(self, sort_fields, **kwargs):
        if not isinstance(sort_fields, _Sort):
            raise TypeError("sort_fields must be an instance of filter.sort")
        if "name" not in kwargs:
            name = self._gen_index_name(sort_fields["orderby"])
        else:
            name = kwargs.pop("name")

        key = SON()
        for k,v in sort_fields["orderby"]:
            key.update({k:v})

        index = SON(dict( ns=str(self), name=name, key=key))

        if "drop_dups" in kwargs:
            kwargs["dropDups"] = kwargs.pop("drop_dups")

        if "bucket_size" in kwargs:
            kwargs["bucketSize"] = kwargs.pop("bucket_size")
        
        index.update(kwargs)
        yield from self._database.system.indexes.insert(index, safe=True)
        return name

    @asyncio.coroutine
    def ensure_index(self, sort_fields, **kwargs):
        # ensure_index is an alias of create_index since we are not 
        # keep an index cache same way pymongo does
        return (yield from self.create_index(sort_fields, **kwargs))

    @asyncio.coroutine
    def drop_index(self, index_identifier):
        if isinstance(index_identifier, str):
            name = index_identifier
        elif isinstance(index_identifier, _Sort):
            name = self._gen_index_name(index_identifier["orderby"])
        else:
            raise TypeError("index_identifier must be a name or instance of filter.sort")

        cmd = SON([("deleteIndexes", self._collection_name), ("index", name)])
        return (yield from self._database["$cmd"].find_one(cmd))

    @asyncio.coroutine
    def drop_indexes(self):
        return (yield from self.drop_index("*"))

    @asyncio.coroutine
    def index_information(self):
        raw = yield from self._database.system.indexes.find({"ns": str(self)})
        info = {}
        for idx in raw:
            info[idx["name"]] = idx["key"].items()
        return info

    @asyncio.coroutine
    def rename(self, new_name):
        cmd = SON([("renameCollection", str(self)), ("to", "%s.%s" % \
            (str(self._database), new_name))])
        return (yield from self._database("admin")["$cmd"].find_one(cmd))

    @asyncio.coroutine
    def distinct(self, key, spec=None):
        cmd = SON([("distinct", self._collection_name), ("key", key)])
        if spec:
            cmd["query"] = spec

        result = yield from self._database["$cmd"].find_one(cmd)
        if result:
            return result.get("values")
        return {}

    @asyncio.coroutine
    def aggregate(self, pipeline, full_response=False):
        """ not stable yet """
        cmd = SON([("aggregate", self._collection_name),
                   ("pipeline", pipeline)])

        result = yield from self._database["$cmd"].find_one(cmd)
        if full_response:
            return result
        return result.get("result")

    @asyncio.coroutine
    def map_reduce(self, map, reduce, full_response=False, **kwargs):
        """ not stable yet """
        cmd = SON([("mapreduce", self._collection_name), ("map", map), ("reduce", reduce)])
        cmd.update(**kwargs)
        result = yield from self._database["$cmd"].find_one(cmd)
        if full_response:
            return result
        return result.get("result")

    @asyncio.coroutine
    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
        if not update and not kwargs.get('remove', None):
            raise ValueError("Must either update or remove")
        if update and kwargs.get('remove', None):
            raise ValueError("Can't do both update and remove")

        cmd = SON([("findAndModify", self._collection_name)])
        cmd.update(kwargs)
        # No need to include empty args
        if query:
            cmd['query'] = query
        if update:
            cmd['update'] = update
        if upsert:
            cmd['upsert'] = upsert

        result = yield from self._database["$cmd"].find_one(cmd)
        no_obj_error = "No matching object found"
        if not result['ok']:
            if result["errmsg"] == no_obj_error:
                return None
            else:
                raise ValueError("Unexpected Error: %s" % (result,))
        return result.get('value')


class Database(object):
    def __init__(self, protocol, database_name):
        self.__protocol = protocol
        self._database_name = database_name

    def __str__(self):
        return self._database_name

    def __repr__(self):
        return "<mongodb Database: %s>" % self._database_name

    def __call__(self, database_name):
        return Database(self.__protocol, database_name)

    def __getitem__(self, collection_name):
        return Collection(self, collection_name)

    def __getattr__(self, collection_name):
        return self[collection_name]

    @property
    def _protocol(self):
        return self.__protocol

    @asyncio.coroutine
    def create_collection(self, name, options=None):
        collection = Collection(self, name)

        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            command = SON({"create": name})
            command.update(options)
            result = yield from self["$cmd"].find_one(command)
            if result.get("ok", 0.0):
                return collection
            else:
                raise RuntimeError(result.get("errmsg", "unknown error"))
        else:
            return collection

    @asyncio.coroutine
    def drop_collection(self, name_or_collection):
        if isinstance(name_or_collection, Collection):
            name = name_or_collection._collection_name
        elif isinstance(name_or_collection, str):
            name = name_or_collection
        else:
            raise TypeError("name must be an instance of basestring or txmongo.Collection")

        return self["$cmd"].find_one({"drop": name})

    @asyncio.coroutine
    def collection_names(self):
        results = yield from self["system.namespaces"].find()
        names = [r["name"] for r in results]
        names = [n[len(str(self)) + 1:] for n in names
                 if n.startswith(str(self) + ".")]
        names = [n for n in names if "$" not in n]
        return names

    @asyncio.coroutine
    def authenticate(self, name, password):
        """
        Send an authentication command for this database.
        mostly stolen from asyncio_mongo._pymongo
        """
        if not isinstance(name, str):
            raise TypeError("name must be an instance of str")
        if not isinstance(password, str):
            raise TypeError("password must be an instance of str")
        # First get the nonce
        result = yield from self["$cmd"].find_one({"getnonce": 1})
        return (yield from self.authenticate_with_nonce(result, name, password))

    @asyncio.coroutine
    def authenticate_with_nonce(self, result, name, password):
        nonce = result['nonce']
        key = auth._auth_key(nonce, name, password)
        # hacky because order matters
        auth_command = SON(authenticate=1)
        auth_command['user'] = name
        auth_command['nonce'] = nonce
        auth_command['key'] = key
        # Now actually authenticate
        result = yield from self["$cmd"].find_one(auth_command)
        return self.authenticated(result)

    @asyncio.coroutine
    def authenticated(self, result):
        """might want to just call callback with 0.0 instead of errback"""
        ok = result['ok']
        if ok:
            return ok
        else:
            raise errors.PyMongoError(result['errmsg'])


class Connection(object):
    """
    Wrapper around the protocol and transport which takes care of establishing
    the connection and reconnecting it.
    connection = yield from Connection.create(host='localhost', port=6379)
    result = yield from connection.set('key', 'value')
    """
    protocol = _Protocol
    """
    The :class:`_Protocol` class to be used this connection.
    """

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=27017, loop=None, auto_reconnect=False):
        connection = cls()
        connection.host = host
        connection.port = port
        connection._loop = loop
        connection._retry_interval = .5
        # Create protocol instance
        protocol_factory = type('_Protocol', (cls.protocol,), {})
        if auto_reconnect:
            class protocol_factory(protocol_factory):
                def connection_lost(self, exc):
                    super().connection_lost(exc)
                    asyncio.Task(connection._reconnect())

        connection.protocol = protocol_factory()
        # Connect
        yield from connection._reconnect()
        return connection

    @asyncio.coroutine
    def disconnect(self):
        if self.transport:
            return self.transport.close()

    @property
    def transport(self):
        """ The transport instance that the protocol is currently using. """
        return self.protocol.transport

    def _get_retry_interval(self):
        """ Time to wait for a reconnect in seconds. """
        return self._retry_interval

    def _reset_retry_interval(self):
        """ Set the initial retry interval. """
        self._retry_interval = .5

    def _increase_retry_interval(self):
        """ When a connection failed. Increase the interval."""
        self._retry_interval = min(60, 1.5 * self._retry_interval)

    def _reconnect(self):
        """
        Set up Mongo connection.
        """
        loop = self._loop or asyncio.get_event_loop()
        while True:
            try:
                # print('connecting...')
                yield from loop.create_connection(lambda: self.protocol, self.host, self.port)
                self._reset_retry_interval()
                return
            except OSError:
                # Sleep and try again
                self._increase_retry_interval()
                interval = self._get_retry_interval()
                print('Connecting to mongo failed. Retrying in %i seconds' % interval)
                yield from asyncio.sleep(interval)

    def __getitem__(self, database_name):
        return Database(self.protocol, database_name)

    def __getattr__(self, database_name):
        return self[database_name]

    def __repr__(self):
        return 'Connection(host=%r, port=%r)' % (self.host, self.port)



class Pool(object):
    """
    Pool of connections. Each Takes care of setting up the connection and connection pooling.
    When pool_size > 1 and some connections are in use because of 
    transactions or blocking requests, the other are preferred.
    pool = yield from Pool.create(host='localhost', port=6379, pool_size=10)
    result = yield from connection.set('key', 'value')
    """

    protocol = _Protocol
    """ The :class:`_Protocol` class to be used for each connection in this pool. """

    @classmethod
    def get_connection_class(cls):
        """ Return the :class:`Connection` class to be used for every connection in
            this pool. Normally this is just a ``Connection`` using the defined ``protocol``
        """
        class ConnectionClass(Connection):
            protocol = cls.protocol
        return ConnectionClass

    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=27017, loop=None, poolsize=1, auto_reconnect=True):
        """ Create a new pool instance. """
        self = cls()
        self._host = host
        self._port = port
        self._pool_size = poolsize

        # Create connections
        self._connections = []

        for i in range(poolsize):
            connection_class = cls.get_connection_class()
            connection = yield from connection_class.create(host=host, port=port, loop=loop,
                                                            auto_reconnect=auto_reconnect)
            self._connections.append(connection)

        return self

    def __repr__(self):
        return 'Pool(host=%r, port=%r, pool_size=%r)' % (self._host, self._port, self._poolsize)

    @property
    def pool_size(self):
        """ Number of parallel connections in the pool."""
        return self._poolsize

    @property
    def connections_connected(self):
        """
        The amount of open TCP connections.
        """
        return sum([1 for c in self._connections if c.protocol.is_connected])

    def close(self):
        for conn in self._connections:
            conn.disconnect()

    def _get_free_connection(self):
        """
        Return the next protocol instance that's not in use.
        (A protocol in pubsub mode or doing a blocking request is considered busy,
        and can't be used for anything else.)
        """
        self._shuffle_connections()
        for c in self._connections:
            if c.protocol.is_connected:
                return c

    def _shuffle_connections(self):
        """
        'shuffle' protocols. Make sure that we divide the load equally among the protocols.
        """
        self._connections = self._connections[1:] + self._connections[:1]

    def __getattr__(self, name):
        """
        Proxy to a protocol. (This will choose a protocol instance that's not
        busy in a blocking request or transaction.)
        """
        if 'close' == name:
            return self.close
        connection = self._get_free_connection()

        if connection:
            return getattr(connection, name)
        else:
            raise errors.PyMongoError('No available connections in the pool: size=%s, connected=%s' % 
                                      (self.pool_size, self.connections_connected))
        return None



@asyncio.coroutine
def test():
    from pprint import pprint
    mc = yield from Connection.create(host='127.0.0.1', port=27017)
    doc = yield from mc.local.startup_log.find_one()
    pprint(doc)
    yield from mc.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())




