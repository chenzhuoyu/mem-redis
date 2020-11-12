#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import heapq
import signal
import fnmatch
import asyncio
import logging

from typing import Any
from typing import Set
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union
from typing import Callable
from typing import Iterable
from typing import Optional

from asyncio import StreamReader
from asyncio import StreamWriter
from asyncio import BaseEventLoop
from asyncio import IncompleteReadError

### Protocol Implementations ###

Value = Union[
    int,
    str,
    Optional[bytes],
    Iterable['Value'],
]

WriterFunction = Callable[
    [Value],
    None,
]

class Error:
    msg  : str
    kind : Optional[str]

    def __init__(self, msg: str, kind: Optional[str] = 'ERR'):
        self.msg  = msg
        self.kind = kind

    @property
    def error(self) -> str:
        if self.kind is None:
            return self.msg
        else:
            return self.kind + ' ' + self.msg

async def _read(rd: StreamReader) -> List[bytes]:
    ret = []
    size = await _read_size(rd, b'*')

    # check for size
    if size < 0:
        raise SyntaxError('negative array size')

    # read every element
    for _ in range(size):
        ret.append(await _read_binary(rd))
    else:
        return ret

async def _read_size(rd: StreamReader, tag: bytes) -> int:
    if await rd.readexactly(1) != tag:
        raise SyntaxError('invalid header type')
    else:
        return int((await rd.readline()).strip())

async def _read_binary(rd: StreamReader) -> Optional[bytes]:
    rv = None
    nb = await _read_size(rd, b'$')

    # negative values other than `-1` are invalid
    if nb < -1:
        raise SyntaxError('invalid bulk string length: %d' % nb)

    # special case for `Null`
    if nb == -1:
        return rv

    # read the blob
    rv = await rd.readexactly(nb)
    el = await rd.readline()

    # must be no more than `nb` bytes
    if el != b'\r\n':
        raise SyntaxError('%d more bytes than expected' % (len(el) - nb - 2))
    else:
        return rv

async def _write(wr: StreamWriter, val: Value):
    _write_value(wr, val)
    await wr.drain()

def _write_value(wr: StreamWriter, val: Value):
    if val is None:
        wr.write(b'$-1\r\n')
    elif isinstance(val, int):
        wr.write(b':%d\r\n' % val)
    elif isinstance(val, str):
        wr.write(b'+%s\r\n' % val.encode('utf-8'))
    elif isinstance(val, bytes):
        wr.write(b'$%d\r\n%s\r\n' % (len(val), val))
    elif isinstance(val, Error):
        wr.write(b'-%s\r\n' % val.error.encode('utf-8'))
    else:
        try:
            it = iter(val)
            wr.write(b'*%d\r\n' % len(val))
        except TypeError:
            raise TypeError('invalid value type: ' + type(val).__name__)
        else:
            for vv in it:
                _write_value(wr, vv)

class Redis:
    ctx  : Any
    log  : logging.Logger
    bind : str
    port : int
    loop : Optional[BaseEventLoop]
    cmds : Dict[str, 'RedisCommand']

    def __init__(self, bind: str, port: int, cmds: List['RedisCommand'], ctx: Any):
        self.ctx  = ctx
        self.log  = logging.getLogger('redism')
        self.bind = bind
        self.port = port
        self.loop = None
        self.cmds = {c.name.upper(): c() for c in cmds}

    def run(self):
        loop = asyncio.new_event_loop()
        psrv = loop.run_until_complete(asyncio.start_server(self._handle_conn, self.bind, self.port, loop = loop))

        # log the server start event
        self.loop = loop
        self.log.info('Mem Redis started on %s:%d' % (self.bind, self.port))

        # start the server
        try:
            loop.run_forever()
        finally:
            psrv.close()
            loop.run_until_complete(psrv.wait_closed())
            loop.close()

    def stop(self):
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self._stop_server)

    def _stop_server(self):
        for task in asyncio.all_tasks(loop = self.loop):
            task.cancel()
        else:
            self.loop.call_soon(self.loop.stop)

    def _close_writer(self, wr: StreamWriter):
        try:
            wr.close()
        except RuntimeError:
            pass

    async def _handle_conn(self, rd: StreamReader, wr: StreamWriter):
        try:
            while True:
                await self._handle_request(await _read(rd), wr)
        except IncompleteReadError as e:
            if e.partial:
                self.log.warn('Unexpected EOF from client.')
        except SyntaxError as e:
            self.log.error('Invalid client command: %s.' % e.msg)
        except Exception as e:
            self.log.exception('Errors occured when handling requests: ' + str(e))
        finally:
            self._close_writer(wr)

    async def _handle_request(self, req: List[bytes], wr: StreamWriter):
        if not req:
            raise SyntaxError('empty command')
        else:
            await self._handle_command(req[0], req[1:], wr)

    async def _handle_command(self, cmd: bytes, args: List[bytes], wr: StreamWriter):
        cmds = cmd.decode('utf-8')
        cmdc = self.cmds.get(cmds.upper())

        # writer function
        async def write_func(v: Value):
            await _write(wr, v)

        # check for commands
        if cmdc is None:
            print(cmds)
            await _write(wr, Error('unknown command `%s`' % cmds))
        else:
            await cmdc.handle(self.ctx, args, write_func)

### Redis Command Interface ###

class ClassProperty:
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, obj, cls = None):
        return self.fget.__get__(obj, cls or type(obj))()

def clsproperty(method):
    return ClassProperty(classmethod(method))

class RedisCommand:
    @clsproperty
    def name(self) -> str:
        raise NotImplementedError('not implemented: RedisCommand.name')

    @clsproperty
    def arity(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.arity')

    @clsproperty
    def flags(self) -> List[str]:
        raise NotImplementedError('not implemented: RedisCommand.flags')

    @clsproperty
    def key_end(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.key_end')

    @clsproperty
    def key_step(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.key_step')

    @clsproperty
    def key_begin(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.key_begin')

    @classmethod
    def descriptor(cls) -> (bytes, int, List[str], int, int, int):
        return (
            cls.name.lower().encode('utf-8'),
            cls.arity,
            cls.flags,
            cls.key_begin,
            cls.key_end,
            cls.key_step,
        )

    async def handle(self, ctx: Any, args: List[bytes], write: WriterFunction):
        raise NotImplementedError('not implemented: RedisCommand.handle')

del clsproperty
del ClassProperty

### Storage Implementations ###

MemValue = Union[
    bytes,
    Set[bytes],
    List[bytes],
    Dict[bytes, bytes],
]

class Node:
    ttl: int
    key: bytes
    val: MemValue

    def __init__(self, ttl: int, key: bytes, val: MemValue):
        self.ttl = ttl
        self.key = key
        self.val = val

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, Node):
            raise TypeError("'<' not supported between instances of 'Node' and '%s'" % type(other).__name__)
        else:
            return 0 <= self.ttl < other.ttl

    def __repr__(self) -> str:
        return '{NODE:%s:%d}' % (self.key, self.ttl)

class Storage:
    heap: List[Node]
    data: Dict[bytes, Node]

    def __init__(self):
        self.heap = []
        self.data = {}

    @property
    def keys(self) -> Iterable[bytes]:
        return self.data.keys()

    def get(self, key: bytes) -> Optional[Node]:
        return self.data.get(key)

    def put(self, node: Node):
        self.data[node.key] = node
        self.expire(node)

    def drop(self, keys: List[bytes]) -> int:
        return sum(bool(self.data.pop(k, None)) for k in keys)

    def sweep(self):
        while self.heap and time.time_ns() >= self.heap[0].ttl:
            prev = heapq.heappop(self.heap)
            curr = self.data.get(prev.key)

            # nodes may be replaced by another node with the same key
            if prev is curr:
                del self.data[prev.key]

    def expire(self, node: Node):
        if node.ttl >= 0:
            heapq.heappush(self.heap, node)

### Abstract Redis Command ###

class AbstractRedisCommand(RedisCommand):
    async def handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        try:
            await self._handle(ctx, args, write)
        finally:
            ctx.sweep()

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        raise NotImplementedError('not implemented: AbstractRedisCommand._handle')

### String Commands ###

class DelCommand(AbstractRedisCommand):
    name      = 'DEL'
    arity     = -2
    flags     = ['write']
    key_begin = 1
    key_end   = -1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if not args:
            await write(Error('incorrect number of arguments for command'))
        else:
            await write(ctx.drop(args))

class GetCommand(AbstractRedisCommand):
    name      = 'GET'
    arity     = 2
    flags     = ['readonly', 'fast']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 1:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._write_node(ctx.get(args[0]), write)

    async def _write_node(self, val: Optional[Node], write: WriterFunction):
        if val is None:
            await write(None)
        elif isinstance(val.val, bytes):
            await write(val.val)
        else:
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))

class SetCommand(AbstractRedisCommand):
    name      = 'SET'
    arity     = -3
    flags     = ['write', 'denyoom']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    ExpOpt  = Union[None, bool, int]
    CondOpt = Optional[bool]

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) < 2:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_set(ctx, args[0], args[1], args[2:], write)

    async def _handle_set(self, ctx: Storage, key: bytes, val: bytes, args: List[bytes], write: WriterFunction):
        try:
            ttl, args = self._parse_ttl(args)
            cond, args = self._parse_cond(args)
        except ValueError:
            await write(Error('value is not an integer or out of range'))
        else:
            if not args:
                await self._handle_setp(ctx, key, val, ttl, cond, False, write)
            elif len(args) == 1 and args[0].upper() == b'GET':
                await self._handle_setp(ctx, key, val, ttl, cond, True, write)
            else:
                await write(Error('incorrect number of arguments for command'))

    async def _handle_setp(self, ctx: Storage, key: bytes, val: bytes, ttl: ExpOpt, cond: CondOpt, get: bool, write: WriterFunction):
        old = ctx.get(key)
        now = time.time_ns()

        # check for value type when `GET` is present
        if get and old is not None and not isinstance(old.val, bytes):
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))
            return

        # check for 'NX' or 'EX' options
        if cond is not None:
            if cond is not (old == None):
                await write((old and old.val) if get else 'OK')
                return

        # no ttl specified
        if ttl is None:
            put = True
            new = Node(-1, key, val)

        # 'EX' or 'PX' is set
        elif ttl is not True:
            put = True
            new = Node(now + ttl, key, val)

        # 'KEEPTTL' while the old value exists
        elif old is not None:
            new = old
            put = False

        # 'KEEPTTL' but the old value doesn't exist
        else:
            put = True
            new = Node(-1, key, val)

        # put or update the node
        if put:
            ctx.put(new)
        else:
            new.val = val

        # return the old value if needed
        if not get:
            await write('OK')
        elif old is None:
            await write(None)
        else:
            await write(old.val)

    def _parse_ttl(self, args: List[bytes]) -> (ExpOpt, List[bytes]):
        if not args:
            return None, []
        else:
            return self._parse_ttl_mode(args[0].upper(), args)

    def _parse_ttl_mode(self, mode: bytes, args: List[bytes]) -> (ExpOpt, List[bytes]):
        if mode == b'KEEPTTL':
            return True, args[1:]
        elif mode == b'PX':
            return int(args[1]) * 1000000, args[2:]
        elif mode == b'EX':
            return int(args[1]) * 1000000000, args[2:]
        else:
            return None, args

    def _parse_cond(self, args: List[bytes]) -> (CondOpt, List[bytes]):
        if not args:
            return None, []
        else:
            return self._parse_cond_mode(args[0].upper(), args)

    def _parse_cond_mode(self, mode: bytes, args: List[bytes]) -> (CondOpt, List[bytes]):
        if mode == b'NX':
            return True, args[1:]
        elif mode == b'XX':
            return False, args[1:]
        else:
            return None, args

class GetSetCommand(SetCommand):
    name  = 'GETSET'
    arity = 3
    flags = ['write', 'denyoom', 'fast']

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 2:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_setp(ctx, args[0], args[1], None, None, True, write)

### Hash Commands ###

class HDelCommand(AbstractRedisCommand):
    name      = 'HDEL'
    arity     = -3
    flags     = ['write', 'fast']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) < 2:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_hdel(ctx, args[0], ctx.get(args[0]), args[1:], write)

    async def _handle_hdel(self, ctx: Storage, key: bytes, node: Optional[Node], keys: List[bytes], write: WriterFunction):
        if node is None:
            await write(0)
        elif isinstance(node.val, dict):
            await write(sum(bool(node.val.pop(k, None)) for k in keys))
            node.val or ctx.drop([key])
        else:
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))

class HGetCommand(AbstractRedisCommand):
    name      = 'HGET'
    arity     = 3
    flags     = ['readonly', 'fast']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 2:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_hget(ctx.get(args[0]), args[1], write)

    async def _handle_hget(self, node: Optional[Node], key: bytes, write: WriterFunction):
        if node is None:
            await write(None)
        elif isinstance(node.val, dict):
            await write(node.val.get(key))
        else:
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))

class HSetCommand(AbstractRedisCommand):
    name      = 'HSET'
    arity     = -4
    flags     = ['write', 'denyoom', 'fast']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) < 3 or not (len(args) & 1):
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_hset(ctx, args[0], ctx.get(args[0]), dict(zip(args[1::2], args[2::2])), write)

    async def _handle_hset(self, ctx: Storage, key: bytes, node: Optional[Node], data: Dict[bytes, bytes], write: WriterFunction):
        if node is None:
            ctx.put(Node(-1, key, data))
            await write(len(data))
        elif isinstance(node.val, dict):
            await write(sum(k not in node.val for k in data))
            node.val.update(data)
        else:
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))

class HMGetCommand(AbstractRedisCommand):
    name      = 'HMGET'
    arity     = -3
    flags     = ['readonly', 'fast']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) < 2:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_hmget(ctx.get(args[0]), args[1:], write)

    async def _handle_hmget(self, node: Optional[Node], keys: List[bytes], write: WriterFunction):
        if node is None:
            await write([None] * len(keys))
        elif isinstance(node.val, dict):
            await write(list(map(node.val.get, keys)))
        else:
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))

class HMSetCommand(HSetCommand):
    name = 'HMSET'

class HKeysCommand(AbstractRedisCommand):
    name      = 'HKEYS'
    arity     = 2
    flags     = ['readonly']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 1:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_hkeys(ctx.get(args[0]), write)

    async def _handle_hkeys(self, node: Optional[Node], write: WriterFunction):
        if node is None:
            await write([])
        elif isinstance(node.val, dict):
            await write(list(node.val.keys()))
        else:
            await write(Error('incorrect value type', kind = 'WRONGTYPE'))

### Decay Control Commands ###

class TTLCommand(AbstractRedisCommand):
    name       = 'TTL'
    arity      = 2
    flags      = ['readonly', 'random', 'fast']
    key_begin  = 1
    key_end    = 1
    key_step   = 1
    ttl_factor = 1000000000

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 1:
            await write(Error('incorrect number of arguments for command'))
        else:
            await self._handle_ttl(ctx.get(args[0]), write)

    async def _handle_ttl(self, node: Optional[Node], write: WriterFunction) -> int:
        if node is None:
            await write(-2)
        elif node.ttl == -1:
            await write(-1)
        else:
            await write((node.ttl - time.time_ns()) // self.ttl_factor)

class PTTLCommand(TTLCommand):
    name       = 'PTTL'
    ttl_factor = 1000000

class ExpireCommand(AbstractRedisCommand):
    name       = 'EXPIRE'
    arity      = 3
    flags      = ['write', 'fast']
    key_begin  = 1
    key_end    = 1
    key_step   = 1
    ttl_factor = 1000000000

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 2:
            await write(Error('incorrect number of arguments for command'))
        else:
            try:
                ttl = int(args[1])
            except ValueError:
                await write(Error('value is not an integer or out of range'))
            else:
                await self._handle_expire(ctx, ctx.get(args[0]), ttl * self.ttl_factor, write)

    async def _handle_expire(self, ctx: Storage, node: Optional[Node], ttl: int, write: WriterFunction):
        if node is None:
            await write(0)
        else:
            ctx.put(Node(time.time_ns() + ttl, node.key, node.val))
            await write(1)

class PExpireCommand(ExpireCommand):
    name       = 'PEXPIRE'
    ttl_factor = 1000000

### Generic Commands ###

class KeysCommand(AbstractRedisCommand):
    name      = 'KEYS'
    arity     = 1
    flags     = ['readonly']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if len(args) != 1:
            await write(Error('incorrect number of arguments for command'))
        else:
            await write(fnmatch.filter(ctx.keys, args[0]))

class CommandCommand(AbstractRedisCommand):
    name      = 'COMMAND'
    arity     = -1
    flags     = ['random', 'loading', 'stale']
    key_begin = 0
    key_end   = 0
    key_step  = 0

    async def _handle(self, ctx: Storage, args: List[bytes], write: WriterFunction):
        if not args:
            await self._handle_list(write)
        elif args[0].upper() == b'INFO':
            await self._handle_info(write, args[1:])
        elif args[0].upper() == b'COUNT' and len(args) == 1:
            await self._handle_count(write)
        elif args[0].upper() == b'GETKEYS' and len(args) >= 2:
            await self._handle_getkeys(write, args[1], args[2:])
        else:
            await write(Error('unknown subcommand or wrong number of arguments for `%s`'))

    async def _handle_list(self, write: WriterFunction):
        await self._handle_info(write, None)

    async def _handle_info(self, write: WriterFunction, cmds: Optional[List[bytes]]):
        retv = []
        cmdn = None if cmds is None else set(v.decode('utf-8') for v in cmds)

        # build the commands
        for cmd in COMMANDS:
            if cmdn is None or cmd.name in cmdn:
                retv.append(cmd.descriptor())
        else:
            await write(retv)

    async def _handle_count(self, write: WriterFunction):
        await write(len(COMMANDS))

    async def _handle_getkeys(self, write: WriterFunction, cmd: str, args: List[bytes]):
        cmd  = cmd.upper()
        cmdc = next((c for c in COMMANDS if c.name == cmd), None)

        # check for commands
        if cmdc is None:
            await write(Error('unknown command `%s`' % cmd))
            return

        # check for argc
        if (cmdc.arity < 0 and len(args) < -cmdc.arity - 1) or (cmdc.arity >= 0 and len(args) != cmdc.arity - 1):
            await write(Error('incorrect number of arguments for command'))
            return

        # key positioning information
        r = []
        e = cmdc.key_end
        d = cmdc.key_step
        i = cmdc.key_begin

        # extract each keys
        while i <= len(args) and (e < 0 or i <= e):
            r.append(args[i - 1])
            i += d
        else:
            await write(r)

### Command Discovery ###

def _is_valid_command(v: Any) -> bool:
    try:
        v.descriptor()
    except Exception as e:
        return False
    else:
        return True

COMMANDS = [
    cls for cls in globals().values() if (
        type(cls) is type and
        issubclass(cls, RedisCommand) and
        _is_valid_command(cls)
    )
]

### Bootstrap Routine ###

def main():
    import os
    import sys
    import optparse

    # configure logging facility
    logging.basicConfig(
        level  = logging.DEBUG,
        format = '[%(asctime)s] %(name)-8s %(levelname)-8s - %(message)s'
    )

    # build option parser
    ps = optparse.OptionParser()
    ps.set_usage('%prog [-b BIND] [-p PORT]')
    ps.add_option('-b', '--bind', action = 'store', dest = 'bind', default = '127.0.0.1', help = 'bind address [%default]')
    ps.add_option('-p', '--port', action = 'store', dest = 'port', default = '6379', help = 'bind port [%default]')

    # parse the options
    opts, args = ps.parse_args()
    if args:
        print('* error: invalid argument: ' + args[0] + '\n', file = sys.stderr)
        ps.print_help(file = sys.stderr)
        sys.exit(1)

    # parse the port
    try:
        port = int(opts.port)
        if not (1 <= port <= 65535):
            raise ValueError('')
    except ValueError:
        print('* error: invalid port: ' + opts.port + '\n', file = sys.stderr)
        ps.print_help(file = sys.stderr)
        sys.exit(1)

    # print a new line if running in console, to prever the '^C' messing up with logging
    def nl_iftty():
        if os.isatty(sys.stdout.fileno()):
            sys.stdout.write('\n')
            sys.stdout.flush()

    # signal handler
    def stop_server(*_):
        nl_iftty()
        logging.getLogger('redism').info('Stopping ...')
        rds.stop()

    # setup the signals
    signal.signal(signal.SIGHUP, stop_server)
    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)
    signal.signal(signal.SIGQUIT, stop_server)

    # start the server
    rds = Redis(opts.bind, port, COMMANDS, Storage())
    rds.run()

if __name__ == "__main__":
    main()
