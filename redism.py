#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import signal
import asyncio
import logging

from typing import Any
from typing import Dict
from typing import List
from typing import Union
from typing import Callable
from typing import Optional

from asyncio import StreamReader
from asyncio import StreamWriter
from asyncio import BaseEventLoop
from asyncio import IncompleteReadError

### Protocol Implementations ###

VT_ERR   = ord('-')
VT_INT   = ord(':')
VT_STR   = ord('+')
VT_BLOB  = ord('$')
VT_ARRAY = ord('*')

Value = Union[
    int,
    str,
    Optional[bytes],
    'Array',
]

Array = List[Union[
    int,
    str,
    Optional[bytes],
    'Array',
]]

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

async def _read(rd: StreamReader) -> (int, Value):
    vt, = await rd.readexactly(1)
    rdf = __reader_map__[vt]

    # check for value type
    if rdf is None:
        raise SyntaxError('invalid type tag: ' + repr(chr(vt)))
    else:
        return vt, await rdf(rd)

async def _read_err(_: StreamReader):
    raise SyntaxError('client should not send ERR to server')

async def _read_int(rd: StreamReader) -> int:
    return int((await rd.readline()).strip())

async def _read_str(rd: StreamReader) -> str:
    return (await rd.readline()).decode('utf-8')

async def _read_blob(rd: StreamReader) -> Optional[bytes]:
    rv = None
    nb = await _read_int(rd)

    # special case for `Null`
    if nb == -1:
        return rv

    # other negative values are invalid
    if nb < 0:
        raise SyntaxError('invalid bulk string length: %d' % nb)

    # read the blob
    rv = await rd.readexactly(nb)
    el = await rd.readline()

    # must be no more than `nb` bytes
    if el != b'\r\n':
        raise SyntaxError('%d more bytes than expected' % (len(el) - nb - 2))
    else:
        return rv

async def _read_array(rd: StreamReader) -> Array:
    ret = []
    size = await _read_int(rd)

    # check for size
    if size < 0:
        raise SyntaxError('negative array size')

    # read every element
    for _ in range(size):
        _, vv = await _read(rd)
        ret.append(vv)
    else:
        return ret

async def _write(wr: StreamWriter, val: Value):
    _write_value(wr, val)
    await wr.drain()

def _write_value(wr: StreamWriter, val: Value):
    if val is None:
        wr.write(b'$-1\r\n')
    elif isinstance(val, int):
        wr.write((':%d\r\n' % val).encode('utf-8'))
    elif isinstance(val, str):
        wr.write(('+%s\r\n' % val).encode('utf-8'))
    elif isinstance(val, bytes):
        wr.write(b'$%d\r\n%s\r\n' % (len(val), val))
    elif isinstance(val, Error):
        wr.write(('-%s\r\n' % val.error).encode('utf-8'))
    elif isinstance(val, (set, list, tuple)):
        _write_array_len(wr, val)
        _write_array_body(wr, val)
    else:
        raise TypeError('invalid value type: ' + type(val).__name__)

def _write_array_len(wr: StreamWriter, val: Array):
    wr.write(('*%d\r\n' % len(val)).encode('utf-8'))

def _write_array_body(wr: StreamWriter, val: Array):
    for vv in val:
        _write_value(wr, vv)

__reader_map__ = [None] * 256
__reader_map__[VT_ERR  ] = _read_err
__reader_map__[VT_INT  ] = _read_int
__reader_map__[VT_STR  ] = _read_str
__reader_map__[VT_BLOB ] = _read_blob
__reader_map__[VT_ARRAY] = _read_array

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
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self._handle_conn, self.bind, self.port, loop = loop)
        psrv = loop.run_until_complete(coro)

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
                vt, args = await _read(rd)
                if vt != VT_ARRAY:
                    self.log.error('Invalid client command: expected to be an array.')
                    break
                if not args:
                    self.log.error('Invalid client command: empty command.')
                    break
                await self._handle_command(args[0], args[1:], wr)
        except IncompleteReadError as e:
            if e.partial:
                self.log.warn('Unexpected EOF from client.')
        except SyntaxError as e:
            self.log.error('Invalid client command: %s.' % e.msg)
        except Exception as e:
            self.log.exception('Errors occured when handling requests: ' + str(e))
        finally:
            self._close_writer(wr)

    async def _handle_command(self, cmd: str, args: List[Value], wr: StreamWriter):
        cmds = cmd.decode('utf-8')
        cmdc = self.cmds.get(cmds.upper())

        # writer function
        async def write_func(v: Value):
            await _write(wr, v)

        # check for commands
        if cmdc is None:
            await _write(wr, Error('unknown command `%s`' % cmds))
        else:
            await cmdc.handle(self.ctx, args, write_func)

class RedisCommand:
    @property
    def name(self) -> str:
        raise NotImplementedError('not implemented: RedisCommand.name')

    @property
    def arity(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.arity')

    @property
    def flags(self) -> List[str]:
        raise NotImplementedError('not implemented: RedisCommand.flags')

    @property
    def key_end(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.key_end')

    @property
    def key_step(self) -> int:
        raise NotImplementedError('not implemented: RedisCommand.key_step')

    @property
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

    async def handle(self, ctx: Any, args: List[Value], write: WriterFunction):
        raise NotImplementedError('not implemented: RedisCommand.handle')

### Command Implementations ###

class Context:
    pass

class GetCommand(RedisCommand):
    name      = 'GET'
    arity     = 2
    flags     = ['readonly', 'fast']
    key_begin = 1
    key_end   = 1
    key_step  = 1

    async def handle(self, ctx: Context, args: List[Value], write: WriterFunction):
        await write('hello, world')

class CommandCommand(RedisCommand):
    name      = 'COMMAND'
    arity     = -1
    flags     = ['random', 'loading', 'stale']
    key_begin = 0
    key_end   = 0
    key_step  = 0

    async def handle(self, ctx: Context, args: List[Value], write: WriterFunction):
        if not args:
            await self._handle_list(write)
        elif args[0].upper() == b'INFO':
            await self._handle_info(write, args[1:])
        elif args[0].upper() == b'COUNT' and len(args) == 1:
            await self._handle_count(write)
        elif args[0].upper() == b'GETKEYS' and len(args) >= 2:
            await self._handle_getkeys(write, args[1].decode('utf-8'), args[2:])
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
            await write(Error('incorrect number of arguments for command `%s`' % cmd))
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

COMMANDS = [
    GetCommand,
    CommandCommand,
]

### Bootstrap Helpers ###

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
    rds = Redis(opts.bind, port, COMMANDS, Context())
    rds.run()

if __name__ == "__main__":
    main()
