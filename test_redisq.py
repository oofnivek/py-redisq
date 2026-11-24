"""Tests for redisq. Run with: python3 -m unittest -v"""
import importlib.machinery
import importlib.util
import io
import json
import os
import pty
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "redisq")

# redisq has no .py extension, so load it explicitly
_loader = importlib.machinery.SourceFileLoader("redisq", SCRIPT)
redisq = importlib.util.module_from_spec(importlib.util.spec_from_loader("redisq", _loader))
_loader.exec_module(redisq)


def credentials(port, proxy_port=None):
    proxy = f"socks_proxy = 127.0.0.1:{proxy_port}\n" if proxy_port else ""
    return f"""\
[test]
host = 127.0.0.1
port = {port}
password = s3cr"et pw ; comment

[acl]
host = 127.0.0.1
port = {port}
user = reader
password = pw
db = 3

[nopass]
host = 127.0.0.1
port = {port}

[writer]
host = 127.0.0.1
port = {port}
allow_write = true

[badflag]
host = 127.0.0.1
port = {port}
allow_write = maybe

[proxied]
host = localhost
port = {port}
{proxy}"""


# --- fake servers ----------------------------------------------------------------------------------------------------

def listen():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()
    return server


def serve_forever(server, handler):
    def loop():
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            threading.Thread(target=handler, args=(conn,), daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()


class FakeRedis:
    """Speaks just enough RESP to answer the commands the tests send, and records every command it receives."""

    KEYS = [b"user:%d" % i for i in range(25)]

    def __init__(self):
        self.server = listen()
        self.port = self.server.getsockname()[1]
        self.commands = []
        serve_forever(self.server, self.handle)

    def close(self):
        self.server.close()

    def handle(self, conn):
        with conn, conn.makefile("rb") as f:
            while True:
                argv = redisq.read_reply(f) if f.peek(1) else None
                if argv is None:
                    return
                self.commands.append(argv)
                conn.sendall(self.reply(argv))

    def reply(self, argv):
        name = argv[0].upper()
        if name == b"AUTH":
            return b"+OK\r\n" if argv[-1] in (b's3cr"et pw', b"pw") else b"-WRONGPASS invalid password\r\n"
        if name in (b"SELECT", b"SET"):
            return b"+OK\r\n"
        if name == b"PING":
            return b"+PONG\r\n"
        if name == b"GET":
            return b"$-1\r\n" if argv[1] == b"missing" else b"$3\r\nbar\r\n"
        if name == b"DEL":
            return b":1\r\n"
        if name == b"SCAN":
            # three pages of 10 keys, with one key repeated across pages
            cursor = int(argv[1])
            page = self.KEYS[cursor * 10:cursor * 10 + 10]
            if cursor == 1:
                page = page + [self.KEYS[0]]
            next_cursor = b"0" if cursor == 2 else str(cursor + 1).encode()
            return b"*2\r\n$%d\r\n%s\r\n" % (len(next_cursor), next_cursor) + redisq.encode_command(page)
        if name == b"SLEEP":
            return b""  # never answers
        return b"-ERR unknown command '" + argv[0] + b"'\r\n"


def fake_socks5(conn):
    """Minimal no-auth SOCKS5 server that only handles CONNECT to a domain name."""
    assert redisq.recv_exact(conn, 3) == b"\x05\x01\x00"
    conn.sendall(b"\x05\x00")
    assert redisq.recv_exact(conn, 4) == b"\x05\x01\x00\x03"
    host = redisq.recv_exact(conn, redisq.recv_exact(conn, 1)[0]).decode()
    port = struct.unpack(">H", redisq.recv_exact(conn, 2))[0]
    upstream = socket.create_connection((host, port))
    conn.sendall(b"\x05\x00\x00\x01" + bytes(4) + bytes(2))
    threading.Thread(target=pipe, args=(conn, upstream), daemon=True).start()
    pipe(upstream, conn)


def pipe(src, dst):
    try:
        while data := src.recv(65536):
            dst.sendall(data)
    except OSError:
        pass
    finally:
        src.close()
        dst.close()


# --- unit tests ------------------------------------------------------------------------------------------------------

class CheckReadOnlyTest(unittest.TestCase):
    ALLOWED = [
        ["GET", "k"],
        ["get", "k"],
        ["HGETALL", "h"],
        ["ZRANGE", "z", "0", "-1", "WITHSCORES"],
        ["SCAN", "0", "MATCH", "user:*"],
        ["CONFIG", "GET", "maxmemory"],
        ["config", "get", "*"],
        ["OBJECT", "ENCODING", "k"],
        ["INFO"],
        ["SELECT", "2"],
    ]
    REJECTED = [
        ["SET", "k", "v"],
        ["DEL", "k"],
        ["FLUSHALL"],
        ["KEYS", "*"],
        ["CONFIG", "SET", "maxmemory", "0"],
        ["CONFIG"],
        ["CLIENT", "KILL", "ID", "1"],
        ["AUTH", "admin", "pw"],
        ["HELLO", "3", "AUTH", "admin", "pw"],
        ["EVAL", "return 1", "0"],
        ["MULTI"],
        ["SORT", "k", "STORE", "dst"],
        ["ZRANGESTORE", "dst", "z", "0", "-1"],
        ["SHUTDOWN"],
    ]

    def test_allowed(self):
        for argv in self.ALLOWED:
            with self.subTest(argv=argv):
                redisq.check_read_only([argv])

    def test_rejected(self):
        for argv in self.REJECTED:
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                redisq.check_read_only([["GET", "k"], argv])

    def test_empty(self):
        with self.assertRaises(ValueError):
            redisq.check_read_only([])


class ParseCommandsTest(unittest.TestCase):
    def test_lines_and_quoting(self):
        text = 'GET a\n\n  # comment\nHGET "my hash" \'field one\'\nSET k "say \\"hi\\""\n'
        self.assertEqual(redisq.parse_commands(text),
                         [["GET", "a"], ["HGET", "my hash", "field one"], ["SET", "k", 'say "hi"']])

    def test_unbalanced_quote(self):
        with self.assertRaisesRegex(ValueError, "line 2"):
            redisq.parse_commands('GET a\nGET "b\n')


class ProtocolTest(unittest.TestCase):
    def parse(self, data):
        return redisq.read_reply(io.BytesIO(data))

    def test_encode(self):
        self.assertEqual(redisq.encode_command(["GET", "kéy"]), b"*2\r\n$3\r\nGET\r\n$4\r\nk\xc3\xa9y\r\n")

    def test_resp2(self):
        self.assertEqual(self.parse(b"+OK\r\n"), "OK")
        self.assertIsInstance(self.parse(b"+OK\r\n"), redisq.Status)
        self.assertEqual(str(self.parse(b"-ERR bad\r\n")), "ERR bad")
        self.assertEqual(self.parse(b":42\r\n"), 42)
        self.assertEqual(self.parse(b"$5\r\na\r\nb!\r\n"), b"a\r\nb!")
        self.assertIsNone(self.parse(b"$-1\r\n"))
        self.assertEqual(self.parse(b"*2\r\n$1\r\na\r\n*1\r\n:1\r\n"), [b"a", [1]])
        self.assertIsNone(self.parse(b"*-1\r\n"))

    def test_resp3(self):
        self.assertEqual(self.parse(b"%1\r\n+k\r\n:1\r\n"), ["k", 1])
        self.assertIsNone(self.parse(b"_\r\n"))
        self.assertEqual(self.parse(b"#t\r\n"), 1)
        self.assertEqual(self.parse(b"=7\r\ntxt:abc\r\n"), b"abc")
        self.assertEqual(self.parse(b"|1\r\n+a\r\n+b\r\n:5\r\n"), 5)

    def test_closed_connection(self):
        with self.assertRaises(ConnectionError):
            self.parse(b"$10\r\nabc")


class FormatTest(unittest.TestCase):
    def test_human(self):
        reply = [b"a", [b"b\n\"c\"", 3], None, [], redisq.Status("OK"), redisq.ReplyError("ERR x")] + [b"x"] * 5
        self.assertEqual(redisq.format_human(reply), "\n".join([
            ' 1) "a"',
            ' 2) 1) "b\\n\\"c\\""',
            '    2) (integer) 3',
            ' 3) (nil)',
            ' 4) (empty array)',
            ' 5) OK',
            ' 6) (error) ERR x',
            ' 7) "x"', ' 8) "x"', ' 9) "x"', '10) "x"', '11) "x"',
        ]))

    def test_quote_binary(self):
        self.assertEqual(redisq.quote(b"\x00\xff\xc3\xa9"), '"\\x00\\xff\\xc3\\xa9"')

    def test_raw_and_json(self):
        reply = [b"a", [b"b", 2], None]
        self.assertEqual(redisq.format_raw(reply), b"a\nb\n2\n")
        self.assertEqual(redisq.to_json(reply + [b"\xff", redisq.ReplyError("ERR x")]),
                         ["a", ["b", 2], None, "\\xff", {"error": "ERR x"}])


class LoadProfileTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "credentials")
        with open(self.path, "w") as f:
            f.write(credentials(6379) + "[nohost]\nport = 1\n\n[useronly]\nhost = x\nuser = u\n")
        os.chmod(self.path, 0o600)
        patcher = mock.patch.dict(os.environ, {"REDISQ_CREDENTIALS": self.path})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_loads_profile(self):
        profile = redisq.load_profile("test")
        self.assertEqual(profile["host"], "127.0.0.1")
        self.assertEqual(profile["password"], 's3cr"et pw')

    def test_rejects_group_or_world_access(self):
        for mode in (0o640, 0o604, 0o644, 0o660):
            os.chmod(self.path, mode)
            with self.subTest(mode=oct(mode)), self.assertRaisesRegex(SystemExit, "chmod 600"):
                redisq.load_profile("test")

    def test_missing_file(self):
        os.remove(self.path)
        with self.assertRaisesRegex(SystemExit, "not found"):
            redisq.load_profile("test")

    def test_unknown_profile(self):
        with self.assertRaisesRegex(SystemExit, "profile 'nope' not found"):
            redisq.load_profile("nope")

    def test_missing_host(self):
        with self.assertRaisesRegex(SystemExit, "missing host"):
            redisq.load_profile("nohost")

    def test_user_without_password(self):
        with self.assertRaisesRegex(SystemExit, "user but no password"):
            redisq.load_profile("useronly")


# --- end to end ------------------------------------------------------------------------------------------------------

class MainTest(unittest.TestCase):
    """Runs the script against a fake Redis server, optionally through a fake SOCKS5 proxy."""

    def setUp(self):
        self.redis = FakeRedis()
        self.addCleanup(self.redis.close)
        self.proxy = listen()
        self.addCleanup(self.proxy.close)
        serve_forever(self.proxy, fake_socks5)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        creds = os.path.join(self.tmp.name, "credentials")
        with open(creds, "w") as f:
            f.write(credentials(self.redis.port, self.proxy.getsockname()[1]))
        os.chmod(creds, 0o600)
        self.env = dict(os.environ, REDISQ_CREDENTIALS=creds)
        self.env.pop("REDISQ_PROFILE", None)

    def run_redisq(self, *args, **kwargs):
        kwargs.setdefault("stdin", subprocess.DEVNULL)
        return subprocess.run([sys.executable, SCRIPT, *args], env=self.env, capture_output=True, text=True,
                              timeout=20, **kwargs)

    def sent(self):
        return [[a.decode() for a in argv] for argv in self.redis.commands]

    def test_authenticates_then_runs_command(self):
        result = self.run_redisq("--profile", "test", "GET", "foo")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '"bar"\n')
        self.assertEqual(self.sent(), [["AUTH", 's3cr"et pw'], ["GET", "foo"]])

    def test_acl_user_and_db(self):
        self.run_redisq("--profile", "acl", "PING")
        self.assertEqual(self.sent(), [["AUTH", "reader", "pw"], ["SELECT", "3"], ["PING"]])

    def test_db_flag_overrides_profile(self):
        self.run_redisq("--profile", "acl", "-n", "0", "PING")
        self.assertEqual(self.sent(), [["AUTH", "reader", "pw"], ["PING"]])

    def test_no_password_skips_auth(self):
        self.run_redisq("--profile", "nopass", "PING")
        self.assertEqual(self.sent(), [["PING"]])

    def test_single_argument_is_split(self):
        result = self.run_redisq("--profile", "nopass", 'GET "my key"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.sent(), [["GET", "my key"]])

    def test_arguments_that_look_like_options(self):
        result = self.run_redisq("--profile", "nopass", "GET", "-1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.sent(), [["GET", "-1"]])

    def test_reads_commands_from_piped_stdin(self):
        result = self.run_redisq("--profile", "nopass", stdin=None, input="GET a\n\nGET missing\nPING\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '"bar"\n(nil)\nPONG\n')

    def test_errors_instead_of_waiting_on_a_terminal(self):
        controller, terminal = pty.openpty()
        self.addCleanup(os.close, controller)
        try:
            result = self.run_redisq("--profile", "nopass", stdin=terminal)
        finally:
            os.close(terminal)
        self.assertEqual(result.returncode, 2)
        self.assertIn("no command given", result.stderr)

    def test_error_reply_sets_exit_status_but_later_commands_run(self):
        result = self.run_redisq("--profile", "nopass", stdin=None, input="TIME\nPING\n")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "(error) ERR unknown command 'TIME'\nPONG\n")

    def test_wrong_password(self):
        with open(self.env["REDISQ_CREDENTIALS"], "a") as f:
            f.write(f"\n[wrong]\nhost = 127.0.0.1\nport = {self.redis.port}\npassword = nope\n")
        result = self.run_redisq("--profile", "wrong", "PING")
        self.assertEqual(result.returncode, 1)
        self.assertIn("authentication failed: WRONGPASS", result.stderr)
        self.assertEqual(self.sent(), [["AUTH", "nope"]])

    def test_rejected_command_never_reaches_redis(self):
        result = self.run_redisq("--profile", "nopass", stdin=None, input="GET a\nDEL a\n")
        self.assertEqual(result.returncode, 1)
        self.assertIn("rejected: DEL is not a read command", result.stderr)
        self.assertEqual(self.redis.commands, [])

    def test_keys_suggests_scan(self):
        result = self.run_redisq("--profile", "nopass", "KEYS", "*")
        self.assertIn("--scan", result.stderr)
        self.assertEqual(self.redis.commands, [])

    def test_allow_write(self):
        result = self.run_redisq("--profile", "writer", "DEL", "a")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "(integer) 1\n")
        self.assertEqual(self.sent(), [["DEL", "a"]])

    def test_invalid_allow_write_is_an_error(self):
        result = self.run_redisq("--profile", "badflag", "PING")
        self.assertEqual(result.returncode, 1)
        self.assertIn("allow_write must be true or false", result.stderr)
        self.assertEqual(self.redis.commands, [])

    def test_socks_proxy(self):
        result = self.run_redisq("--profile", "proxied", "GET", "foo")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '"bar"\n')

    def test_scan_dedupes_and_follows_cursor(self):
        result = self.run_redisq("--profile", "nopass", "--scan", "user:*")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [k.decode() for k in FakeRedis.KEYS])
        self.assertEqual([argv[1] for argv in self.sent()], ["0", "1", "2"])
        self.assertEqual(self.sent()[0][2:], ["MATCH", "user:*", "COUNT", "1000"])

    def test_scan_max_rows(self):
        result = self.run_redisq("--profile", "nopass", "--scan", "*", "--max-rows", "12")
        self.assertEqual(len(result.stdout.splitlines()), 12)
        self.assertIn("stopped after 12 keys", result.stderr)

    def test_raw_and_json_output(self):
        raw = self.run_redisq("--profile", "nopass", "--raw", "GET", "a")
        self.assertEqual(raw.stdout, "bar\n")
        js = self.run_redisq("--profile", "nopass", "--json", stdin=None, input="GET a\nGET missing\nPING\n")
        self.assertEqual([json.loads(line) for line in js.stdout.splitlines()], ["bar", None, "PONG"])

    def test_timeout(self):
        result = self.run_redisq("--profile", "writer", "--timeout", "1", "SLEEP")
        self.assertEqual(result.returncode, 1)
        self.assertIn("no reply after 1 seconds", result.stderr)

    def test_connection_refused(self):
        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            port = unused.getsockname()[1]
        with open(self.env["REDISQ_CREDENTIALS"], "a") as f:
            f.write(f"\n[down]\nhost = 127.0.0.1\nport = {port}\n")
        result = self.run_redisq("--profile", "down", "PING")
        self.assertEqual(result.returncode, 1)
        self.assertIn("cannot connect", result.stderr)


if __name__ == "__main__":
    unittest.main()
