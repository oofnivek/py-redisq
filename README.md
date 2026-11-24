# redisq

Read-only Redis commands by named profile, optionally through a SOCKS5 proxy.

```sh
redisq --profile uat GET user:42
redisq --profile uat "HGETALL session:abc"
redisq --profile uat -n 2 ZRANGE leaderboard 0 -1 WITHSCORES
redisq --profile uat --scan "user:*"
printf 'GET a\nTTL a\n' | redisq --profile uat
```

Pass a command as separate arguments, or as one argument that redisq splits with shell-style quoting
(`'GET "my key"'`). With no command, redisq reads commands from stdin, one per line; blank lines and lines starting with
`#` are skipped. Every command runs on one connection, and the exit status is 1 if any of them got an error reply.

redisq speaks the Redis protocol itself, so it needs only Python 3.8+ and no `redis-cli`. That also means the command
it checks is exactly the command it sends.

## Read-only

Only read commands are accepted: `GET`, `HGETALL`, `ZRANGE`, `SCAN`, `TTL`, `TYPE`, `INFO` and the like, plus the read
subcommands of `CONFIG`, `CLIENT`, `OBJECT`, `MEMORY`, `XINFO`, `CLUSTER`, `COMMAND`, `SLOWLOG` and `LATENCY`
(`CONFIG GET` passes, `CONFIG SET` doesn't). The full list is `READ_COMMANDS` and `READ_SUBCOMMANDS` in `redisq`.
`AUTH` and `HELLO` are refused, since they could switch the session to a user with write access. Lua scripts
(`EVAL`, `FCALL`) and transactions are refused too.

Unlike MySQL, Redis has no read-only session to fall back on, so this check is the only guard on redisq's side. For a
second, server-side guard, use an ACL user with read-only permissions, for example:

```
ACL SETUSER reader on >change-me ~* +@read +@connection -@dangerous
```

### KEYS and --scan

`KEYS` is refused because it blocks the whole server while it walks the keyspace. Use `--scan` instead, which walks the
keyspace with `SCAN` and prints matching keys one per line:

```sh
redisq --profile uat --scan "session:*"
redisq --profile uat --scan "*" --max-rows 0     # every key
```

`--scan` stops after 1000 keys by default; change that with `--max-rows`, using `0` for no limit.

## Output

The default output looks like `redis-cli`'s: `"quoted"` strings, `(integer) 5`, `(nil)`, numbered arrays. Use `--raw`
for bare values one per line, or `--json` for one line of JSON per reply (`null` for nil, `{"error": "..."}` for errors).

## Timeout

redisq gives up after waiting 30 seconds for a reply. Change it with `--timeout`, using `0` for no limit. Redis can't
cancel a command it has started, so this stops redisq waiting but not the server working.

## Credentials

redisq reads profiles from **`~/.redisq/credentials`**. To use a different file, set `REDISQ_CREDENTIALS` to its
path.

Start from the example in this repo:

```sh
mkdir -p ~/.redisq
cp credentials.example ~/.redisq/credentials
chmod 700 ~/.redisq
chmod 600 ~/.redisq/credentials
```

Then edit `~/.redisq/credentials` and fill in your real hosts and passwords. redisq refuses to run if the file is owned
by someone else or readable by group or others, so keep it at `chmod 600`. Keep the real file outside this repo, and
never commit it. Only `credentials.example` belongs in git.

Each `[section]` is a profile:

| Key           | Required | Description                                                    |
|---------------|----------|----------------------------------------------------------------|
| `host`        | yes      | Redis host                                                     |
| `port`        | no       | Defaults to `6379`                                             |
| `user`        | no       | ACL user (Redis 6+); needs `password`                          |
| `password`    | no       | Sent with `AUTH`; leave it out for a server without auth       |
| `db`          | no       | Database number, defaults to `0`; `-n` overrides it per run    |
| `tls`         | no       | `true` to connect over TLS; defaults to `false`                |
| `tls_ca`      | no       | CA bundle to verify the server with; defaults to the system's  |
| `socks_proxy` | no       | `host:port` of a SOCKS5 proxy (no authentication)              |
| `allow_write` | no       | `true` to allow writes; defaults to `false`                    |

Inline comments must start with `;`. The password is sent over the connection, never on a command line where `ps`
would show it. Through a SOCKS proxy, the proxy resolves `host`, and TLS still verifies the certificate against `host`.

### Writing

A profile is read-only unless it sets `allow_write = true`. With it on, redisq skips the command check, so `SET`,
`DEL`, `EXPIRE`, `FLUSHDB` and the rest go through (as far as the Redis user's ACL allows). Flip it back to `false`
when you're done.

Instead of passing `--profile` every time, you can set a default:

```sh
export REDISQ_PROFILE=uat
```

## Tests

```sh
python3 -m unittest -v
```

The tests use a fake Redis server and SOCKS5 proxy, so they need no Redis.
