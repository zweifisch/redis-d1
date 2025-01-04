type Primitive = string | number | boolean | null

export class KV {

  private initialized = false
  private initialization?: Promise<D1ExecResult>
  private table = 'kv_store'

  constructor(private db: D1Database, opts?: Partial<{initialize: boolean, table: string}>) {
    this.db = db
    if (opts?.table) {
      this.table = `"${opts.table}"`
    }
    if (opts?.initialize) {
      this.init()
    }
  }

  init() {
    if (!this.initialization) {
      this.initialization = this.db
        .exec(`\
          CREATE TABLE IF NOT EXISTS ${this.table} (
            key TEXT PRIMARY KEY,
            value JSON,
            expire_at INTEGER
          ) WITHOUT ROWID;
          CREATE INDEX IF NOT EXISTS idx_expire_at ON ${this.table} (expire_at);`)
      this.initialization.then(() => this.initialized = true)
    }
    return this.initialization
  }

  async set<T>(key: string, value: T, opts?: Partial<{ex: number, nx: boolean}>) {
    if (!this.initialized) {
      await this.init()
    }
    const expire_at = typeof opts?.ex === 'number' ? opts.ex + Math.floor(Date.now() / 1000) : null
    const result = await this.db.prepare(`INSERT OR ${opts?.nx ? 'IGNORE' : 'REPLACE'}
      INTO ${this.table} (key, value, expire_at) VALUES (?, ?, ?)`)
      .bind(key, this.encode(value), expire_at)
      .run()
    return result?.meta.changes > 0
  }

  private encode(value: any) {
    return value === undefined ? 'null' :
      typeof value === 'number' ? value :
      JSON.stringify(value)
  }

  private decode(value: any) {
    return typeof value === 'string' ? JSON.parse(value) : value
  }

  async get(key: string) {
    if (!this.initialized) {
      await this.init()
    }
    const val = await this.db.prepare(`\
      SELECT value FROM ${this.table}
        WHERE key = ?
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind(key)
      .first()
    if (val) {
      return this.decode(val.value)
    }
  }

  async mset(obj: Record<string, any>) {
    if (!this.initialized) {
      await this.init()
    }
    const entries = Object.entries(obj)
    return this.db.prepare(`INSERT OR REPLACE INTO ${this.table} (key, value) VALUES ${entries.map(_ => '(?,?)').join(',')}`)
      .bind(...entries.map(([k, v]) => [k, this.encode(v)]).flat())
      .run()
  }

  async mget(...keys: string[]) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      SELECT key, value FROM ${this.table}
        WHERE key IN (${keys.map(_ => '?').join(',')})
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind(...keys)
      .run()
    return Object.fromEntries(result.results.map(x => [x.key, this.decode(x.value)]))
  }

  async keys(pattern: string) {
    this.initialized || await this.init()
    const escape = pattern.includes('_') || pattern.includes('%')
    if (escape) {
      pattern = pattern.replaceAll('_', '\\_').replaceAll('%', '\\%')
    }
    pattern = pattern.replaceAll('?', '_').replaceAll('*', '%')
    return this.db.prepare(`\
      SELECT key FROM ${this.table}
        WHERE key LIKE '${pattern}'${escape ? " ESCAPE '\\'" : ''}
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind()
      .run()
      .then(rows => rows.results.map(x => x.key))
  }

  async incr(key: string) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`INSERT INTO ${this.table} (key, value) VALUES (?, 1) ON CONFLICT(key) DO UPDATE SET value = value + 1 RETURNING value`)
      .bind(key)
      .first()
    return parseInt(result!.value as string)
  }

  async decr(key: string) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      INSERT INTO ${this.table} (key, value) VALUES (?, -1)
      ON CONFLICT(key) DO UPDATE SET value = value - 1 RETURNING value`)
      .bind(key)
      .first()
    return parseInt(result!.value as string)
  }

  async lpush<T>(key: string, value: T) {
    this.initialized || await this.init()
    const val = this.encode(value)
    await this.db.prepare(`\
    INSERT INTO ${this.table} (key, value) VALUES (?, json_array(json(?)))
    ON CONFLICT(key) DO UPDATE SET value = json_insert(value, '$[#]', json(?))`)
      .bind(key, val, val)
      .run()
  }

  async lpop(key: string) {
    this.initialized || await this.init()
    const [result] = await this.db.batch([
      this.db.prepare(`\
        SELECT value ->> '$[0]' value FROM ${this.table}
          WHERE key = ?
          AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`).bind(key),
      this.db.prepare(`\
        UPDATE ${this.table} SET value = json_remove(value, "$[0]")
        WHERE key = ?
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`).bind(key)
      ])
    return (result?.results?.[0] as any)?.value
  }

  async rpush<T>(key: string, value: T) {
    this.initialized || await this.init()
    const val = JSON.stringify([value])
    await this.db.prepare(`\
    INSERT INTO ${this.table} (key, value) VALUES (?, json(?))
    ON CONFLICT(key) DO UPDATE SET value = json(? || substr(value, 2))`)
      .bind(key, val, val.slice(0, -1) + ',')
      .run()
  }

  async rpop(key: string) {
    this.initialized || await this.init()
    const [result] = await this.db.batch([
      this.db.prepare(`\
        SELECT value ->> '$[#-1]' value FROM ${this.table}
          WHERE key = ?
          AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`).bind(key),
      this.db.prepare(`\
        UPDATE ${this.table} SET value = json_remove(value, "$[#-1]")
          WHERE key = ?
          AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`).bind(key),
      ])
    return (result?.results?.[0] as any)?.value
  }

  async lrange(key: string, start: number, end: number) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      SELECT value FROM ${this.table}
        WHERE key = ?
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind(key)
      .first()
    if (result?.value) {
      const arr = JSON.parse(result.value as string)
      return end = -1 ? arr.slice(start) : arr.slice(start, end + 1)
    }
    return []
  }

  async llen(key: string) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      SELECT json_array_length(value) value FROM ${this.table}
        WHERE key = ?
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind(key)
      .first()
    if (typeof result?.value === 'number') {
      return result.value
    }
    return 0
  }

  async lrem(key: string, count: number, element: Primitive) {
    this.initialized || await this.init()

    if (count === 0) {
      return this.db.prepare(`\
        UPDATE ${this.table} SET value = (
          SELECT json_group_array(el.value) FROM ${this.table} tbl, json_each(tbl.value) el
            WHERE tbl.key = ?
            AND el.value <> ?
            AND (expire_at IS NULL OR expire_at > UNIXEPOCH())
        ) WHERE key = ?`
      ).bind(key, element, key).run()
    }

    return this.db.prepare(`\
      UPDATE ${this.table} SET value = (
        SELECT json_group_array(value)
        FROM (
            SELECT
              el.value value,
              el.key key,
              ROW_NUMBER() OVER (PARTITION BY el.value order by el.key ${count > 0 ? 'ASC' : 'DESC'}) rn
            FROM ${this.table} tbl, json_each(tbl.value) el
              WHERE tbl.key = ?
              AND (expire_at IS NULL OR expire_at > UNIXEPOCH())
            ORDER BY el.key
        )
        WHERE value <> ? OR rn > ?
      )
      WHERE key = ?`
    ).bind(key, element, Math.abs(count), key).run()
  }

  async expire(key: string, seconds: number) {
    this.initialized || await this.init()
    return this.db.prepare(`UPDATE ${this.table} SET expire_at = UNIXEPOCH() + ? WHERE key = ?`)
      .bind(seconds, key)
      .run()
  }

  async ttl(key: string) {
    this.initialized || await this.init()
    const result: {expire_at: number | null} | undefined | null = await this.db.prepare(`\
      SELECT expire_at FROM ${this.table} WHERE key = ?`)
      .bind(key)
      .first()
    const now = Math.floor(Date.now() / 1000)
    if (!result || result.expire_at && result.expire_at <= now) {
      return -2
    }
    if (!result.expire_at) {
      return -1
    }
    return result.expire_at - now
  }

  async hset<T>(key: string, field: string, value: T) {
    this.initialized || await this.init()
    const val = this.encode(value)
    return this.db.prepare(`\
      INSERT INTO ${this.table} (key, value) VALUES (?, json_object(?, json(?)))
        ON CONFLICT(key) DO UPDATE SET value = json_set(value, '$.${field}', json(?))`)
      .bind(key, field, val, val)
      .run()
  }

  async hget(key: string, field: string) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      SELECT value -> '$.${field}' value FROM ${this.table}
        WHERE key = ?
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind(key)
      .first()
    return this.decode(result?.value)
  }

  async hgetall(key: string) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      SELECT value ->> '$' value FROM ${this.table}
        WHERE key = ?
        AND (expire_at IS NULL OR expire_at > UNIXEPOCH())`)
      .bind(key)
      .first()
    return this.decode(result?.value)
  }

  async del(...keys: string[]) {
    this.initialized || await this.init()
    return this.db.prepare(`DELETE FROM ${this.table} WHERE key IN (${keys.map(_ => '?').join(',')})`)
      .bind(...keys).run()
  }
}