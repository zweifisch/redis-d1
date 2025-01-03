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
        .exec(`CREATE TABLE IF NOT EXISTS ${this.table} (key TEXT PRIMARY KEY, value JSON)`)
      this.initialization.then(() => this.initialized = true)
    }
    return this.initialization
  }

  async set(key: string, value: any) {
    if (!this.initialized) {
      await this.init()
    }
    return this.db.prepare(`INSERT OR REPLACE INTO ${this.table} (key, value) VALUES (?, ?)`)
      .bind(key, this.encode(value))
      .run()
  }

  encode(value: any) {
    return value === undefined ? 'null' :
      typeof value === 'number' ? value :
      JSON.stringify(value)
  }

  decode(value: any) {
    return typeof value === 'string' ? JSON.parse(value) : value
  }

  async get(key: string) {
    if (!this.initialized) {
      await this.init()
    }
    const val = await this.db.prepare(`SELECT value FROM ${this.table} WHERE key = ?`)
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
    const result = await this.db.prepare(`SELECT key, value FROM ${this.table} WHERE key IN (${keys.map(_ => '?').join(',')})`)
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
    return this.db.prepare(`SELECT key FROM ${this.table} where key like '${pattern}'${escape ? " ESCAPE '\\'" : ''}`)
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
    const result = await this.db.prepare(`\
    INSERT INTO ${this.table} (key, value) VALUES (?, json_array(?))
    ON CONFLICT(key) DO UPDATE SET value = json_insert(value, '$[#]', ?)`)
      .bind(key, val, val)
      .run()
  }

  async lpop(key: string) {
    this.initialized || await this.init()
    const [result] = await this.db.batch([
      this.db.prepare(`\
        SELECT json_extract(value, "$[0]") value FROM ${this.table} WHERE key = ?`).bind(key),
      this.db.prepare(`\
        UPDATE ${this.table} SET value = json_remove(value, "$[0]") WHERE key = ?`).bind(key)
      ])
    return (result?.results?.[0] as any)?.value
  }

  async rpop(key: string) {
    this.initialized || await this.init()
    const [result] = await this.db.batch([
      this.db.prepare(`\
        SELECT json_extract(value, "$[#-1]") value FROM ${this.table} WHERE key = ?`).bind(key),
      this.db.prepare(`\
        UPDATE ${this.table} SET value = json_remove(value, "$[#-1]") WHERE key = ?`).bind(key)
      ])
    return (result?.results?.[0] as any)?.value
  }

  async lrange(key: string, start: number, end: number) {
    this.initialized || await this.init()
    const result = await this.db.prepare(`\
      SELECT value FROM ${this.table} WHERE key = ?`)
      .bind(key)
      .first()
    if (result?.value) {
      const arr = JSON.parse(result!.value as string)
      return end = -1 ? arr.slice(start) : arr.slice(start, end + 1)
    }
    return []
  }

  async del(...keys: string[]) {
    this.initialized || await this.init()
    return this.db.prepare(`DELETE FROM ${this.table} WHERE key IN (${keys.map(_ => '?').join(',')})`)
      .bind(...keys).run()
  }
}