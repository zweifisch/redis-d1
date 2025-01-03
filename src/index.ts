export class KV {

  private initialized = false
  private initialization?: Promise<D1ExecResult>
  private table = 'kv_store'

  constructor(private db: D1Database, opts?: Partial<{initialize: boolean, table: string}>) {
    this.db = db
    if (opts?.table) {
      this.table = opts.table
    }
    if (opts?.initialize) {
      this.init()
    }
  }

  init() {
    if (!this.initialization) {
      this.initialization = this.db
        .exec(`CREATE TABLE IF NOT EXISTS ${this.table} (key TEXT PRIMARY KEY, value TEXT)`)
      this.initialization.then(() => this.initialized = true)
    }
    return this.initialization
  }

  async set(key: string, value: any) {
    if (!this.initialized) {
      await this.init()
    }
    return this.db.prepare(`INSERT OR REPLACE INTO ${this.table} (key, value) VALUES (?, ?)`)
      .bind(key, value === undefined ? 'null' : JSON.stringify(value))
      .run()
  }

  async get(key: string) {
    if (!this.initialized) {
      await this.init()
    }
    const val = await this.db.prepare(`SELECT value FROM ${this.table} WHERE key = ?`)
      .bind(key)
      .first()
    if (val) {
      return JSON.parse(val.value as string)
    }
  }

  keys(pattern: string) {
    const escape = pattern.includes('_') || pattern.includes('%')
    if (escape) {
      pattern = pattern.replaceAll('_', '\\_').replaceAll('%', '\\%')
    }
    pattern = pattern.replaceAll('?', '_').replaceAll('*', '%')
    return this.db.prepare(`SELECT key FROM '${this.table}' where key like '${pattern}'${escape ? " ESCAPE '\\'" : ''}`)
      .bind()
      .all()
      .then(rows => rows.results.map(x => x.key))
  }

  async del(...keys: string[]) {
    if (!this.initialized) {
      await this.init()
    }
    return this.db.prepare(`DELETE FROM ${this.table} WHERE key IN (${keys.map(_ => '?').join(',')})`)
      .bind(...keys).run()
  }
}