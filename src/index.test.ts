import { D1Database } from "@cloudflare/workers-types"
import { Database } from "bun:sqlite"
import { KV } from './'
import { test, expect } from "bun:test"

class MockD1Database {

  db = new Database(":memory:")

  prepare(sql: string) {
    return {
      bind: (...params: Array<any>) => {
        // console.log(sql)
        return {
          all: async () => {
            return { results: this.db.query(sql).all(...params) }
          },
          first: async () => {
            return this.db.query(sql).get(...params)
          },
          run: () => {
            const {changes, lastInsertRowid} = this.db.query(sql).run(...params)
            return { meta: {changes, last_row_id: lastInsertRowid}}
          }
        }
      }
    }
  }

  async exec(sql: string) {
    return this.db.exec(sql)
  }

  async batch(statements: Array<any>) {
    const result: any[] = []
    this.db.transaction(() => {
      for (const stmt of statements) {
        result.push(stmt.run())
      }
    })()
    return result
  }
}

const db = new MockD1Database() as unknown as D1Database

test('set/get', async () => {
  const kv = new KV(db)

  expect(await kv.get('k0')).toBe(undefined)

  await kv.set('k0', null)
  expect(await kv.get('k0')).toBe(null)

  await kv.set('k1', undefined)
  expect(await kv.get('k1')).toBe(null)

  await kv.set('k2', 'val')
  expect(await kv.get('k2')).toBe('val')

  await kv.set('k2', 0)
  expect(await kv.get('k2')).toBe(0)

  await kv.set('list', [])
  expect(await kv.get('list')).toEqual([])

  await kv.set('obj', {key: null})
  expect(await kv.get('obj')).toEqual({key: null})

  await kv.set('k2025', new Date(2025, 0, 1))
  expect(await kv.get('k2025')).toEqual('2025-01-01T00:00:00.000Z')

  expect(await kv.keys('k?')).toEqual(['k0', 'k1', 'k2'])

  expect(await kv.keys('k*')).toEqual(['k0', 'k1', 'k2', 'k2025'])
})

test('keys', async () => {
  const kv = new KV(db, {table: 'keys'})
  await kv.set('k00', 0)
  await kv.set('k_1', 1)
  await kv.set('k_12', 12)
  expect(await kv.keys('k*')).toEqual(['k00', 'k_1', 'k_12'])
  expect(await kv.keys('k_?')).toEqual(['k_1'])

  await kv.del('k00', 'k_12')
  expect(await kv.keys('*')).toEqual(['k_1'])
})

test('mset/mget', async () => {
  const kv = new KV(db, {table: 'mget'})

  expect(await kv.mget('k0')).toEqual({})

  await kv.set('k0', null)
  await kv.set('k1', 1)
  expect(await kv.mget('k0', 'k1')).toEqual({k0: null, k1: 1})

  await kv.mset({k0: 0, k1: 1, k2: 2})
  expect(await kv.mget('k0', 'k1', 'k2')).toEqual({k0: 0, k1: 1, k2: 2})
})

test('incr/decr', async () => {
  const kv = new KV(db, {table: 'incr'})
  expect(await kv.incr('k0')).toBe(1)
  expect(await kv.incr('k0')).toBe(2)
  expect(await kv.decr('k0')).toBe(1)
})