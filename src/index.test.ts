import { D1Database, D1PreparedStatement } from "@cloudflare/workers-types"
import { Database } from "bun:sqlite"
import { KV } from './'
import { test, expect } from "bun:test"

class MockD1Database {

  db = new Database(":memory:")

  prepare(sql: string) {
    return {
      bind: (...params: Array<any>) => {
        // console.log(sql)
        const stmt = this.db.prepare(sql, params)
        return {
          first: async () => {
            return stmt.get()
          },
          run: async () => {
            if (sql.trimStart().startsWith('SELECT') || sql.trimStart().startsWith('select')) {
              return { results: stmt.all() }
            }
            const {changes, lastInsertRowid} = stmt.run()
            return { meta: {changes, last_row_id: lastInsertRowid} }
          }
        }
      }
    }
  }

  async exec(sql: string) {
    return this.db.exec(sql)
  }

  async batch(statements: Array<D1PreparedStatement>) {
    const result: any[] = []
    this.db.transaction(() => {
      for (const stmt of statements) {
        result.push(stmt.run())
      }
    })()
    return Promise.all(result)
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

  await kv.set('k2', '1')
  expect(await kv.get('k2')).toBe('1')

  await kv.set('k2', 'val')
  expect(await kv.get('k2')).toBe('val')

  await kv.set('k2', 0)
  expect(await kv.get('k2')).toBe(0)

  expect(await kv.set('k2', 1, {nx: true})).toBeFalse()
  expect(await kv.get('k2')).toBe(0)

  expect(await kv.set('nx', 1, {nx: true})).toBeTrue()
  expect(await kv.get('nx')).toBe(1)

  await kv.set('bool', true)
  expect(await kv.get('bool')).toEqual(true)

  await kv.set('bool', false)
  expect(await kv.get('bool')).toEqual(false)

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

  await kv.mset({k0: 0, k1: 2, k2: 3})
  expect(await kv.mget('k0', 'k1', 'k2')).toEqual({k0: 0, k1: 2, k2: 3})
})

test('incr/decr', async () => {
  const kv = new KV(db, {table: 'incr'})
  expect(await kv.incr('k0')).toBe(1)
  expect(await kv.incr('k0')).toBe(2)
  expect(await kv.decr('k0')).toBe(1)
})

test('lists', async () => {
  const kv = new KV(db, {table: 'lists'})

  expect(await kv.llen('l1')).toBe(0)
  await kv.lpush('l1', 1)
  await kv.lpush('l1', 2)
  expect(await kv.lrange('l1', 0, 1)).toEqual([1, 2])
  expect(await kv.lpop('l1')).toEqual(1)
  expect(await kv.lrange('l1', 0, -1)).toEqual([2])
  expect(await kv.llen('l1')).toBe(1)
  await kv.lpush('l1', 'true')
  expect(await kv.get('l1')).toEqual([2, 'true'])
  expect(await kv.lrange('l1', 0, -1)).toEqual([2, 'true'])
  await kv.lpush('l1', {ok: true})
  expect(await kv.get('l1')).toEqual([2, 'true', {ok: true}])
  expect(await kv.lrange('l1', 0, -1)).toEqual([2, 'true', {ok: true}])

  expect(await kv.lindex('l1', 0)).toEqual(2)
  expect(await kv.lindex('l1', 1)).toEqual('true')
  expect(await kv.lindex('l1', -1)).toEqual({ok: true})

  expect(await kv.lpop('l2')).toEqual(undefined)
  await kv.lpush('l2', 2)
  await kv.lpush('l2', 3)
  expect(await kv.lrange('l2', 0, -1)).toEqual([2, 3])
  expect(await kv.rpop('l2')).toEqual(3)
  expect(await kv.lrange('l2', 0, -1)).toEqual([2])
  expect(await kv.rpop('l2')).toEqual(2)
  expect(await kv.lrange('l2', 0, -1)).toEqual([])
  expect(await kv.rpop('l2')).toEqual(null)

  for (const c of 'abcbbabc') {
    await kv.lpush('l3', c)
  }
  await kv.lrem('l3', 2, 'b')
  expect(await kv.lrange('l3', 0, -1)).toEqual([... 'acbabc'])
  await kv.lrem('l3', -1, 'a')
  expect(await kv.lrange('l3', 0, -1)).toEqual([... 'acbbc'])
  await kv.lrem('l3', 0, 'c')
  expect(await kv.lrange('l3', 0, -1)).toEqual([... 'abb'])
})

test('rpush', async () => {
  const kv = new KV(db, {table: 'rpush'})
  await kv.rpush('l1', 2)
  expect(await kv.lrange('l1', 0, 1)).toEqual([2])
  await kv.rpush('l1', 1)
  expect(await kv.lrange('l1', 0, 0)).toEqual([1])
  await kv.rpush('l1', '0')
  expect(await kv.lrange('l1', 0, -1)).toEqual(['0', 1, 2])
  await kv.rpush('l1', '0')
  expect(await kv.lrange('l1', 0, -1)).toEqual(['0', '0', 1, 2])
  await kv.rpush('l1', {ok: false})
  expect(await kv.get('l1')).toEqual([{ok: false}, '0', '0', 1, 2])
  expect(await kv.lrange('l1', 0, -1)).toEqual([{ok: false}, '0', '0', 1, 2])

  for (const c of [...'abcbbabc'].reverse()) {
    await kv.rpush('l2', c)
  }
  await kv.lrem('l2', -2, 'b')
  expect(await kv.lrange('l2', 0, -1)).toEqual([... 'abcbac'])
})

test('pop', async () => {
  const kv = new KV(db, {table: 'pop'})
  await kv.lpush('k', [])
  expect(await kv.lpop('k')).toEqual([])

  await kv.lpush('k', {})
  expect(await kv.rpop('k')).toEqual({})
})

test('ttl', async () => {
  const kv = new KV(db, {table: 'ttl'})
  await kv.set('k', 1)
  expect(await kv.ttl('k')).toEqual(-1)
  await kv.expire('k', 1)
  expect(await kv.ttl('k')).toEqual(1)

  await kv.expire('k', 0)
  expect(await kv.ttl('k')).toEqual(-2)
  expect(await kv.get('k')).toBeUndefined()

  await kv.set('k2', 1, {ex: 1})
  expect(await kv.get('k2')).toEqual(1)
  expect(await kv.ttl('k2')).toEqual(1)

  await kv.set('k2', 1, {ex: 0})
  expect(await kv.get('k2')).toBeUndefined()
})

test('hset/hget', async () => {
  const kv = new KV(db, {table: 'hash'})
  expect(await kv.hget('h', 'f')).toBeUndefined()
  await kv.hset('h', 'f', 1)
  expect(await kv.hget('h', 'f')).toBe(1)
  expect(await kv.hget('h', 'f2')).toBeNull()
  await kv.hset('h', 'f', '2')
  expect(await kv.hget('h', 'f')).toBe('2')
  await kv.hset('h', 'f', '{"ok": true}')
  expect(await kv.hget('h', 'f')).toBe('{"ok": true}')
  expect(await kv.hgetall('h')).toEqual({f: '{"ok": true}'})

  await kv.hset('h', 'f', {ok: true})
  expect(await kv.hget('h', 'f')).toEqual({ok: true})
  expect(await kv.hgetall('h')).toEqual({f: {ok: true}})
  expect(await kv.get('h')).toEqual({f: {ok: true}})
})
