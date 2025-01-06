# redis-d1

d1 as kv store.

```js
import { KV } from 'redis-d1'

const kv = new KV(ctx.env.db)

await kv.set('k0', 0)
await kv.get('k0') // 0
await kv.set('k1', {kv: true})
await kv.get('k1') // {kv: true}

await kv.keys('k*') // ['k0', 'k1']
```

## Supported Commands

- decr
- del
- expire
- get
- hdel
- hget
- hgetall
- hset
- incr
- keys
- lindex
- lpop
- lpush
- lrange
- lrem
- lset
- mget
- mset
- rpop
- rpush
- set
- ttl

## Multiple Stores

```js
const kv2 = new KV(ctx.env.db, {table: 'kv_store2'})
```
