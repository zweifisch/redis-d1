# d1-kv

d1 as kv store.

```js
import { KV } from 'd1-kv'

const kv = new KV(ctx.env.db)

await kv.set('k0', 0)
await kv.get('k0') // 0
await kv.set('k1', '1')
await kv.get('k1') // '1'

await kv.keys('k*') // ['k0', 'k1']

await kv.del('k0', 'k1')
```

## Supported Commands

- set
- get
- keys
- del
- mset
- mget

## Multiple Stores

```js
const kv2 = new KV(ctx.env.db, {table: 'kv_store2'})
```