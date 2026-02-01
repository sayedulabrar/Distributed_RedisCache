## 1. What are `info('stats')` and `info('keyspace')`?

### Redis `INFO` command

Redis exposes internal metrics via:

```
INFO <section>
```

Each section returns **plain text**, e.g.:

```
# Stats
keyspace_hits:12345
keyspace_misses:678
total_commands_processed:99999
```

and

```
# Keyspace
db0:keys=4200,expires=1200,avg_ttl=987654
```

Your code is calling:

```js
node.client.info('stats')
node.client.info('keyspace')
```

So the variables contain **strings**, not objects.

---


## JavaScript `String.match()` + Regex Capturing Groups — Quick Notes

### Basic rule

```js
str.match(regex)
```

Returns:

```
[ fullMatch, group1, group2, group3, ... ]
```

* `[0]` → entire matched text
* `[1+]` → captured groups (from parentheses)

---

### Capturing groups depend on `()`

Number of results = **number of `()` pairs**

Example:

```js
/db0:keys=(\d+)/
```

Only **one** group → result:

```js
["db0:keys=4200", "4200"]
```

So:

* match[0] = full match
* match[1] = keys
* match[2+] ❌ never exists


### Multiple groups example

```js
/db(\d+):keys=(\d+),expires=(\d+)/
```

Result:

```js
[
  "db0:keys=4200,expires=1200",
  "0",
  "4200",
  "1200"
]
```


### Multiple matches behavior

| Method          | Multiple matches | Capture groups      |
| --------------- | ---------------- | ------------------- |
| `match()`       | ❌ first only     | ✅ yes               |
| `match(/.../g)` | ✅ yes            | ❌ no                |
| `matchAll()`    | ✅ yes            | ✅ yes (recommended) |

Use:

```js
[...str.matchAll(regex)]
```

when you need **all matches + groups**.


### Non-capturing group

If you don’t need a group:

```js
(?:...)
```

Prevents creating extra array entries.

Example:

```js
/db(?:\d+):keys=(\d+)/
```

---

### Redis example

```js
const m = keyspace.match(/db0:keys=(\d+)/);
const keys = m ? parseInt(m[1]) : 0;
```

---

**Rule of thumb:**
👉 Count `()` → that’s how many `groupN` you get.

# So how do you get multiple matches + groups?

Use:

## Option A — `matchAll()` (best modern solution)

```js
const matches = [...keyspace.matchAll(/db\d+:keys=(\d+)/g)];
```

### Return shape

Each element behaves like a normal `.match()` result:

```js
[
  ["db0:keys=4200", "4200"],
  ["db1:keys=3000", "3000"]
]
```

### Usage

```js
for (const m of matches) {
  console.log(m[1]); // key count
}
```

---

---

## Option B — manual parsing

```js
keyspace.split('\n')
```

But regex is cleaner.

---

---

# Summary table

| Method          | Multiple matches | Capture groups |
| --------------- | ---------------- | -------------- |
| `match()`       | ❌ first only     | ✅ yes          |
| `match(/.../g)` | ✅ yes            | ❌ no           |
| `matchAll()`    | ✅ yes            | ✅ yes (best)   |

---

---

# Practical Redis example (all DBs)

If you want **all DB key counts**:

```js
const counts = [...keyspace.matchAll(/db\d+:keys=(\d+)/g)]
  .map(m => parseInt(m[1]));

const totalKeys = counts.reduce((a, b) => a + b, 0);
```

---

# Rule of thumb

If you ever need:

> multiple matches + captured values

Always use:

```
matchAll()
```
