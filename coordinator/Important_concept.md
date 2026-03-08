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
one Redis server/container can contain multiple logical databases (db0, db1, etc.), and db0:keys=4200 means database 0 currently holds 4200 keys.By default, Redis has 16 databases (0–15).You can change this in `redis.conf`: `databases 16`


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

**Regex Notes**

**1️⃣ Non-capturing group `(?: )`**

Use `(?: ... )` when you **need grouping but don’t want to capture it**.

Example:

```
/db(?:10|11|12):keys=(\d+)/
```

But if grouping isn't needed, just write it normally:

```
/db\d+:keys=(\d+)/
```

Rule:

* `( )` → grouping **and** capturing
* `(?: )` → grouping **without capturing**
* no `()` → when grouping isn't needed

---

**2️⃣ Why parentheses are needed with `|`**

`|` splits the **entire regex**, so grouping is required.

Correct:

```
/db(10|11|12):keys=(\d+)/
```

Matches:

```
db10:keys=5
db11:keys=7
db12:keys=9
```

Wrong:

```
/db10|11|12:keys=(\d+)/
```

This actually means:

```
db10
OR
11
OR
12:keys=(\d+)
```

Rule:

```
prefix(alt1|alt2)suffix
```

Use parentheses when `|` should apply only to part of the pattern.


---