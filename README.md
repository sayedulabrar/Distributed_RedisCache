# Distributed_Cache_Lab-02


## Compare behaviors

### Sequential (slow)

```js
for (const node of this.nodes.values()) {
  await node.client.connect();
}
```

Timeline:

```
A finishes → B starts → C starts → D starts
```

Total time = A + B + C + D

---

### Concurrent (fast)

```js
const promises = [];
for (const node of this.nodes.values()) {
  promises.push(node.client.connect());
}
await Promise.all(promises);
```

Timeline:

```
A, B, C, D start together
wait for all
```

Total time = max(A, B, C, D)

---

### One-liner (same behavior)

```js
await Promise.all(
  [...this.nodes.values()].map(n => n.client.connect())
);
```

Still concurrent — `.map()` starts them first.

---

## Mental model

### Rule to remember

* **calling async fn → starts work**
* **await / Promise.all → waits**

---

## Quick analogy

Think:

* `connect()` → press "start"
* `Promise.all()` → "wait until all are done"

---

So yes — `Promise.all` is commonly used *with* parallel work, but it doesn’t create the parallelism itself. The function calls do.