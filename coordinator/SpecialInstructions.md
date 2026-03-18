### Case 1: `setTimeout` with recursive scheduling

Example:

```js
function runTask() {
  doWork();           // takes time
  setTimeout(runTask, 2000);
}

runTask();
```

#### If `doWork()` takes longer than 2 seconds

Example timeline:

```
0s   runTask starts
0-5s doWork runs (takes 5s)
5s   setTimeout scheduled
7s   next runTask starts
```

Important points:

* `setTimeout` is scheduled **after `doWork()` finishes**
* So the delay starts **after completion**
* **No overlapping executions**

Effective gap = `doWork time + timeout delay`

```
next run = work_time + delay
```

---

### Case 2: `setInterval`

```js
setInterval(() => {
  doWork();
}, 2000);
```

If `doWork()` takes **5 seconds**:

Timeline:

```
0s   doWork starts
2s   interval fires again
4s   interval fires again
```

But JavaScript is **single-threaded**, so callbacks queue in the **event loop**.

Result:

```
0s   doWork start
5s   doWork end
5s   next queued callback runs immediately
```

So executions can **pile up in the queue**, causing bursts.

Example:

```
0s  run
5s  run immediately
10s run immediately
```

This can cause:

* CPU spikes
* Memory pressure
* Unpredictable scheduling

---

### Why recursive `setTimeout` is safer

Pattern:

```js
async function runTask() {
  await doWork();
  setTimeout(runTask, 2000);
}

runTask();
```

Guarantees:

```
doWork finishes → wait → run again
```

No overlapping
No queue buildup

---

### Production example (health checks / polling)

```js
async function healthCheckLoop() {
  try {
    await checkNodes();
  } catch (err) {
    console.error(err);
  }

  setTimeout(healthCheckLoop, 5000);
}

healthCheckLoop();
```

This ensures **only one health check runs at a time**.

---

✅ **Rule of thumb**

| Use               | When                                      |
| ----------------- | ----------------------------------------- |
| `setTimeout` loop | async tasks, network calls, health checks |
| `setInterval`     | lightweight tasks like UI updates         |

---
