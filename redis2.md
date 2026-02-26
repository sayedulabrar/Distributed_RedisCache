### What Are Streams?

A Stream is an append-only log of entries. Each entry has a unique ID (timestamp-sequence) and contains field-value pairs. Messages are never modified after insertionâ€”they're immutable. Streams automatically handle ID generation using timestamps, ensuring messages are ordered.


![](https://raw.githubusercontent.com/poridhiEng/lab-asset/refs/heads/main/Redis%20Labs/Lab%2010/images/4.svg)

Both consumers can—and usually do—**work at the same time** on the same stream, but they handle **different messages**.

Think of it like a supermarket with two checkout lanes (Consumer A1 and Consumer A2) serving one long line of customers (the Stream):

### 1. Simultaneous, Not Sequential

It isn't that A2 sits idle waiting for A1 to crash. Instead:

* **A1** takes Message #1.
* **A2** takes Message #2.
* They both process their respective messages **simultaneously**.
* This is how you "scale out" your processing power. If one consumer isn't fast enough to keep up with the Web App's `XADD` commands, you add A2, A3, and so on.

### 2. The "Backup" Behavior (Claiming)

The "backup" part you mentioned happens only if one of them fails.
If A1 takes Message #1 but then crashes (or the server restarts) before sending an `XACK` (acknowledgment), that message stays in A1's **PEL (Pending Entries List)**.

Redis knows Message #1 was given to A1 but never finished. After a timeout, **Consumer A2** can "claim" that specific message using the `XCLAIM` command and finish the job.

---

### Comparison of Roles

| Scenario | Consumer A1 | Consumer A2 |
| --- | --- | --- |
| **Normal Operation** | Processes Message #1 | Processes Message #2 (at the same time) |
| **High Traffic** | Processes Message #3 | Processes Message #4 |
| **A1 Crashes** | Offline (Message #5 is stuck) | **Claims** Message #5 and finishes it |

### Summary

In the diagram, A1 and A2 are **active teammates**, not a "Primary and Standby." They divide the work to get it done faster. If one teammate trips, the other picks up the ball they dropped.


---

Redis handles all the "heavy lifting" of that algorithm so the consumers don't have to coordinate with each other. This is the core magic of **Consumer Groups**.

The algorithm is strictly managed by the Redis server itself, not the client-side code. Here is how it prevents overlaps:

### 1. The "Read-and-Assign" Logic

When a consumer calls `XREADGROUP`, it doesn't just "look" at the stream; it asks Redis for the next available message.

* **Atomic Pointer:** Redis maintains a special "Last Delivered ID" for the entire group.
* **The Handshake:** When Consumer A1 asks for a message, Redis:
1. Finds the next message ID after the Last Delivered ID.
2. Updates the group's pointer to that new ID.
3. Moves that message into a **Pending Entries List (PEL)** specifically for Consumer A1.


* **The Result:** Because this happens **atomically** inside Redis, it’s physically impossible for Consumer A2 to get that same message ID in the same request. Redis simply points A2 to the *next* message in line.

---

### 2. The Pending Entries List (PEL)

This is the "safety net" algorithm. If you look under the hood of a Consumer Group, Redis keeps a ledger for every consumer:

| Consumer | Messages "Checked Out" (PEL) | Status |
| --- | --- | --- |
| **A1** | [Message #101, Message #103] | Processing... |
| **A2** | [Message #102, Message #104] | Processing... |

A message only leaves the PEL when the consumer sends an **`XACK`** (Acknowledgment).

### 3. Avoiding Overlap During Failures

What if A1 crashes? Does Redis just give the message to A2 automatically? **No.** If Redis did that automatically, we might end up with "Double Processing" if A1 was just slow, not dead.

* **Explicit Claiming:** Another consumer (like A2) must specifically run an algorithm called **`XPENDING`** to see which messages have been stuck for too long.
* **Ownership Transfer:** Only then does A2 use **`XCLAIM`**. This command tells Redis: *"Move Message #101 from A1's PEL to my PEL because A1 hasn't responded in 10 seconds."*

---

### Summary Table: How Redis prevents "Double Work"

| Mechanism | Purpose |
| --- | --- |
| **Internal Pointer** | Ensures the same "New" message is never given to two people. |
| **PEL (State)** | Tracks who is currently responsible for which message. |
| **XACK** | Formally deletes the message from the "active work" list. |
| **Visibility Timeout** | Ensures that "Stuck" messages aren't touched by others unless a specific time threshold is met. |

It’s a bit like **database locking** . Just as a row-level lock prevents two processes from editing the same record simultaneously, Redis's internal pointer and PEL act as a distributed lock for stream messages.
