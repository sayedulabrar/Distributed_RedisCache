# Distributed_Cache_Lab-01
### Use `axios.get / post / put` when:

* Method is known at compile time
* Code is short and explicit
* Typical frontend API calls

```js
axios.get("/users");
axios.post("/users", data);
```

---

### Use `axios(config)` when:

* Method is dynamic
* You’re building a proxy, gateway, or SDK
* You want one reusable request function
* You’re forwarding requests (like here)

```js
axios({
  method,
  url,
  headers,
  data,
  timeout
});
```