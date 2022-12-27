# Memcached Client Library

This library implements basic operations for memcached communication such as Set and Get. It provides an idle pool of 
connections that can be reused as per memcached protocol the server performance is better when connections are 
maintained opened rather than established for every request. For applications critical to the network latency, 
the library provides a profiling interface to collect information about network connections, set or get times, and some 
detailed information for socket operations.

The client instantiated with `NewClient()` is thread safe and therefore can be used in multiple goroutines.

The internal pool of reusable connections is maintained for optimization purposes. The memcached protocol suggests to 
reuse network connections as long as possible rather than close them at the end of every request. This pool size can be
set via `maxIdleConn` parameter of the `NewClient()`. In case of a peak load the new connections will still be allowed to
be created even if the number exceeds the pool size. They will be closed once used so that the preferred pool size
is maintained.

WARNING: The default memcached server time is 2 minutes until it closes inactive connection. This library uses a default
and at this point can only be changed via `Client.idleTimeout`.

## Using The Client Library

This library can be used as a Go package. To create an instance of memcached client

```golang
    // Instantiate
    client, err := memcached.NewClient("127.0.0.1:11211", 100)
    if err != nil {
        return err
    }
	
    // Set data with key "1/2/3" and expiration time 30 seconds
    if err := client.Set(context.Background(), "1/2/3", []byte{"hello"}, 30); err != nil {
        return err
    }
	
    // Get data
    resp, err := client.Get(context.Background(), "1/2/3")
    if err != nil {
        return err
    }
    process(resp.Data)
```

To enable profiler and receive the network io values

```golang
    p := memcached.NewFuncProfiler(func(name string, duration time.Duration) {
        // put any custom code here that can read metric name/duration
        metrics.Record("memcached_profiler", duration, name)
    })
    memcachedClient.SetProfiler(p)
```

To read some client stats

```golang
    
    t := time.NewTicker(1 * time.Second)
    for {
        select {
        case <-c.Done():
            return
        case _ = <-t.C:
            stats := client.GetStats()
            metrics.Save("first_metric", stats.ConnPoolLen, "conn_pool_len")
        }
    }
```

## License
Comcast.github.io is licensed under [Apache License 2.0](LICENSE). Valid-License-Identifier: Apache-2.0

## Code of Conduct
We take our [code of conduct](CODE_OF_CONDUCT.md) very seriously. Please abide by it.

## Contributing
Please read our [contributing guide](CONTRIBUTING.md) for details on how to contribute to our project.