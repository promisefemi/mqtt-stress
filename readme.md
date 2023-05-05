## A Stress test that simulates a producer and a consumer.
** This code is not designed to be pretty **


### Producer 
The producer simulates sensor publishing messages to an mqtt broker

```
-b: Broker IP Address (default "127.0.0.1:8000")
-p: Interval for each connection to send message (default 5)
-q: QOS level (default 1)
-s: Number of clients to simulate (default 1)
-t: Time to run in seconds (default 10)
```

### Consumer
The consumer simulates shared wildcard subscription

```
-b: Broker IP Address (default "127.0.0.1:8000")
-q: QOS level (default 1)
-s: Number of clients to simulate (default 1)
```