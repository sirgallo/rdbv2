# Load Test


## Locust

Locust is an open source tool written in python for load testing apis


## Test

This test handles mixed load, with
```
80% read
20% write
```

Each user will generate a request between `0.1` and `0.5` seconds. 


## Run

start master:
```
locust -f locustfile.py --master
```

start workers:
```
locust -f locustfile.py --worker
```


## Dashboard

To access the dashboard, open your web browser to `0.0.0.0:8089`. Here, the test can be configured for both concurrency (num of users) and time to ramp up users to max concurrency.


## Results

`System Config`

  Macbook Pro 14 M2Pro with 16GB Ram and 512GB SSD

`Cluster Config`

  Haproxy with 5 nodes running in docker compose with
  ```
    docker desktop resources:
      6 cpus 
      8 GB Ram
      1.5 GB Swap
      64 GB Virtual Storage
  ```

`1000 concurrent users`
```
average rps over 10 minutes: ~2900-3050rps
read operation latency: 8-20ms
write operation latency: 148-350ms
90%ile latency: 160ms
99%ile latency: 300ms
Max latency: 872ms
Average latency (all ops): 35ms
```