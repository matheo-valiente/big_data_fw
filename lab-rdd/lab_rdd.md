# Lab: Spark RDD API

Bronze `users.csv` and `orders.csv` downloaded from S3, processed in `pyspark --master local[*]` (Spark 4.1.1).

## Partitions and lazy evaluation
```python
orders_lines = sc.textFile("orders.csv")        # 2 partitions, repartition(8) -> 8
header = orders_lines.first()
orders_data = orders_lines.filter(lambda l: l != header and l.strip())
orders_data.count()                              # 2829
```
`filter` only builds a lineage (`toDebugString()`); each `count()` runs a new job unless the RDD is cached.

## Multiline addresses
```python
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda l: UUID.match(l) is not None)  # 101 lines -> 50 users
```
`textFile` splits on every newline, even inside quoted addresses; `spark.read.csv(multiLine=True)` does not.

## Parsing and aggregation
```python
orders_rdd = orders_data.map(parse_order).cache()  # split(",") into a typed dict
pairs = orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
pairs.reduceByKey(add)                # combines per partition, then shuffles
pairs.groupByKey().mapValues(sum)     # same totals, but shuffles every value
```

## Deduplication and join
```python
orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()  # 2829 -> 2829
orders_kv.join(users_kv)              # 2829 rows: (user_id, (order, username))
```
No duplicate `order_id` in the file. The `reduceByKey` shuffle plays the role of `PARTITION BY order_id`.

## Exercises
- **aggregateByKey**: `(order_count, total)` per user in one pass, e.g. `(4, 12)` -> average 3.0.
- **leftOuterJoin**: after injecting a fake `user_id`, 1 orphan order with username `None`.
- **DataFrame**: `groupBy("user_id").sum("quantity")`; `explain()` shows `partial_sum` -> `Exchange` -> `sum`, chosen by Catalyst.
- **Cache**: ~0.22 s without vs ~0.13 s with (5 runs); small gain because the file is only 330 KB.
