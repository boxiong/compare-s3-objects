# compare-s3-objects
compare two sets of s3 objects specified in manifest files

## Notes

* Process a large data set on an EMR master node

spark-shell --master yarn --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g

sc.getConf.getAll.sorted.foreach(println)

rows.rdd.partitions.size
rows.rdd.getNumPartitions


* Sample commands

=== Interactive ===

```
spark-shell \
    --master yarn \
    --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g \
    --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' \
    --jars s3://path-to-jar/S3Differ-1.0.jar
```

```
// Setup
val args = Seq[String](
  "--bucket",
  "placeholder",
  "--aws-principal",
  "placeholder",
  "--aws-credential",
  "placeholder",
  "--public-key",
  "placeholder",
  "--private-key",
  "placeholder",
  "--material-set",
  "placeholder",
  "--material-serial",
  "1",
  "--region",
  "us-east-1",
  "--baseline-manifest-key",
  "placeholder",
  "--manifest-key",
  "placeholder",
  "--diffs-to-show",
  "3"
)

import org.skygate.falcon._
import org.skygate.falcon.S3Differ._
val conf = new Conf(args)
```

```
// Inspect content of a manifest or data object
val encryptionMaterials = S3Reader.buildRsaMaterials(conf.publicKey(), conf.privateKey(),
  conf.materialSet(), conf.materialSerial().toString)
val reader = new S3Reader(conf.awsPrincipal(), conf.awsCredential(),
  encryptionMaterials, conf.region())
val x = reader.readObject("bucket", "manifestKey")
x.foreach(println)
val y = reader.readGzippedObject("bucket", "gzippedObjKey")
y.take(1).foreach(println)
```

```
// Render rows
val rows = S3Differ.readRows(conf, spark, false).cache
rows.printSchema

val data = rows.flatten
data.cache.count

// Render data from a temp view
data.createOrReplaceTempView("data_table")
spark.sql("select * from data_table limit 3").show

// Find matching row(s) based on a qualifer
val matchedRows = rows.select(rows.col("*")).filter("visitorRecord.qualifier = 200000057996741")
matchedRows.flatten.show(3)
```

```
// Group rows by multiple columns
val aggregatedData = data.groupBy($"eventSource_15", $"adUserId_21", $"adBrowserId_20").count.where($"count" > 1).sort($"count".desc)
aggregatedData.cache.show(20, 200)
aggregatedData.select(sum("count")).show

val summary = data.groupBy($"eventSource_15", $"adUserId_21", $"adBrowserId_20").count.describe()
summary.show(20, 200)
```

```
// Dedupe a given set of events in S3
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window

// Partition rows by multiple columns, and dedupe rows per partition based on a tie-breaker
val partitionKey = Seq($"eventSource_15", $"adUserId_21", $"adBrowserId_20")
val tierBreaker = $"qualifier_18"
val partitionedWindow = Window.partitionBy(partitionKey: _*).orderBy(tierBreaker.desc)
val dedupedData = data.withColumn("rownum", row_number.over(partitionedWindow)).where($"rownum" === 1).drop("rownum").cache
dedupedData.groupBy($"eventSource_15", $"adUserId_21", $"adBrowserId_20").count.where($"count" > 1).count

// Identify rows that used to have duplicates
val dedupedRows = dedupedData.join(
    aggregatedData,
    Seq("eventSource_15", "adUserId_21","adBrowserId_20"),
    "left"
).cache
dedupedRows.count
dedupedRows.where("adUserId_21 = '0101d99c99c2f5d331bb1926b4331976e98f7adaf44fbac5390e34ef88b9b154de92'").show
data.where("adUserId_21 = '0101d99c99c2f5d331bb1926b4331976e98f7adaf44fbac5390e34ef88b9b154de92'").orderBy(tierBreaker.desc).show
```

```
// Find out differences between two sets of events specified by manfifest
val diff = S3Differ.process(conf, spark)
diff.show(3)
```

```
// Simulate the case with diffs
val baselineRows = S3Differ.readRows(conf, spark)
baselineRows.cache.count
val rows = baselineRows.sample(false, 0.9999, 1).limit(19690)
rows.cache.count

val diff1 = baselineRows.except(rows)
diff1.cache.count

val updatedRows = diff1.selectExpr("""
  named_struct(
    'eventRecord', eventRecord,
    'recordType', recordType,
    'visitorRecord', named_struct(
      'domain', upper(visitorRecord.domain),
      'eventIdentity', visitorRecord.eventIdentity,
      'eventSource', visitorRecord.eventSource,
      'ignoreIdentityUpdate', false,
      'payload', visitorRecord.payload,
      'qualifier', visitorRecord.qualifier,
      'timestamp', visitorRecord.timestamp + 88,
      'userIdentity', named_struct(
        'adBrowserId', visitorRecord.userIdentity.adBrowserId,
        'adUserId', visitorRecord.userIdentity.adUserId,
        'ksoDeviceId', visitorRecord.userIdentity.ksoDeviceId,
        'sisDeviceId', visitorRecord.userIdentity.sisDeviceId
      )
    )
  ) as bx_struct
""").select($"bx_struct.*")
val newRows = rows.union(updatedRows)

baselineRows.except(newRows).flatten.select($"qualifier_18".alias("qualifier"), $"domain_9", $"timestamp_19").sort($"qualifier").show(20, false)
newRows.except(rows).flatten.select($"qualifier_18".alias("qualifier"), $"domain_9", $"timestamp_19").sort($"timestamp_19").show(20, false)
```

=== Non-Interactive ===
```
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g \
    --class org.skygate.falcon.S3Differ \
    s3://path-to-jar/S3Differ-1.0.jar \
    --bucket "placeholder" \
    --aws-principal "placeholder" \
    --aws-credential "placeholder" \
    --public-key "placeholder" \
    --private-key "placeholder" \
    --material-set "placeholder" \
    --material-serial 1 \
    --region "us-east-1" \
    --baseline-manifest-key "placeholder" \
    --manifest-key "placeholder"
    --diffs-to-show 3 > diff.log 2>&1

grep 'INFO S3Differ' diff.log
```

=== Interactive for small data set ===
```
[Zeppelin]

Step 1: Remote into master node and download the JAR file

$ sudo mkdir /var/bx && sudo chmod 755 /var/bx && cd /var/bx
$ sudo aws s3 cp s3://path-to-jar/S3Differ-1.0.jar .

Step 2: Follow the instructions below to load the JAR via the Zeppelin GUI

https://zeppelin.apache.org/docs/latest/usage/interpreter/dependency_management.html

Step 3: Run adhoc queries from GUI

import org.skygate.falcon._
val conf = new Conf(args)

// Render rows
val rows = S3Differ.readRows(conf, spark, false)
rows.show(3)
z.show(rows.flatten.limit(3))

// Render diff rows
val baselineRows = S3Differ.readRows(conf, spark)
val diff = rows.except(baselineRows)
diff.count
z.show(diff.flatten.limit(3))
```

