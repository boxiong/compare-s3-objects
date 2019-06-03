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


```
spark-shell \
    --master yarn \
    --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g \
    --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' \
    --jars s3://path-to-jar/S3Differ-1.0.jar


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
val conf = new Conf(args)

// Render diff rows
val diff = S3Differ.process(conf, spark)
diff.show(3)
diff.select(S3Differ.flattenSchema(diff.schema) : _*).show(3)


// Render baseline rows
val baselineRows = S3Differ.readRows(conf, spark)
baselineRows.show(3)
baselineRows.select(S3Differ.flattenSchema(baselineRows.schema) : _*).show(3)

// Render rows
val rows = S3Differ.readRows(conf, spark, false)
rows.show(3)
rows.select(S3Differ.flattenSchema(rows.schema) : _*).show(3)
rows.select(S3Differ.flattenSchema(rows.schema) : _*).limit(3).select($"adUserId", $"adBrowserId").show(20, false)


// Find matching row(s) based on the qualifer identified from a baseline row
val matchedRows = rows.select(rows.col("*")).filter("visitorRecord.qualifier = 200000057996741")
matchedRows.select(S3Differ.flattenSchema(matchedRows.schema) : _*).show(3)


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
z.show(rows.select(S3Differ.flattenSchema(rows.schema) : _*).limit(3))

// Render diff rows
val baselineRows = S3Differ.readRows(conf, spark)
val diff = rows.except(baselineRows)
diff.count
z.show(diff.select(S3Differ.flattenSchema(diff.schema) : _*).limit(3))
```