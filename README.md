# compare-s3-objects
compare two sets of s3 objects



## Notes

spark-shell --master yarn --deploy-mode client \
    --driver-memory 66G \
    --executor-memory 66G --executor-cores 12 --num-executors 10 \
    --conf spark.driver.maxResultSize=20g

sc.getConf.getAll.sorted.foreach(println)

rows.rdd.partitions.size
rows.rdd.getNumPartitions
