package org.skygate.falcon

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val bucket = opt[String](required = true)
  val principal = opt[String](required = true)
  val credential = opt[String](required = true)
  val publicKey = opt[String](required = true)
  val privateKey = opt[String](required = true)
  val materialSet = opt[String](required = true)
  val materialSerial = opt[Int](default = Some(1))
  val region = opt[String](default = Some("us-east-1"))

  val baselineManifest = opt[String](required = true)
  val manifest = opt[String](required = true)

  verify()
}

object S3Differ {

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

  def convertToRows(lines: Seq[String], spark: SparkSession) : Dataset[Row] = {
    import spark.implicits._

    val step = 1000
    val segments = lines.sliding(step, step).toSeq
    val frames = segments.map(segment => {
      val frame = spark.read.json(segment.toDS)
      frame
    })
    println(s"${segments.length} segments => ${frames.length} frames")

    val rows = frames.reduce(_ union _)
    rows
  }

  def readRows(reader: S3Reader, bucket: String, manifestKey: String,
    spark: SparkSession) : Dataset[Row] = {

    val manifest = reader.readObject(bucket, manifestKey)
    val lines = reader.readObjects(bucket, manifest)
    val rows = convertToRows(lines, spark)
    println(s"rows.count = ${rows.count} from ${manifestKey}")

    rows
  }

  def process(conf: Conf, spark: SparkSession) : Dataset[Row] = {
    val encryptionMaterials = S3Reader.buildRsaMaterials(conf.publicKey(), conf.privateKey(),
      conf.materialSet(), conf.materialSerial().toString)
    val reader = new S3Reader(conf.principal(), conf.credential(),
      encryptionMaterials, conf.region())

    val baselineRows = readRows(reader, conf.bucket(), conf.baselineManifest(), spark)
    val rows = readRows(reader, conf.bucket(), conf.manifest(), spark)

    rows.except(baselineRows)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]")
      .appName("S3DifferApp")
      .getOrCreate()

    val conf = new Conf(args)
    process(conf, spark)
  }

}
