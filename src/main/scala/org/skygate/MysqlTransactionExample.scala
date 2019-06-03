package org.skygate

import org.apache.spark.sql.SparkSession

object MysqlTransactionExample {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    val simpleDf = sparkSession.read
      .format("org.skygate.datasourcev2.simple")
      .load()
      .as[String]

    val errorDf = simpleDf.map(value => {
      if (value == "3") throw new IllegalArgumentException("value cannot be 3")
      else value
    })

    //errorDf.show()

    // results in partial writes
    errorDf.write
      .format(
        "org.skygate.datasourcev2.simplemysqlwriter")
      .save()

    //use transactional ones

    errorDf.write
      .format(
        "org.skygate.datasourcev2.mysqlwithtransaction")
      .save()

    sparkSession.stop()

  }
}
