package com.github.xcloureiro.newday

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

abstract class BaseSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
  private val master = "local"
  private val appName = "testing"

  private val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.driver.allowMultipleContexts","true")

  implicit var sc: SparkContext = _
  implicit var sql: HiveContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sc = SparkContext.getOrCreate(conf)
    sql = new HiveContext(sc)
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
      sql = null
    } finally {
      super.afterAll()
    }
  }
}