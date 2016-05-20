package org.bdgenomics.adam.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.{ TableName, HBaseConfiguration }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.spark.HBaseContext

/**
 * Created by jp on 5/20/16.
 */
object Hbasetest1 {
  def mytestfunc(): String = { "Justin is cool" }
}
