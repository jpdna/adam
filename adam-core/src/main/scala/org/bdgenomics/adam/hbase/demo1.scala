package org.bdgenomics.adam.hbase

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Filter, Projection }
import org.bdgenomics.adam.rdd
import org.bdgenomics.formats.avro.AlignmentRecord
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.file.DataFileWriter
import org.apache.avro.io.DatumWriter
import java.io.ByteArrayOutputStream

import org.apache.avro.io.EncoderFactory
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.client.Scan
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.spark.FamiliesQualifiersValues
import org.apache.hadoop.hbase.spark.ByteArrayWrapper
import org.apache.hadoop.hbase.client.HTable

import sys.process._

/**
 * Created by jp on 7/22/16.
 */
object demo1 {
  def mytest1(sc: SparkContext, aRdd: AlignmentRecordRDD): Unit = {
    val confHbase = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, confHbase)

    val tableName = "jptest1"
    val columnFamily = "jpcf1"

    val rdd = sc.parallelize(Array(
      (Bytes.toBytes("1"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
      (Bytes.toBytes("2"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
      (Bytes.toBytes("3"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
      (Bytes.toBytes("4"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
      (Bytes.toBytes("5"),
        Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))
    ))

    rdd.hbaseBulkPut(hbaseContext, TableName.valueOf(tableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2,
          putValue._3))
        put
      })

  }

}

