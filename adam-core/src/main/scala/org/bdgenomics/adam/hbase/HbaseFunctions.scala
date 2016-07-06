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
 * Created by justin on 7/3/16.
 */
object HbaseFunctions {
  def mytestfunc2(sc: SparkContext, aRdd: AlignmentRecordRDD): Unit = {

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

  def saveHbaseAlignments(sc: SparkContext, aRdd: AlignmentRecordRDD, hbaseTableName: String, hbaseColFam: String, hbaseCol: String): Unit = {

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    val rdd1 = aRdd.rdd
    val rdd2 = rdd1.repartition(20)

    val rdd1Bytes = rdd2.mapPartitions((iterator) => {
      val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      val alignmentRecordDatumWriter: DatumWriter[AlignmentRecord] = new SpecificDatumWriter[AlignmentRecord](scala.reflect.classTag[AlignmentRecord].runtimeClass.asInstanceOf[Class[AlignmentRecord]])
      val myList = iterator.toList
      myList.map((putRecord) => {
        baos.reset()
        val myRowKey = (Bytes.toBytes(putRecord.getContigName.toString + "_" + String.format("%10s", putRecord.getStart.toString).replace(' ', '0')))
        alignmentRecordDatumWriter.write(putRecord, encoder)
        encoder.flush()
        baos.flush()
        (myRowKey, Bytes.toBytes(hbaseColFam), Bytes.toBytes(hbaseCol), baos.toByteArray())
      }).iterator
    }
    )

    rdd1Bytes.hbaseBulkPut(hbaseContext,
      TableName.valueOf(hbaseTableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        put.addColumn(putRecord._2, putRecord._3, putRecord._4)
      })

  }

  def loadHbaseAlignments(sc: SparkContext, hbaseTableName: String, hbaseColFam: String, hbaseCol: String, start: String, stop: String): RDD[AlignmentRecord] = {

    val scan = new Scan()
    scan.setCaching(100)

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)
    scan.addColumn(Bytes.toBytes(hbaseColFam), Bytes.toBytes(hbaseCol))
    scan.setStartRow(Bytes.toBytes(start))
    scan.setStopRow(Bytes.toBytes(stop))

    val getRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

    val result: RDD[AlignmentRecord] = getRDD.mapPartitions((iterator) => {
      val cf_bytes = Bytes.toBytes(hbaseColFam)
      val qual_bytes = Bytes.toBytes(hbaseCol)

      val myList = iterator.toList
      myList.flatMap((curr) => {

        val myValList = curr._2.getColumnCells(cf_bytes, qual_bytes)
        myValList.map((myVal) => {
          val alignmentRecordDatumReader: DatumReader[AlignmentRecord] = new SpecificDatumReader[AlignmentRecord](scala.reflect.classTag[AlignmentRecord].runtimeClass.asInstanceOf[Class[AlignmentRecord]])
          val decoder = DecoderFactory.get().binaryDecoder(CellUtil.cloneValue(myVal), null)
          myVal.getValueArray()
          alignmentRecordDatumReader.read(null, decoder)
        })
      }).iterator
    }
    )

    result
  }

}
