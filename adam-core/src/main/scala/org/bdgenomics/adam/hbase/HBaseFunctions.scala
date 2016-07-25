package org.bdgenomics.adam.hbase

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.spark._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Filter, Projection }
import org.bdgenomics.adam.rdd
import org.bdgenomics.adam.rdd.variation.VariantContextRDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.apache.hadoop.hbase.client.Put
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
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.HTable

import org.bdgenomics.formats.avro.Variant
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.adam.models.VariantContext

import scala.collection.mutable.ListBuffer
import sys.process._

/**
 * Created by jp on 7/23/16.
 */

object HBaseFunctions {

  def saveHBaseAlignmentsPut(sc: SparkContext, aRdd: AlignmentRecordRDD, hbaseTableName: String, hbaseColFam: String, hbaseCol: String): Unit = {
    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    val rdd1 = aRdd.rdd.zipWithUniqueId()
    val rdd2 = rdd1.repartition(32)
    //val rdd2 = rdd1

    val rdd1Bytes = rdd2.mapPartitions((iterator) => {
      val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      val alignmentRecordDatumWriter: DatumWriter[AlignmentRecord] = new SpecificDatumWriter[AlignmentRecord](scala.reflect.classTag[AlignmentRecord].runtimeClass.asInstanceOf[Class[AlignmentRecord]])
      val myList = iterator.toList
      myList.map((putRecord) => {
        baos.reset()
        val myRowKey = Bytes.toBytes(putRecord._1.getContigName + "_" + String.format("%10s", putRecord._1.getStart.toString).replace(' ', '0') + "_" + putRecord._2)
        alignmentRecordDatumWriter.write(putRecord._1, encoder)
        encoder.flush()
        baos.flush()
        (myRowKey, Bytes.toBytes(hbaseColFam), Bytes.toBytes(hbaseCol), baos.toByteArray)
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


  def loadHBaseAlignments(sc: SparkContext, hbaseTableName: String, hbaseColFam: String, hbaseCol: String, start: String = null, stop: String = null): RDD[AlignmentRecord] = {

    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)
    scan.addColumn(Bytes.toBytes(hbaseColFam), Bytes.toBytes(hbaseCol))

    if (!start.isEmpty) scan.setStartRow(Bytes.toBytes(start))
    if (!stop.isEmpty) scan.setStopRow(Bytes.toBytes(stop))

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


  def saveHBaseGenotypesSingleSample(sc: SparkContext, vcRdd: VariantContextRDD, hbaseTableName: String): Unit = {
    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    val data = vcRdd.rdd
    val dataGroupedByRowKey = data.groupBy(c => c.variant.variant.getContigName + "_" + c.variant.variant.getStart.toString.replace(' ', '0') + "_" + c.variant.variant.getAlternateAllele)

    val variantContextHBaseRows: RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])] = dataGroupedByRowKey.mapPartitions((iterator) => {
      //val myList = iterator.toList
      val genotypebaos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val genotypeEncoder = EncoderFactory.get().binaryEncoder(genotypebaos, null)
      val genotypeDatumWriter: DatumWriter[Genotype] = new SpecificDatumWriter[Genotype](scala.reflect.classTag[Genotype].runtimeClass.asInstanceOf[Class[Genotype]])

      iterator.map((putRecord) => {
        //val myRowKey = Bytes.toBytes(putRecord.variant.variant.getContigName + "_" + String.format("%10s", putRecord.variant.variant.getStart.toString).replace(' ', '0') + "_" + putRecord.variant.variant.getAlternateAllele)
        genotypebaos.reset()
        val myRowKey = Bytes.toBytes(putRecord._1)

        // item.genotypes.toList.head is selected because this function assumes that vcRdd contains only genotypes from a single sample
        // genotypeAtCurrRowKey contains all the Genotype objects for this sample which have the same rowKey (start position and allele)
        // This is necessary because some VCF files contain multiple rows with same start position and alt allele
        val genotypeAtCurrRowKey: Iterable[Genotype] = putRecord._2.map(item => item.genotypes.toList.head)

        val sampleId = genotypeAtCurrRowKey.head.getSampleId

        //packs multiple genotypes with the the same rowKey into a single HBase value
        genotypeAtCurrRowKey.foreach(genotypeDatumWriter.write(_, genotypeEncoder))

        genotypeEncoder.flush()
        genotypebaos.flush()

        (myRowKey, Bytes.toBytes("g"), Bytes.toBytes(sampleId), genotypebaos.toByteArray)

      })
    })

    variantContextHBaseRows.hbaseBulkPut(hbaseContext,
      TableName.valueOf(hbaseTableName),
      (putRecord) => {
        val put = new Put(putRecord._1)
        put.addColumn(putRecord._2, putRecord._3, putRecord._4)
        put
      })

  }


  def loadHBaseGenotypes(sc: SparkContext, hbaseTableName: String, sampleIDs: List[String]): RDD[Genotype] = {
    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    sampleIDs.foreach(sampleId => {
      scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId))
    })

    val resultHBaseRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

    val result: RDD[Genotype] = resultHBaseRDD.mapPartitions((iterator) => {
      iterator.flatMap((curr) => {

        var resultList = new ListBuffer[Genotype]

        while (curr._2.advance()) {
          val myVal = curr._2.value

          val genotypeDatumReader: DatumReader[Genotype] = new SpecificDatumReader[Genotype](scala.reflect.classTag[Genotype].runtimeClass.asInstanceOf[Class[Genotype]])
          val decoder = DecoderFactory.get().binaryDecoder(myVal, null)

          while (!decoder.isEnd) {
            resultList += genotypeDatumReader.read(null, decoder)
          }
        }

        resultList

      })
    })

    result
  }


  /*
// Alternative save functions that uses bulk load via map reduce
// At the moment, doesn't seem to offer performance advantage, more testing is needed on cluster to assess
def saveHBaseAlignmentsBulkLoad(sc: SparkContext, aRdd: AlignmentRecordRDD, hbaseTableName: String, hbaseColFam: String, hbaseCol: String, hbaseStagingFolder: String): Unit = {
  //val rdd1 = aRdd.rdd.repartition(50).zipWithUniqueId()
  val rdd1 = aRdd.rdd.zipWithUniqueId()
  //val stagingFolder = "hdfs://x2.justin.org:8020/user/justin/jptemp5"
  val stagingFolder = hbaseStagingFolder

  val conf = HBaseConfiguration.create()
  val hbaseContext = new HBaseContext(sc, conf)

  val rdd1Bytes = rdd1.mapPartitions((iterator) => {
    val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)
    val alignmentRecordDatumWriter: DatumWriter[AlignmentRecord] = new SpecificDatumWriter[AlignmentRecord](scala.reflect.classTag[AlignmentRecord].runtimeClass.asInstanceOf[Class[AlignmentRecord]])
    val myList = iterator.toList
    myList.map((putRecord) => {
      baos.reset()
      val myRowKey = Bytes.toBytes(putRecord._1.getContigName + "_" + String.format("%10s", putRecord._1.getStart.toString).replace(' ', '0') + "_" + putRecord._2)
      alignmentRecordDatumWriter.write(putRecord._1, encoder)
      encoder.flush()
      baos.flush()

      (myRowKey, Bytes.toBytes(hbaseColFam), Bytes.toBytes(hbaseCol), baos.toByteArray())
    }).iterator
  }
  )

  val familyHBaseWriterOptions = new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions]
  val f1Options = new FamilyHFileWriteOptions("GZ", "ROW", 128, "PREFIX")

  familyHBaseWriterOptions.put(Bytes.toBytes("columnFamily1"), f1Options)

  rdd1Bytes.hbaseBulkLoad(hbaseContext,
    TableName.valueOf(hbaseTableName),
    t => {
      val rowKey = t._1
      val family: Array[Byte] = t._2
      val qualifier = t._3
      val value = t._4
      val keyFamilyQualifier = new KeyFamilyQualifier(rowKey, family, qualifier)
      Seq((keyFamilyQualifier, value)).iterator
    },
    stagingFolder, familyHBaseWriterOptions,
    compactionExclude = false,
    HConstants.DEFAULT_MAX_FILE_SIZE)

  ("hadoop fs -chmod -R 777 " + stagingFolder) !
  val conn = ConnectionFactory.createConnection(conf)
  val load = new LoadIncrementalHFiles(conf)
  load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, conn.getTable(TableName.valueOf(hbaseTableName)), conn.getRegionLocator(TableName.valueOf(hbaseTableName)))

}

*/




}

