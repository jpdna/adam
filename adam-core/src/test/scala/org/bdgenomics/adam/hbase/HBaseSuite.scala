package org.bdgenomics.adam.hbase

import org.bdgenomics.adam.util.ADAMFunSuite
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.mockito.{ ArgumentCaptor, Matchers, Mockito }
import org.mockito.Mockito._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.adam.rdd.ADAMContext._

/**
 * Created by paschallj on 11/25/16.
 */
class HBaseSuite extends ADAMFunSuite {

  sparkBefore("Create HBase test database") {
  }

  sparkTest("Save data from a VCF into HBase") {

    val inputVariantContext = sc.loadVcf(testFile("small.vcf"))

    val dao = Mockito.mock(classOf[HBaseFunctions.HBaseSparkDAO])

    val mockTable = Mockito.mock(classOf[org.apache.hadoop.hbase.client.Table])
    when(dao.getTable(Matchers.anyObject())).thenReturn(mockTable)

    val genodataCaptor = ArgumentCaptor.forClass(classOf[RDD[(Array[Byte], List[(String, Array[Byte])])]])
    val hbaseTableNameCaptor = ArgumentCaptor.forClass(classOf[String])
    val flatMapCaptor = ArgumentCaptor.forClass(classOf[scala.Function1[(Array[Byte], List[(String, Array[Byte])]), scala.Iterator[scala.Tuple2[org.apache.hadoop.hbase.spark.KeyFamilyQualifier, scala.Array[scala.Byte]]]]])
    val stagingFolderCaptor = ArgumentCaptor.forClass(classOf[String])

    HBaseFunctions.saveVariantContextRDDToHBaseBulk(dao, inputVariantContext, "mytable1", "mySeqDict", true, None, "mystagingfolder")

    verify(dao).hbaseBulkLoad(genodataCaptor.capture(),
      hbaseTableNameCaptor.capture(),
      flatMapCaptor.capture(),
      stagingFolderCaptor.capture())

    println("genodataCatpor: " + genodataCaptor.getValue.take(1))

  }

  sparkTest("Load data from  HBase using KeyStrategy1") {

    val dao = Mockito.mock(classOf[HBaseFunctions.HBaseSparkDAO])

    val loadValueInput1: Array[Byte] = Array(49, 95, 48, 48, 48, 48, 48, 49, 52, 51, 57, 54, 95, 67, 84, 71, 84, 95, 67, 95, 52)
    val loadValue1 = new org.apache.hadoop.hbase.io.ImmutableBytesWritable(loadValueInput1)
    val loadValue2array: Array[Byte] = Array(2, 2, -106, 2, 0, 0, 0, 2, 8, 67, 84, 71, 84, 2, 2, 67, 0, 0, 0, 2, 2, 49, 2, -8,
      -32, 1, 2, -128, -31, 1, 2, 2, 0, 2, 14, 73, 110, 100, 101, 108, 81, 68, 0, 0, 0, 2, -23, 38, -7, 64, 2, 82,
      -72, -42, 65, 2, 0, 2, -49, -9, -13, -65, 2, -90, -101, -60, 62, 0, 0, 0, 0, 0, 2, 14, 78, 65, 49, 50, 56, 55,
      56, 0, 0, 4, 0, 2, 0, 0, 2, 32, 2, 8, 2, 40, 0, 2, -58, 1, 6, 0, -68, -116, -85, 0, 0, -128, -1, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0)

    val myCell: Cell = new KeyValue(Bytes.toBytes("1_0000014396_CTGT_C_4"), Bytes.toBytes("g"), Bytes.toBytes("NA12878"), 100L, loadValue2array)
    val myCellArray = Array(myCell)
    val myResult: Result = Result.create(myCellArray)

    val myHBaseTypeArray = Array((loadValue1, myResult))

    println("test1: " + myHBaseTypeArray)

    val loadHBaseRDD = sc.parallelize(myHBaseTypeArray)

    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    println("loadHBaseRDD: " + loadHBaseRDD)

    when(dao.getHBaseRDD(Matchers.anyObject(), Matchers.anyObject())).thenReturn(loadHBaseRDD)

    val samples = List("NA12878")

    val genoData: Genotype = HBaseFunctions.loadVariantContextsFromHBase(dao, "myTable", Some(samples))
      .take(1)(0).genotypes.toList.head

    assert(genoData.getSampleId === "NA12878")

  }

  sparkTest("Read data from HBase into VariantContextRDD and convert to GentypeRDD") {}

  sparkAfter("Delete HBase test database") {
  }

}
