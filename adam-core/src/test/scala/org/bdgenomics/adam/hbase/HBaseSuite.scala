package org.bdgenomics.adam.hbase

import org.bdgenomics.adam.util.ADAMFunSuite
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.Matchers
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.Genotype

/**
 * Created by paschallj on 11/25/16.
 */
class HBaseSuite extends ADAMFunSuite {

  //val hbaseConn = new HBaseFunctions.ADAMHBaseConnection(sc)

  sparkBefore("Create HBase test database") {
    //HBaseFunctions.createHBaseGenotypeTable(hbaseConn, "testADAM")

  }

  sparkTest("Save data from a VCF into HBase") {

  }

  sparkTest("Load data from  HBase") {

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
    //when(dao.getHBaseRDD(TableName.valueOf("myTable"), scan)).thenReturn(loadHBaseRDD)
    when(dao.getHBaseRDD(Matchers.anyObject(), Matchers.anyObject())).thenReturn(loadHBaseRDD)

    val samples = List("NA12878")
    //val x = HBaseFunctions.loadGenotypesFromHBaseToVariantContextRDD(dao, "myTable", samples, "seqDictID" )

    val genoData: Genotype = HBaseFunctions.loadVariantContextsFromHBase(dao, "myTable", Some(samples))
      .take(1)(0).genotypes.toList.head

    assert(genoData.getSampleId === "NA12878")

    /*
    println("X: " + x)

    val z: Array[VariantContext] = x.take(1)
    val z2: VariantContext = z(0)

    println("z: " + z)
    println("z2: " + z2)
    println("z2.variant: " + z2.variant)
    println("z2: " + z2.genotypes.toList.head)

    assert(x2.)
*/
  }

  sparkTest("Read data from HBase into VariantContextRDD and convert to GentypeRDD") {}

  sparkAfter("Delete HBase test database") {
    //hbaseConn.admin.disableTable(TableName.valueOf("testADAM"))
    //hbaseConn.admin.deleteTable(TableName.valueOf("testADAM"))
  }

}
