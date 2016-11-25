package org.bdgenomics.adam.hbase

import org.bdgenomics.adam.util.ADAMFunSuite
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }

/**
 * Created by paschallj on 11/25/16.
 */
class HBaseSuite extends ADAMFunSuite {

  val hbaseConn = new HBaseFunctions.ADAMHBaseConnection(sc)

  sparkBefore("Create HBase test database") {
    HBaseFunctions.createHBaseGenotypeTable(hbaseConn, "testADAM")
  }

  sparkTest("Save data from a VCF into HBase") {

  }

  sparkTest("Read data from HBase into VariantContextRDD and convert to GentypeRDD") {}

  sparkAfter("Delete HBase test database") {
    hbaseConn.admin.disableTable(TableName.valueOf("testADAM"))
    hbaseConn.admin.deleteTable(TableName.valueOf("testADAM"))
  }

}
