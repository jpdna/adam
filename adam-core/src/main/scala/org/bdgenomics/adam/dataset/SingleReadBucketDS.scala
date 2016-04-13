package org.bdgenomics.adam.dataset

import org.apache.spark.sql.{ DataFrame, SQLContext }

/**
 * Created by jp on 4/13/16.
 */

object SingleReadBucketDS {
  //def apply(mypath: String, sqlContext: SQLContext) = {
  def apply(df: DataFrame, sqlContext: SQLContext) = {

    //val adamFile2 = sqlContext.read.load("mypath")
    // adamFile2.registerTempTable("adamFile2Table")

    import sqlContext.implicits._

    println("Hello in Single ReadBuckets")

    val ds = df.as[AlignmentRecordLimitProjDS]

    ds.groupBy(p => (p.recordGroupName, p.readName))
      .mapGroups {
        case ((recordGroupName, readName), reads: Iterator[AlignmentRecordLimitProjDS]) => {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          new SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
        }
      }

  }
}

case class SingleReadBucketDS(
    primaryMapped: Seq[AlignmentRecordLimitProjDS],
    secondaryMapped: Seq[AlignmentRecordLimitProjDS],
    unmapped: Seq[AlignmentRecordLimitProjDS]) {

  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

}

