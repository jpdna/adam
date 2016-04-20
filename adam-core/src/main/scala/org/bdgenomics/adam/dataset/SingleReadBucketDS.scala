package org.bdgenomics.adam.dataset

import org.apache.spark.sql.{ Dataset, DataFrame, SQLContext }

/**
 * Created by jp on 4/13/16.
 */

/*
object SingleReadBucketDS {
  def apply(myds: Dataset[AlignmentRecordLimitProjDS]) = {

    //import sqlContext.implicits._
    //myds..

    //val ds = df.as[AlignmentRecordLimitProjDS]

    myds.groupBy(p => (p.recordGroupName, p.readName))
    /*.mapGroups {
        case ((recordGroupName, readName), reads: Seq[AlignmentRecordLimitProjDS]) => {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = new SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          (ReferencePositionPairDS(curr_bucket), curr_bucket)
        } */
  }

}
*/

case class SingleReadBucketDS(
    primaryMapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty,
    secondaryMapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty,
    unmapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty) {

  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

}

/*
object SingleReadBucketDS {
  def apply(myds: Dataset[AlignmentRecordLimitProjDS], sqlContext: SQLContext) = {

    import sqlContext.implicits._
    //myds..

    //val ds = df.as[AlignmentRecordLimitProjDS]

    myds.groupBy(p => (p.recordGroupName, p.readName))
      .mapGroups {
        case ((recordGroupName, readName), reads: Seq[AlignmentRecordLimitProjDS]) => {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = new SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          (ReferencePositionPairDS(curr_bucket), curr_bucket)
        }
      }

  }
}

case class SingleReadBucketDS(
    primaryMapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty,
    secondaryMapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty,
    unmapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty) {

  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

}

*/

/*
object SingleReadBucketDS {
  def apply(df: DataFrame, sqlContext: SQLContext) = {

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

*/

