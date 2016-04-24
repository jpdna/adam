
package org.bdgenomics.adam.dataset

/**
 * Created by jp on 4/14/16.
 */

import java.lang.Long

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, DataFrame, Dataset }
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferencePosition,
  ReferencePositionPair,
  SingleReadBucket
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

//case class TestARholder(myreads: Seq[AlignmentRecordLimitProjDS] = Seq.empty)

object TestDataset1 extends Serializable with Logging {

  def leftPositionAndLibrary(p: (ReferencePositionPairDS, SingleReadBucketDS),
                             rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
    //val curr_refpospair = ReferencePositionPairDS(p)
    if (p._2.allReads.head.recordGroupName != null) {
      (p._1.read1refPos, rgd(p._2.allReads.head.recordGroupName).library.getOrElse(null))
    } else {
      (p._1.read1refPos, null)
    }
  }

  /*
  def leftPositionAndLibrarytest(p: (ReferencePositionPairDS, SingleReadBucketDS)): Long = {
    //val curr_refpospair = ReferencePositionPairDS(p)
    p._1.read1refPos match {
      case Some(x) => x.pos
      case _       => 0.toLong
    }
  }
  */

  def apply(df: DataFrame,
            rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          //SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          (ReferencePositionPairDS(curr_bucket), curr_bucket)
        }
    }.groupBy(leftPositionAndLibrary(_, rgd)).flatMapGroups {
      case ((referencePosition, recordGroupName), myreads) => {
        myreads.toSeq.flatMap(read => read._2.allReads)
      }
    }

  }

  def testrealgrouped2(df: DataFrame,
                       rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          //SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          (ReferencePositionPairDS(curr_bucket), curr_bucket)
        }
    }.groupBy(leftPositionAndLibrary(_, rgd))

  }

  def testrealgrouped1(df: DataFrame, rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName))

  }

}
/*}
    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName))
      .mapGroups {
        (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
          {
            val (mapped, unmapped) = reads.partition(_.readMapped)
            val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
            //SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
            val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)

            val mypos = leftPositionAndLibrarytest((ReferencePositionPairDS(curr_bucket), curr_bucket))

            (mypos.toString, ReferencePositionPairDS(curr_bucket), curr_bucket)
          }
      }.groupBy(_._1)
      .flatMapGroups {
        //case ((referencePosition, recordGroupName), myreads) => {
        case (x, y) =>
          Seq("Dude")
      }

    */

/*
    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName))
      .mapGroups {
        (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
          {
            val (mapped, unmapped) = reads.partition(_.readMapped)
            val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
            //SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
            val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)

            val mypos = leftPositionAndLibrarytest((ReferencePositionPairDS(curr_bucket), curr_bucket))

            (mypos.toString, ReferencePositionPairDS(curr_bucket), curr_bucket)
          }
      }.groupBy(_._1)
      .flatMapGroups {
        //case ((referencePosition, recordGroupName), myreads) => {
        case (x, y) =>
          Seq("Dude")
      }
    */

