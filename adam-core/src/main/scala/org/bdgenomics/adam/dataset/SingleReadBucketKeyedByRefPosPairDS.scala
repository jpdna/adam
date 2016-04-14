package org.bdgenomics.adam.dataset

import org.apache.spark.sql.{ SQLContext, DataFrame }

object SingleReadBucketKeyedByRefPosPairDS {

  def apply(df: DataFrame, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    val ds = df.as[AlignmentRecordLimitProjDS]

    ds.groupBy(p => (p.recordGroupName, p.readName))
      .mapGroups {
        case ((recordGroupName, readName), reads: Iterator[AlignmentRecordLimitProjDS]) => {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val currSingleReadBucketDS = new SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          new SingleReadBucketKeyedByRefPosPairDS(ReferencePositionPairDS(currSingleReadBucketDS),
            currSingleReadBucketDS)
        }
      }

  }

}

case class SingleReadBucketKeyedByRefPosPairDS(
  referencePositionPair: ReferencePositionPairDS,
  singleReadBucket: SingleReadBucketDS)