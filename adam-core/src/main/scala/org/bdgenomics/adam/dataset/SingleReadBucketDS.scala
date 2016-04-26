package org.bdgenomics.adam.dataset

import org.apache.spark.sql.{ Dataset, DataFrame, SQLContext }

/**
 * Created by jp on 4/13/16.
 */

case class SingleReadBucketDS(
    primaryMapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty,
    secondaryMapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty,
    unmapped: Seq[AlignmentRecordLimitProjDS] = Seq.empty) {

  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

}
