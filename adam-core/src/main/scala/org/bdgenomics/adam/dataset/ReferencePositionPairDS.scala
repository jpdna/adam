package org.bdgenomics.adam.dataset

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.spark.Logging
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{ ReferencePosition, SingleReadBucket }
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.{ Strand, AlignmentRecord }
import Ordering.Option
import org.apache.spark.Logging
//import org.bdgenomics.adam.instrumentation.Timers.CreateReferencePositionPair
import org.bdgenomics.adam.models.ReferenceRegion._
import org.bdgenomics.adam.rich.RichAlignmentRecord

object ReferencePositionPairDS extends Logging {
  def apply(singleReadBucketDS: SingleReadBucketDS): ReferencePositionPairDS = {
    val firstOfPair = (singleReadBucketDS.primaryMapped.filter(_.readInFragment == 0) ++
      singleReadBucketDS.unmapped.filter(_.readInFragment == 0)).toSeq
    val secondOfPair = (singleReadBucketDS.primaryMapped.filter(_.readInFragment == 1) ++
      singleReadBucketDS.unmapped.filter(_.readInFragment == 1)).toSeq

    def getPos(r: AlignmentRecordLimitProjDS): ReferencePosition = {
      if (r.readMapped) {
        RichAlignmentRecord.recordToRichRecord(new AlignmentRecord(r.readInFragment,
          r.contigName,
          r.start,
          r.oldPosition,
          r.end,
          r.mapq,
          r.readName,
          r.sequence,
          r.qual,
          r.cigar,
          r.oldCigar,
          r.basesTrimmedFromStart,
          r.basesTrimmedFromEnd,
          r.readPaired,
          r.properPair,
          r.readMapped,
          r.mateMapped,
          r.failedVendorQualityChecks,
          r.duplicateRead,
          r.readNegativeStrand,
          r.mateNegativeStrand,
          r.primaryAlignment,
          r.secondaryAlignment,
          r.supplementaryAlignment,
          r.mismatchingPositions,
          null,
          null,
          r.recordGroupName,
          r.recordGroupSample,
          r.mateAlignmentStart,
          r.mateAlignmentEnd,
          r.mateContigName,
          r.inferredInsertSize)).fivePrimeReferencePosition

      } else {
        ReferencePosition(r.sequence, 0L)
      }
    }

    if (firstOfPair.size + secondOfPair.size > 0) {
      new ReferencePositionPairDS(
        firstOfPair.lift(0).map(getPos),
        secondOfPair.lift(0).map(getPos)
      )
    } else {
      new ReferencePositionPairDS(
        (singleReadBucketDS.primaryMapped ++
          singleReadBucketDS.unmapped).toSeq.lift(0).map(getPos),
        None
      )
    }
  }
}

case class ReferencePositionPairDS(
  read1refPos: Option[ReferencePosition],
  read2refPos: Option[ReferencePosition])
