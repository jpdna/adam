package org.bdgenomics.adam.dataset

/**
 * Created by jp on 4/14/16.
 */
import org.apache.spark.Logging
import org.apache.spark.sql.{ SQLContext, DataFrame, Dataset }
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.RecordGroupDictionary

import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

// Already defined in MarkDuplicatesDS
//case class AlignmentRecordSeqHolder(myreads: Seq[AlignmentRecordLimitProjDS] = Seq.empty)

object MarkDuplicatesDSSlow extends Serializable with Logging {

  // Calculates the sum of the phred scores that are greater than or equal to 15
  def score(r: AlignmentRecordLimitProjDS): Int = {

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
      r.inferredInsertSize)).qualityScores.filter(15 <=).sum

  }

  private def scoreBucket(bucket: SingleReadBucketDS): Int = {
    bucket.primaryMapped.map(score).sum
  }

  private def markReadsInBucket(bucket: SingleReadBucketDS, primaryAreDups: Boolean, secondaryAreDups: Boolean) {
    bucket.primaryMapped.foreach(read => {
      read.duplicateRead = primaryAreDups
    })
    bucket.secondaryMapped.foreach(read => {
      read.duplicateRead = secondaryAreDups
    })
    bucket.unmapped.foreach(read => {
      read.duplicateRead = false
    })
  }

  private def markReads(reads: Iterable[(ReferencePositionPairDS, SingleReadBucketDS)], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[(ReferencePositionPairDS, SingleReadBucketDS)], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[(ReferencePositionPairDS, SingleReadBucketDS)] = None) = MarkReads.time {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read._2, primaryAreDups, secondaryAreDups)
    })
  }

  def markduplicates(df: DataFrame,
                     rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    val emptyRgs = rgd.recordGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(rg => {
      log.warn("Library ID is empty for record group %s from sample %s.".format(rg.recordGroupName,
        rg.sample))
    })

    if (emptyRgs.nonEmpty) {
      log.warn("For duplicate marking, all reads whose library is unknown will be treated as coming from the same library.")
    }

    // Group by library and left position

    def leftPositionAndLibrary(p: (ReferencePositionPairDS, SingleReadBucketDS),
                               rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
      //val curr_refpospair = ReferencePositionPairDS(p)
      if (p._2.allReads.head.recordGroupName != null) {
        (p._1.read1refPos, rgd(p._2.allReads.head.recordGroupName).library.getOrElse(null))
      } else {
        (p._1.read1refPos, null)
      }
    }

    def rightPosition(p: (ReferencePositionPairDS, SingleReadBucketDS)): Option[ReferencePositionDS] = {
      p._1.read2refPos
    }

    import sqlContext.implicits._

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName))
      .mapGroups {
        (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
          {
            val (mapped, unmapped) = reads.partition(_.readMapped)
            val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
            val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
            val myRefPosPair = (ReferencePositionPairDS(curr_bucket), curr_bucket)

            val currARSeqholder = AlignmentRecordSeqHolder(curr_bucket.allReads)

            val pos = currARSeqholder.myreads(0).start

            val currPosition2 = if (curr_bucket.allReads.head.recordGroupName != null) {
              (myRefPosPair._1.read1refPos, rgd(curr_bucket.allReads.head.recordGroupName).library.getOrElse(null))
            } else {
              (myRefPosPair._1.read1refPos, null)
            }

            (currPosition2, curr_bucket)
            //(ReferencePositionPairDS(curr_bucket), curr_bucket)
          }
      }
      .groupBy(_._1).flatMapGroups {
        //case ((referencePosition: Option[ReferencePositionDS], recordGroupName: String), myreads: Iterator[(ReferencePositionPairDS, SingleReadBucketDS)]) => {
        (poskey: (Option[ReferencePositionDS], String), myreads: Iterator[((Option[ReferencePositionDS], String), SingleReadBucketDS)]) =>
          {

            val leftPos: Option[ReferencePositionDS] = poskey._1
            val readsAtLeftPos: Seq[(ReferencePositionPairDS, SingleReadBucketDS)] = myreads.map(x => (ReferencePositionPairDS(x._2), x._2)).toSeq

            //val readsAtLeftPos: Iterable[(Option[ReferencePositionDS, String)], SingleReadBucketDS)] = myreads.toSeq
            leftPos match {

              // These are all unmapped reads. There is no way to determine if they are duplicates
              case None =>
                markReads(readsAtLeftPos, areDups = false)

              // These reads have their left position mapped
              case Some(leftPosWithOrientation) =>

                val readsByRightPos = readsAtLeftPos.groupBy(rightPosition)

                val groupCount = readsByRightPos.size

                readsByRightPos.foreach(e => {

                  val rightPos = e._1
                  val reads = e._2

                  val groupIsFragments = rightPos.isEmpty

                  // We have no pairs (only fragments) if the current group is a group of fragments
                  // and there is only one group in total
                  val onlyFragments = groupIsFragments && groupCount == 1

                  // If there are only fragments then score the fragments. Otherwise, if there are not only
                  // fragments (there are pairs as well) mark all fragments as duplicates.
                  // If the group does not contain fragments (it contains pairs) then always score it.
                  if (onlyFragments || !groupIsFragments) {
                    // Find the highest-scoring read and mark it as not a duplicate. Mark all the other reads in this group as duplicates.
                    val highestScoringRead = reads.max(ScoreOrdering)
                    markReadsInBucket(highestScoringRead._2, primaryAreDups = false, secondaryAreDups = true)
                    markReads(reads, primaryAreDups = true, secondaryAreDups = true, ignore = Some(highestScoringRead))
                  } else {
                    markReads(reads, areDups = true)
                  }
                }
                )
            }

            readsAtLeftPos.flatMap(read => { read._2.allReads })
          }

      }

  }

  private object ScoreOrdering extends Ordering[(ReferencePositionPairDS, SingleReadBucketDS)] {
    override def compare(x: (ReferencePositionPairDS, SingleReadBucketDS), y: (ReferencePositionPairDS, SingleReadBucketDS)): Int = {
      // This is safe because scores are Ints
      scoreBucket(x._2) - scoreBucket(y._2)
    }
  }

}
