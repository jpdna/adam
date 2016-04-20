package org.bdgenomics.adam.dataset

/**
 * Created by jp on 4/14/16.
 */
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
//import org.bdgenomics.adam.rdd.read.MarkDuplicates._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

object MarkDuplicatesDS extends Serializable with Logging {

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

  private def markReads(reads: Iterable[SingleReadBucketDS], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[SingleReadBucketDS], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[SingleReadBucketDS] = None) = MarkReads.time {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read, primaryAreDups, secondaryAreDups)
    })
  }

  // This second version of markRead2 is needed because one usage required read: Iterable[] parameter and the
  // other a reads: TraversableOnce parameter and couldn't find yet a way to convert between those in the call
  /* private def markReads2(reads: Iterable[SingleReadBucketKeyedByRefPosPairDS], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                         ignore: Option[SingleReadBucketKeyedByRefPosPairDS] = None) = MarkReads.time {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read.singleReadBucket, primaryAreDups, secondaryAreDups)
    })
  }
*/

  def apply(df: DataFrame,
            rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    // do we have record groups where the library name is not set? if so, print a warning message
    // to the user, as all record groups without a library name will be treated as coming from
    // a single library
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
    /* def leftPositionAndLibrary(p: SingleReadBucketKeyedByRefPosPairDS,
                               rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
      if (p.singleReadBucket.allReads.head.recordGroupName != null) {
        (p.referencePositionPair.read1refPos, rgd(p.singleReadBucket.allReads.head.recordGroupName).library.getOrElse(null))
      } else {
        (p.referencePositionPair.read1refPos, null)
      }
    }   */

    /*
    def leftPositionAndLibrary(p: (ReferencePositionPairDS, SingleReadBucketDS),
                               rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
      if (p._2.allReads.head.recordGroupName != null) {
        (p._1.read1refPos, rgd(p._2.allReads.head.recordGroupName).library.getOrElse(null))
      } else {
        (p._1.read1refPos, null)
      }
    }
    */
    /*
    def leftPositionAndLibrary(p: (ReferencePositionPairDS, SingleReadBucketDS),
                               rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
      //val curr_refpospair = ReferencePositionPairDS(p)
      if (p._2.allReads.head.recordGroupName != null) {
        (p._1.read1refPos, rgd(p._2.allReads.head.recordGroupName).library.getOrElse(null))
      } else {
        (p._1.read1refPos, null)
      }
    }
    */

    def leftPositionAndLibrary(p: SingleReadBucketDS,
                               rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
      val curr_refpospair = ReferencePositionPairDS(p)
      if (p.allReads.head.recordGroupName != null) {
        //(curr_refpospair.read1refPos, rgd(p.allReads.head.recordGroupName).library.getOrElse(null))
        (curr_refpospair.read1refPos, p.allReads.head.recordGroupName)
      } else {
        (curr_refpospair.read1refPos, null)
      }
    }

    /*
    // Group by right position
    def rightPosition(p: SingleReadBucketKeyedByRefPosPairDS): Option[ReferencePositionDS] = {
      p.referencePositionPair.read2refPos
    }
    */
    def rightPosition(p: SingleReadBucketDS): Option[ReferencePositionDS] = {
      val curr_refpospair = ReferencePositionPairDS(p)
      curr_refpospair.read2refPos
    }

    import sqlContext.implicits._
    /*

    SingleReadBucketKeyedByRefPosPairDS(df, sqlContext)
      .groupBy(leftPositionAndLibrary(_, rgd))
      .flatMapGroups {
        case ((referencePosition, recordGroupName), singleReadBucketKeyedByRefPosPairDS) => {

          val leftPos: Option[ReferencePositionDS] = referencePosition
          val readsAtLeftPos: Iterable[SingleReadBucketKeyedByRefPosPairDS] = singleReadBucketKeyedByRefPosPairDS.toSeq

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
                  markReadsInBucket(highestScoringRead.singleReadBucket, primaryAreDups = false, secondaryAreDups = true)
                  markReads(reads, primaryAreDups = true, secondaryAreDups = true, ignore = Some(highestScoringRead))
                } else {
                  markReads(reads, areDups = true)
                }
              })
          }

          readsAtLeftPos.flatMap(read => { read.singleReadBucket.allReads })
        }

      }

*/

    //SingleReadBucketDS(df, sqlContext)

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //(ReferencePositionPairDS(curr_bucket), curr_bucket)
        }
    }.groupBy(leftPositionAndLibrary(_, rgd))
      .flatMapGroups {
        case (x, z) => Seq("Dude")
      }

    //(x: Option[ReferencePositionDS], thing: String), bcuketread: Iterator[SingleReadBucketDS]) => Seq("dude")
    //case ((x, y), z) => {
    //Seq("dude")
    //((x,y),z)) => Seq("dude") }

    //(Option( thing2, thing3) => Seq("Dude")
    //case ((referencePosition, recordGroupName), singleReadBucketKeyedByRefPosPairDS) => {
    // Seq("Dude")
    /*


          val leftPos: Option[ReferencePositionDS] = referencePosition
          val readsAtLeftPos: Iterable[SingleReadBucketKeyedByRefPosPairDS] = singleReadBucketKeyedByRefPosPairDS.toSeq

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
                  markReadsInBucket(highestScoringRead.singleReadBucket, primaryAreDups = false, secondaryAreDups = true)
                  markReads(reads, primaryAreDups = true, secondaryAreDups = true, ignore = Some(highestScoringRead))
                } else {
                  markReads(reads, areDups = true)
                }
              })
          }

          readsAtLeftPos.flatMap(read => { read.singleReadBucket.allReads })
        }
          */
    // }
    //}

    /*
    SingleReadBucketKeyedByRefPosPairDS(df, sqlContext)
      .groupBy(_.testPos)
      .flatMapGroups {
        case (referencePosition, singleReadBucketKeyedByRefPosPairDS) => {
          Seq(100)

          /*
          val leftPos: Option[ReferencePositionDS] = referencePosition
          val readsAtLeftPos: Iterable[SingleReadBucketKeyedByRefPosPairDS] = singleReadBucketKeyedByRefPosPairDS.toSeq

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
                  markReadsInBucket(highestScoringRead.singleReadBucket, primaryAreDups = false, secondaryAreDups = true)
                  markReads(reads, primaryAreDups = true, secondaryAreDups = true, ignore = Some(highestScoringRead))
                } else {
                  markReads(reads, areDups = true)
                }
              })
          }

          readsAtLeftPos.flatMap(read => { read.singleReadBucket.allReads })
         */

        }

      }

*/
  }
  /*
  private object ScoreOrdering extends Ordering[SingleReadBucketKeyedByRefPosPairDS] {
    override def compare(x: SingleReadBucketKeyedByRefPosPairDS, y: SingleReadBucketKeyedByRefPosPairDS): Int = {
      // This is safe because scores are Ints
      scoreBucket(x.singleReadBucket) - scoreBucket(y.singleReadBucket)
    }
  }
*/
  private object ScoreOrdering extends Ordering[SingleReadBucketDS] {
    override def compare(x: SingleReadBucketDS, y: SingleReadBucketDS): Int = {
      // This is safe because scores are Ints
      scoreBucket(x) - scoreBucket(y)
    }
  }
}