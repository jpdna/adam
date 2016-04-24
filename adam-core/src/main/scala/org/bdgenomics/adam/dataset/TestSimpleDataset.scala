
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

case class TestARholder(myreads: Seq[AlignmentRecordLimitProjDS] = Seq.empty)

/*
object TestPositionHolder extends Logging {
  def apply(testARholder: TestARholder): TestPositionHolder = {


      if()
      TestPositionHolder( testARholder.myreads.lift(0).map(x => x.start) )
  }
}
*/

case class TestPositionHolder(mypos: Option[Long])

object TestSimpleDataset extends Serializable with Logging {

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
          //val (mapped, unmapped) = reads.partition(_.readMapped)
          //val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          //SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //(ReferencePositionPairDS(curr_bucket), curr_bucket)
          val currARholder = TestARholder(reads.toSeq)

          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          ((currPosition, "dude"), currARholder)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myIter) =>
        {
          myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod1(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)

          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          ((currPosition, "dude"), currARholder)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myIter) =>
        {
          myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod5(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)

          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          ((currPosition, "dude"), currARholder, curr_bucket)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod6(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)

          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          ((currPosition, "dude"), currARholder)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod7(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          //val currARholder = TestARholder(curr_bucket.allReads)
          val currARholderPrimary = TestARholder(curr_bucket.primaryMapped)

          //val pos = currARholder.myreads(0).start
          val pos = currARholderPrimary.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          ((currPosition, "dude"), currARholderPrimary)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod8(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          //val currARholder = TestARholder(curr_bucket.allReads)
          val currARholderPrimary = TestARholder(curr_bucket.primaryMapped)
          val currARholderSecondary = TestARholder(curr_bucket.secondaryMapped)

          //val pos = currARholder.myreads(0).start
          val pos = currARholderPrimary.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          ((currPosition, "dude"), currARholderPrimary, currARholderSecondary)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod9(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          //val currARholder = TestARholder(curr_bucket.allReads)
          val currARholderPrimary = TestARholder(curr_bucket.primaryMapped)
          val currARholderSecondary = TestARholder(curr_bucket.secondaryMapped)
          val currARholderUnmapped = TestARholder(curr_bucket.unmapped)

          //val pos = currARholder.myreads(0).start
          val pos = currARholderPrimary.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          ((currPosition, "dude"), currARholderPrimary, currARholderSecondary, currARholderUnmapped)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod10(df: DataFrame,
                rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)
          //val currARholderPrimary = TestARholder(curr_bucket.primaryMapped)
          //val currARholderSecondary = TestARholder(curr_bucket.secondaryMapped)
          //val currARholderUnmapped = TestARholder(curr_bucket.unmapped)

          val pos = currARholder.myreads(0).start
          //val pos = currARholderPrimary.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          ((currPosition, "dude"), currARholder)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  def testmod11(df: DataFrame,
                rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val myRefPosPair = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)
          //val currARholderPrimary = TestARholder(curr_bucket.primaryMapped)
          //val currARholderSecondary = TestARholder(curr_bucket.secondaryMapped)
          //val currARholderUnmapped = TestARholder(curr_bucket.unmapped)

          val pos = currARholder.myreads(0).start
          //val pos = currARholderPrimary.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))
          val currPosition2 = if (curr_bucket.allReads.head.recordGroupName != null) {
            (myRefPosPair._1.read1refPos, rgd(curr_bucket.allReads.head.recordGroupName).library.getOrElse(null))
          } else {
            (myRefPosPair._1.read1refPos, null)
          }

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          //((currPosition, "dude"), currARholder)
          (currPosition2, currARholder)
        }
    }.groupBy(_._1).flatMapGroups {
      (pos, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.myreads)
          //myreads.toSeq.flatMap(v => v._2.allReads)
          //myIter.toSeq.flatMap(x => x._2.myreads)
        }
    }

  }

  /*
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



  private def markReads(reads: Iterable[SingleReadBucketDS], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[SingleReadBucketDS], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[(ReferencePositionPairDS, SingleReadBucketDS)] = None) = MarkReads.time {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read, primaryAreDups, secondaryAreDups)
    })
  }

*/

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

  def testmod12(df: DataFrame,
                rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    def rightPosition(p: (ReferencePositionPairDS, SingleReadBucketDS)): Option[ReferencePositionDS] = {
      p._1.read2refPos
    }

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          val myRefPosPair = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)
          //val currARholderPrimary = TestARholder(curr_bucket.primaryMapped)
          //val currARholderSecondary = TestARholder(curr_bucket.secondaryMapped)
          //val currARholderUnmapped = TestARholder(curr_bucket.unmapped)

          val pos = currARholder.myreads(0).start
          //val pos = currARholderPrimary.myreads(0).start

          //val finalpos =

          //logic from leftPositionAndLibrary function
          val currPosition = TestPositionHolder(Option(pos))
          val currPosition2 = if (curr_bucket.allReads.head.recordGroupName != null) {
            (myRefPosPair._1.read1refPos, rgd(curr_bucket.allReads.head.recordGroupName).library.getOrElse(null))
          } else {
            (myRefPosPair._1.read1refPos, null)
          }

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          //((currPosition, "dude"), currARholder)
          (currPosition2, currARholder)
        }
    }.groupBy(_._1).flatMapGroups {
      (poskey: (Option[ReferencePositionDS], String), myreads: Iterator[((Option[ReferencePositionDS], String), TestARholder)]) =>
        {

          //make the SingleReadBuckets
          val readsAtLeftPos: Seq[(ReferencePositionPairDS, SingleReadBucketDS)] = myreads.map(x => {
            val (mapped, unmapped) = x._2.myreads.partition(_.readMapped)
            val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
            val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
            val myRefPosPair = ReferencePositionPairDS(curr_bucket)
            (myRefPosPair, curr_bucket)
          }).toSeq

          val leftPos = poskey._1

          leftPos match {

            case None =>
              markReads(readsAtLeftPos, areDups = false)

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

  /*
          SingleReadBucketDS())

        val (mapped, unmapped) = myreads.toSeq.flatMap(v => v._2.myreads).partition(_.readMapped)



        val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
        val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
        val myRefPosPair = (ReferencePositionPairDS(curr_bucket), curr_bucket)

        val leftPos: Option[ReferencePositionDS]  = pos._1
        val readsAtLeftPos = curr_b



        val allreads = myreads.toSeq.flatMap(v => v._2.allReads)


        //val (mapped, unmapped) = myreads.partition(_.readMapped)
        myreads.toSeq.flatMap(v => v._2.myreads)
        //myreads.toSeq.flatMap(v => v._2.allReads)
        //myIter.toSeq.flatMap(x => x._2.myreads)
        */
  //}
  //}

  //}

  def leftposgrouper1(p: (ReferencePositionPairDS, SingleReadBucketDS),
                      rgd: RecordGroupDictionary): (Option[ReferencePositionDS], String) = {
    //val curr_refpospair = ReferencePositionPairDS(p)
    if (p._2.allReads.head.recordGroupName != null) {
      (p._1.read1refPos, rgd(p._2.allReads.head.recordGroupName).library.getOrElse(null))
    } else {
      (p._1.read1refPos, null)
    }
  }

  def testmod2(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)
          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          (ReferencePositionPairDS(curr_bucket), curr_bucket)
        }
    }.groupBy(leftposgrouper1(_, rgd)).flatMapGroups {

      case ((referencePosition, recordGroupName), myreads) => {
        myreads.toSeq.flatMap(read => read._2.allReads)
      }
      /*(pos, myIter) =>
      {
        myIter.toSeq.flatMap(x => x._2.myreads)
      } */

    }

  }

  def leftposgrouper2(p: (ReferencePositionPairDS, SingleReadBucketDS),
                      rgd: RecordGroupDictionary): (TestPositionHolder, String) = {
    //val curr_refpospair = ReferencePositionPairDS(p)

    val currbucket = p._2
    val currARholder = TestARholder(currbucket.allReads)
    val pos = currARholder.myreads(0).start
    val currPosition = TestPositionHolder(Option(pos))
    (currPosition, "dude")

    /*if (p._2.allReads.head.recordGroupName != null) {
      (p._1.read1refPos, rgd(p._2.allReads.head.recordGroupName).library.getOrElse(null))
    } else {
      (p._1.read1refPos, null)*/
  }

  def testmod3(df: DataFrame,
               rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {

          //The old stuff
          val (mapped, unmapped) = reads.partition(_.readMapped)
          val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //val my_test = (ReferencePositionPairDS(curr_bucket), curr_bucket)

          //val currARholder = TestARholder(reads.toSeq)
          val currARholder = TestARholder(curr_bucket.allReads)
          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          //((currPosition, "dude"), currARholder)
          (ReferencePositionPairDS(curr_bucket), curr_bucket)
        }
    }.groupBy(leftposgrouper2(_, rgd)).flatMapGroups {
      (x, myreads) =>
        {
          myreads.toSeq.flatMap(v => v._2.allReads)
        }

      /*
    case ((referencePosition, recordGroupName), myreads) => {
        myreads.toSeq.flatMap(read => read._2.allReads)
      }*/
      /*(pos, myIter) =>
      {
        myIter.toSeq.flatMap(x => x._2.myreads)
      } */

    }

  }

  def test1onlygroup(df: DataFrame,
                     rgd: RecordGroupDictionary, sqlContext: SQLContext) = {

    import sqlContext.implicits._

    df.as[AlignmentRecordLimitProjDS].groupBy(p => (p.recordGroupName, p.readName)).mapGroups {
      (k, reads: Iterator[AlignmentRecordLimitProjDS]) =>
        {
          //val (mapped, unmapped) = reads.partition(_.readMapped)
          //val (primaryMapped, secondaryMapped) = mapped.partition(_.primaryAlignment)
          //SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //val curr_bucket = SingleReadBucketDS(primaryMapped.toSeq, secondaryMapped.toSeq, unmapped.toSeq)
          //(ReferencePositionPairDS(curr_bucket), curr_bucket)
          val currARholder = TestARholder(reads.toSeq)

          val pos = currARholder.myreads(0).start

          //val finalpos =

          val currPosition = TestPositionHolder(Option(pos))

          //if currARholder.myreads.

          ((currPosition, "dude"), currARholder)
        }
    }.groupBy(_._1)

  }
}

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

