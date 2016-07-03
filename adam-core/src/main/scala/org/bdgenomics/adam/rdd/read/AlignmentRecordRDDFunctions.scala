/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.read

import java.io.{ InputStream, OutputStream, StringWriter, Writer }
import java.lang.reflect.InvocationTargetException
import htsjdk.samtools._
import htsjdk.samtools.util.{ BinaryCodec, BlockCompressedOutputStream }
import org.apache.avro.Schema
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.algorithms.consensus.{ ConsensusGenerator, ConsensusGeneratorFromReads }
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.realignment.RealignIndels
import org.bdgenomics.adam.rdd.read.recalibration.BaseQualityRecalibration
import org.bdgenomics.adam.rdd.{ ADAMRDDFunctions, ADAMSaveAnyArgs, ADAMSequenceDictionaryRDDAggregator }
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.MapTools
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.Logging
import org.seqdoop.hadoop_bam.SAMRecordWritable
import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.math.{ abs, min }
import scala.reflect.ClassTag

private[rdd] class AlignmentRecordRDDFunctions(val rdd: RDD[AlignmentRecord])
    extends ADAMRDDFunctions[AlignmentRecord] {

  /**
   * Calculates the subset of the RDD whose AlignmentRecords overlap the corresponding
   * query ReferenceRegion.  Equality of the reference sequence (to which these are aligned)
   * is tested by string equality of the names.  AlignmentRecords whose 'getReadMapped' method
   * return 'false' are ignored.
   *
   * The end of the record against the reference sequence is calculated from the cigar string
   * using the ADAMContext.referenceLengthFromCigar method.
   *
   * @param query The query region, only records which overlap this region are returned.
   * @return The subset of AlignmentRecords (corresponding to either primary or secondary alignments) that
   *         overlap the query region.
   */
  def filterByOverlappingRegion(query: ReferenceRegion): RDD[AlignmentRecord] = {
    def overlapsQuery(rec: AlignmentRecord): Boolean =
      rec.getReadMapped &&
        rec.getContigName == query.referenceName &&
        rec.getStart < query.end &&
        rec.getEnd > query.start
    rdd.filter(overlapsQuery)
  }

  private[rdd] def maybeSaveBam(
    args: ADAMSaveAnyArgs,
    sd: SequenceDictionary,
    rgd: RecordGroupDictionary,
    isSorted: Boolean = false): Boolean = {

    if (args.outputPath.endsWith(".sam")) {
      log.info("Saving data in SAM format")
      saveAsSam(
        args.outputPath,
        sd,
        rgd,
        asSingleFile = args.asSingleFile,
        isSorted = isSorted
      )
      true
    } else if (args.outputPath.endsWith(".bam")) {
      log.info("Saving data in BAM format")
      saveAsSam(
        args.outputPath,
        sd,
        rgd,
        asSam = false,
        asSingleFile = args.asSingleFile,
        isSorted = isSorted
      )
      true
    } else
      false
  }

  private[rdd] def maybeSaveFastq(args: ADAMSaveAnyArgs): Boolean = {
    if (args.outputPath.endsWith(".fq") || args.outputPath.endsWith(".fastq") ||
      args.outputPath.endsWith(".ifq")) {
      saveAsFastq(args.outputPath, sort = args.sortFastqOutput)
      true
    } else
      false
  }

  /**
   * Saves AlignmentRecords as a directory of Parquet files.
   *
   * The RDD is written as a directory of Parquet files, with
   * Parquet configuration described by the input param args.
   * The provided sequence dictionary is written at args.outputPath/_seqdict.avro
   * while the provided record group dictionary is written at
   * args.outputPath/_rgdict.avro. These two files are written as Avro binary.
   *
   * @param args Save configuration arguments.
   * @param sd Sequence dictionary describing the contigs these reads are
   *   aligned to.
   * @param rgd Record group dictionary describing the record groups these
   *   reads are from.
   */
  def saveAsParquet(args: ADAMSaveAnyArgs,
                    sd: SequenceDictionary,
                    rgd: RecordGroupDictionary): Unit = {

    // save rdd itself as parquet
    saveRddAsParquet(args)

    // then convert sequence dictionary and record group dictionaries to avro form
    val contigs = sd.toAvro
    val rgMetadata = rgd.recordGroups
      .map(_.toMetadata)

    // and write the sequence dictionary and record group dictionary to disk
    saveAvro("%s/_seqdict.avro".format(args.outputPath),
      rdd.context,
      Contig.SCHEMA$, contigs)
    saveAvro("%s/_rgdict.avro".format(args.outputPath),
      rdd.context,
      RecordGroupMetadata.SCHEMA$,
      rgMetadata)
  }

  /**
   * Saves AlignmentRecords as a directory of Parquet files or as SAM/BAM.
   *
   * This method infers the output format from the file extension. Filenames
   * ending in .sam/.bam are saved as SAM/BAM, and all other files are saved
   * as Parquet.
   *
   * @param args Save configuration arguments.
   * @param sd Sequence dictionary describing the contigs these reads are
   *   aligned to.
   * @param rgd Record group dictionary describing the record groups these
   *   reads are from.
   * @param isSorted If the output is sorted, this will modify the SAM/BAM header.
   */
  def save(
    args: ADAMSaveAnyArgs,
    sd: SequenceDictionary,
    rgd: RecordGroupDictionary,
    isSorted: Boolean = false): Boolean = {

    (maybeSaveBam(args, sd, rgd, isSorted) ||
      maybeSaveFastq(args) ||
      { saveAsParquet(args, sd, rgd); true })
  }

  /**
   * Converts an RDD into the SAM spec string it represents.
   *
   * This method converts an RDD of AlignmentRecords back to an RDD of
   * SAMRecordWritables and a SAMFileHeader, and then maps this RDD into a
   * string on the driver that represents this file in SAM.
   *
   * @param sd Sequence dictionary describing the contigs these reads are
   *   aligned to.
   * @param rgd Record group dictionary describing the record groups these
   *   reads are from.
   * @return A string on the driver representing this RDD of reads in SAM format.
   * @see adamConvertToSAM
   */
  def saveAsSamString(sd: SequenceDictionary,
                      rgd: RecordGroupDictionary): String = {
    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) = convertToSam(sd, rgd)

    val records = convertRecords.coalesce(1, shuffle = true).collect()

    val samHeaderCodec = new SAMTextHeaderCodec
    samHeaderCodec.setValidationStringency(ValidationStringency.SILENT)

    val samStringWriter = new StringWriter()
    samHeaderCodec.encode(samStringWriter, header);

    val samWriter: SAMTextWriter = new SAMTextWriter(samStringWriter)
    //samWriter.writeHeader(stringHeaderWriter.toString)

    records.foreach(record => samWriter.writeAlignment(record.get))

    samStringWriter.toString
  }

  /**
   * Saves an RDD of ADAM read data into the SAM/BAM format.
   *
   * @param filePath Path to save files to.
   * @param sd A dictionary describing the contigs this file is aligned against.
   * @param rgd A dictionary describing the read groups in this file.
   * @param asSam Selects whether to save as SAM or BAM. The default value is true (save in SAM format).
   * @param asSingleFile If true, saves output as a single file.
   * @param isSorted If the output is sorted, this will modify the header.
   */
  def saveAsSam(
    filePath: String,
    sd: SequenceDictionary,
    rgd: RecordGroupDictionary,
    asSam: Boolean = true,
    asSingleFile: Boolean = false,
    isSorted: Boolean = false): Unit = SAMSave.time {

    // if the file is sorted, make sure the sequence dictionary is sorted
    val sdFinal = if (isSorted) {
      sd.sorted
    } else {
      sd
    }

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) =
      convertToSam(
        sdFinal,
        rgd,
        isSorted
      )

    // add keys to our records
    val withKey = convertRecords.keyBy(v => new LongWritable(v.get.getAlignmentStart))

    // write file to disk
    val conf = rdd.context.hadoopConfiguration

    if (!asSingleFile) {
      val bcastHeader = rdd.context.broadcast(header)
      val mp = rdd.mapPartitionsWithIndex((idx, iter) => {
        log.info(s"Setting ${if (asSam) "SAM" else "BAM"} header for partition $idx")
        val header = bcastHeader.value
        synchronized {
          // perform map partition call to ensure that the SAM/BAM header is set on all
          // nodes in the cluster; see:
          // https://github.com/bigdatagenomics/adam/issues/353,
          // https://github.com/bigdatagenomics/adam/issues/676

          asSam match {
            case true =>
              ADAMSAMOutputFormat.clearHeader()
              ADAMSAMOutputFormat.addHeader(header)
              log.info(s"Set SAM header for partition $idx")
            case false =>
              ADAMBAMOutputFormat.clearHeader()
              ADAMBAMOutputFormat.addHeader(header)
              log.info(s"Set BAM header for partition $idx")
          }
        }
        Iterator[Int]()
      }).count()

      // force value check, ensure that computation happens
      if (mp != 0) {
        log.error("Had more than 0 elements after map partitions call to set VCF header across cluster.")
      }

      // attach header to output format
      asSam match {
        case true =>
          ADAMSAMOutputFormat.clearHeader()
          ADAMSAMOutputFormat.addHeader(header)
          log.info("Set SAM header on driver")
        case false =>
          ADAMBAMOutputFormat.clearHeader()
          ADAMBAMOutputFormat.addHeader(header)
          log.info("Set BAM header on driver")
      }

      asSam match {
        case true =>
          withKey.saveAsNewAPIHadoopFile(
            filePath,
            classOf[LongWritable],
            classOf[SAMRecordWritable],
            classOf[InstrumentedADAMSAMOutputFormat[LongWritable]],
            conf
          )
        case false =>
          withKey.saveAsNewAPIHadoopFile(
            filePath,
            classOf[LongWritable],
            classOf[SAMRecordWritable],
            classOf[InstrumentedADAMBAMOutputFormat[LongWritable]],
            conf
          )

      }
    } else {
      log.info(s"Writing single ${if (asSam) "SAM" else "BAM"} file (not Hadoop-style directory)")

      val fs = FileSystem.get(conf)

      val headPath = new Path(filePath + "_head")
      val tailPath = new Path(filePath + "_tail")
      val outputPath = new Path(filePath)

      // get an output stream
      val os = fs.create(headPath)
        .asInstanceOf[OutputStream]

      // TIL: sam and bam are written in completely different ways!
      if (asSam) {
        val sw: Writer = new StringWriter()
        val stw = new SAMTextWriter(os)
        stw.setHeader(header)
        stw.close()
      } else {
        // create htsjdk specific streams for writing the bam header
        val compressedOut: OutputStream = new BlockCompressedOutputStream(os, null)
        val binaryCodec = new BinaryCodec(compressedOut);

        // write a bam header - cribbed from Hadoop-BAM
        binaryCodec.writeBytes("BAM\001".getBytes())
        val sw: Writer = new StringWriter()
        new SAMTextHeaderCodec().encode(sw, header)
        binaryCodec.writeString(sw.toString, true, false)

        // write sequence dictionary
        val ssd = header.getSequenceDictionary
        binaryCodec.writeInt(ssd.size())
        ssd.getSequences
          .toList
          .foreach(r => {
            binaryCodec.writeString(r.getSequenceName(), true, true)
            binaryCodec.writeInt(r.getSequenceLength())
          })

        // flush and close all the streams
        compressedOut.flush()
        compressedOut.close()
      }

      // more flushing and closing
      os.flush()
      os.close()

      // set path to header file
      conf.set("org.bdgenomics.adam.rdd.read.bam_header_path", headPath.toString)

      // set up output format
      val headerLessOutputFormat = if (asSam) {
        classOf[InstrumentedADAMSAMOutputFormatHeaderLess[LongWritable]]
      } else {
        classOf[InstrumentedADAMBAMOutputFormatHeaderLess[LongWritable]]
      }

      // save rdd
      withKey.saveAsNewAPIHadoopFile(
        tailPath.toString,
        classOf[LongWritable],
        classOf[SAMRecordWritable],
        headerLessOutputFormat,
        conf
      )

      // get a list of all of the files in the tail file
      val tailFiles = fs.globStatus(new Path("%s/part-*".format(tailPath)))
        .toSeq
        .map(_.getPath)
        .sortBy(_.getName)
        .toArray

      // try to merge this via the fs api, which should guarantee ordering...?
      // however! this function is not implemented on all platforms, hence the try.
      try {

        // concat files together
        // we need to do this through reflection because the concat method is
        // NOT in hadoop 1.x

        // find the concat method
        val fsMethods = classOf[FileSystem].getDeclaredMethods
        val filteredMethods = fsMethods.filter(_.getName == "concat")

        // did we find the method? if not, throw an exception
        if (filteredMethods.size == 0) {
          throw new IllegalStateException(
            "Could not find concat method in FileSystem. Methods included:\n" +
              fsMethods.map(_.getName).mkString("\n") +
              "\nAre you running Hadoop 1.x?")
        } else if (filteredMethods.size > 1) {
          throw new IllegalStateException(
            "Found multiple concat methods in FileSystem:\n%s".format(
              filteredMethods.map(_.getName).mkString("\n")))
        }

        // since we've done our checking, let's get the method now
        val concatMethod = filteredMethods.head

        // we need to move the head file into the tailFiles directory
        // this is a requirement of the concat method
        val newHeadPath = new Path("%s/header".format(tailPath))
        fs.rename(headPath, newHeadPath)

        // invoke the method on the fs instance
        // if we are on hadoop 2.x, this makes the call:
        //
        // fs.concat(headPath, tailFiles)
        try {
          concatMethod.invoke(fs, newHeadPath, tailFiles)
        } catch {
          case ite: InvocationTargetException => {
            // move the head file back - essentially, unroll the prep for concat
            fs.rename(newHeadPath, headPath)

            // the only reason we have this try/catch is to unwrap the wrapped
            // exception and rethrow. this gives clearer logging messages
            throw ite.getTargetException
          }
        }

        // move concated file
        fs.rename(newHeadPath, outputPath)

        // delete tail files
        fs.delete(tailPath, true)
      } catch {
        case e: Throwable => {

          log.warn("Caught exception when merging via Hadoop FileSystem API:\n%s".format(e))
          log.warn("Retrying as manual copy from the driver which will degrade performance.")

          // doing this correctly is surprisingly hard
          // specifically, copy merge does not care about ordering, which is
          // fine if your files are unordered, but if the blocks in the file
          // _are_ ordered, then hahahahahahahahahaha. GOOD. TIMES.
          //
          // fortunately, the blocks in our file are ordered
          // the performance of this section is hilarious
          // 
          // specifically, the performance is hilariously bad
          //
          // but! it is correct.

          // open our output file
          val os = fs.create(outputPath)

          // prepend our header to the list of files to copy
          val filesToCopy = Seq(headPath) ++ tailFiles.toSeq

          // here is a byte array for copying
          val ba = new Array[Byte](1024)

          @tailrec def copy(is: InputStream,
                            os: OutputStream) {

            // make a read
            val bytesRead = is.read(ba)

            // did our read succeed? if so, write to output stream
            // and continue
            if (bytesRead >= 0) {
              os.write(ba, 0, bytesRead)

              copy(is, os)
            }
          }

          // loop over allllll the files and copy them
          val numFiles = filesToCopy.length
          var filesCopied = 1
          filesToCopy.foreach(p => {

            // print a bit of progress logging
            log.info("Copying file %s, file %d of %d.".format(
              p.toString,
              filesCopied,
              numFiles))

            // open our input file
            val is = fs.open(p)

            // until we are out of bytes, copy
            copy(is, os)

            // close our input stream
            is.close()

            // increment file copy count
            filesCopied += 1
          })

          // flush and close the output stream
          os.flush()
          os.close()

          // delete temp files
          fs.delete(headPath, true)
          fs.delete(tailPath, true)
        }
      }
    }
  }

  /**
   * Converts an RDD of ADAM read records into SAM records.
   *
   * @return Returns a SAM/BAM formatted RDD of reads, as well as the file header.
   */
  def convertToSam(sd: SequenceDictionary,
                   rgd: RecordGroupDictionary,
                   isSorted: Boolean = false): (RDD[SAMRecordWritable], SAMFileHeader) = ConvertToSAM.time {

    // create conversion object
    val adamRecordConverter = new AlignmentRecordConverter

    // create header
    val header = adamRecordConverter.createSAMHeader(sd, rgd)

    if (isSorted) {
      header.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    }

    // broadcast for efficiency
    val hdrBcast = rdd.context.broadcast(SAMFileHeaderWritable(header))

    // map across RDD to perform conversion
    val convertedRDD: RDD[SAMRecordWritable] = rdd.map(r => {
      // must wrap record for serializability
      val srw = new SAMRecordWritable()
      srw.set(adamRecordConverter.convert(r, hdrBcast.value, rgd))
      srw
    })

    (convertedRDD, header)
  }

  /**
   * Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(kmerLength: Int): RDD[(String, Long)] = {
    rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  def sortReadsByReferencePosition(): RDD[AlignmentRecord] = SortReads.time {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. We prefix with tildes ("~";
    // ASCII 126) to ensure that the read name is lexicographically "after" the
    // contig names.
    rdd.sortBy(r => {
      if (r.getReadMapped) {
        ReferencePosition(r)
      } else {
        ReferencePosition(s"~~~${r.getReadName}", 0)
      }
    })
  }

  def sortReadsByReferencePositionAndIndex(sd: SequenceDictionary): RDD[AlignmentRecord] = SortByIndex.time {
    log.info("Sorting reads by reference index, using %s.".format(sd))

    import scala.math.Ordering.{ Int => ImplicitIntOrdering, _ }

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. To do this, we hash the sequence name
    // and add the max contig index
    val maxContigIndex = sd.records.flatMap(_.referenceIndex).max
    rdd.sortBy(r => {
      if (r.getReadMapped) {
        val sr = sd(r.getContigName)
        require(sr.isDefined, "Read %s has contig name %s not in dictionary %s.".format(
          r, r.getContigName, sd))
        require(sr.get.referenceIndex.isDefined,
          "Contig %s from sequence dictionary lacks an index.".format(sr))

        (sr.get.referenceIndex.get, r.getStart: Long)
      } else {
        (min(abs(r.getReadName.hashCode + maxContigIndex), Int.MaxValue), 0L)
      }
    })
  }

  /**
   * Marks reads as possible fragment duplicates.
   *
   * @param rgd A dictionary mapping read groups to sequencing libraries. This
   *   is used when deduping reads, as we only dedupe reads that are from the
   *   same original library.
   * @return A new RDD where reads have the duplicate read flag set. Duplicate
   *   reads are NOT filtered out.
   */
  def markDuplicates(rgd: RecordGroupDictionary): RDD[AlignmentRecord] = MarkDuplicatesInDriver.time {
    MarkDuplicates(rdd, rgd)
  }

  /**
   * Runs base quality score recalibration on a set of reads. Uses a table of
   * known SNPs to mask true variation during the recalibration process.
   *
   * @param knownSnps A table of known SNPs to mask valid variants.
   * @param observationDumpFile An optional local path to dump recalibration
   *                            observations to.
   * @return Returns an RDD of recalibrated reads.
   */
  def recalibateBaseQualities(
    knownSnps: Broadcast[SnpTable],
    observationDumpFile: Option[String] = None,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT): RDD[AlignmentRecord] = BQSRInDriver.time {
    BaseQualityRecalibration(rdd, knownSnps, observationDumpFile, validationStringency)
  }

  /**
   * Realigns indels using a concensus-based heuristic.
   *
   * @see RealignIndels
   * @param isSorted If the input data is sorted, setting this parameter to true avoids a second sort.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   * @param maxConsensusNumber The maximum number of consensus sequences to realign against per
   * target region.
   * @param lodThreshold Log-odds threhold to use when realigning; realignments are only finalized
   * if the log-odds threshold is exceeded.
   * @param maxTargetSize The maximum width of a single target region for realignment.
   * @return Returns an RDD of mapped reads which have been realigned.
   */
  def realignIndels(
    consensusModel: ConsensusGenerator = new ConsensusGeneratorFromReads,
    isSorted: Boolean = false,
    maxIndelSize: Int = 500,
    maxConsensusNumber: Int = 30,
    lodThreshold: Double = 5.0,
    maxTargetSize: Int = 3000): RDD[AlignmentRecord] = RealignIndelsInDriver.time {
    RealignIndels(rdd, consensusModel, isSorted, maxIndelSize, maxConsensusNumber, lodThreshold)
  }

  // Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
  def flagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by record group and read name
   *
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  def groupReadsByFragment(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }

  /**
   * Converts a set of records into an RDD containing the pairs of all unique tagStrings
   * within the records, along with the count (number of records) which have that particular
   * attribute.
   *
   * @return An RDD of attribute name / count pairs.
   */
  def characterizeTags(): RDD[(String, Long)] = {
    rdd.flatMap(RichAlignmentRecord(_).tags.map(attr => (attr.tag, 1L))).reduceByKey(_ + _)
  }

  /**
   * Calculates the set of unique attribute <i>values</i> that occur for the given
   * tag, and the number of time each value occurs.
   *
   * @param tag The name of the optional field whose values are to be counted.
   * @return A Map whose keys are the values of the tag, and whose values are the number of time each tag-value occurs.
   */
  def characterizeTagValues(tag: String): Map[Any, Long] = {
    filterRecordsWithTag(tag).flatMap(RichAlignmentRecord(_).tags.find(_.tag == tag)).map(
      attr => Map(attr.value -> 1L)
    ).reduce {
        (map1: Map[Any, Long], map2: Map[Any, Long]) =>
          MapTools.add(map1, map2)
      }
  }

  /**
   * Returns the subset of the ADAMRecords which have an attribute with the given name.
   *
   * @param tagName The name of the attribute to filter on (should be length 2)
   * @return An RDD[Read] containing the subset of records with a tag that matches the given name.
   */
  def filterRecordsWithTag(tagName: String): RDD[AlignmentRecord] = {
    assert(
      tagName.length == 2,
      "withAttribute takes a tagName argument of length 2; tagName=\"%s\"".format(tagName)
    )
    rdd.filter(RichAlignmentRecord(_).tags.exists(_.tag == tagName))
  }

  /**
   * Saves these AlignmentRecords to two FASTQ files: one for the first mate in each pair, and the other for the second.
   *
   * @param fileName1 Path at which to save a FASTQ file containing the first mate of each pair.
   * @param fileName2 Path at which to save a FASTQ file containing the second mate of each pair.
   * @param validationStringency Iff strict, throw an exception if any read in this RDD is not accompanied by its mate.
   */
  def saveAsPairedFastq(
    fileName1: String,
    fileName2: String,
    outputOriginalBaseQualities: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT,
    persistLevel: Option[StorageLevel] = None): Unit = {
    def maybePersist[T](r: RDD[T]): Unit = {
      persistLevel.foreach(r.persist(_))
    }
    def maybeUnpersist[T](r: RDD[T]): Unit = {
      persistLevel.foreach(_ => r.unpersist())
    }

    maybePersist(rdd)
    val numRecords = rdd.count()

    val readsByID: RDD[(String, Iterable[AlignmentRecord])] =
      rdd.groupBy(record => {
        if (!AlignmentRecordConverter.readNameHasPairedSuffix(record))
          record.getReadName
        else
          record.getReadName.dropRight(2)
      })

    validationStringency match {
      case ValidationStringency.STRICT | ValidationStringency.LENIENT =>
        val readIDsWithCounts: RDD[(String, Int)] = readsByID.mapValues(_.size)
        val unpairedReadIDsWithCounts: RDD[(String, Int)] = readIDsWithCounts.filter(_._2 != 2)
        maybePersist(unpairedReadIDsWithCounts)

        val numUnpairedReadIDsWithCounts: Long = unpairedReadIDsWithCounts.count()
        if (numUnpairedReadIDsWithCounts != 0) {
          val readNameOccurrencesMap: collection.Map[Int, Long] = unpairedReadIDsWithCounts.map(_._2).countByValue()

          val msg =
            List(
              s"Found $numUnpairedReadIDsWithCounts read names that don't occur exactly twice:",

              readNameOccurrencesMap.take(100).map({
                case (numOccurrences, numReadNames) => s"${numOccurrences}x:\t$numReadNames"
              }).mkString("\t", "\n\t", if (readNameOccurrencesMap.size > 100) "\n\t…" else ""),
              "",

              "Samples:",
              unpairedReadIDsWithCounts
                .take(100)
                .map(_._1)
                .mkString("\t", "\n\t", if (numUnpairedReadIDsWithCounts > 100) "\n\t…" else "")
            ).mkString("\n")

          if (validationStringency == ValidationStringency.STRICT)
            throw new IllegalArgumentException(msg)
          else if (validationStringency == ValidationStringency.LENIENT)
            logError(msg)
        }
      case ValidationStringency.SILENT =>
    }

    val pairedRecords: RDD[AlignmentRecord] = readsByID.filter(_._2.size == 2).map(_._2).flatMap(x => x)
    maybePersist(pairedRecords)
    val numPairedRecords = pairedRecords.count()

    maybeUnpersist(rdd.unpersist())

    val firstInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getReadInFragment == 0)
    maybePersist(firstInPairRecords)
    val numFirstInPairRecords = firstInPairRecords.count()

    val secondInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getReadInFragment == 1)
    maybePersist(secondInPairRecords)
    val numSecondInPairRecords = secondInPairRecords.count()

    maybeUnpersist(pairedRecords)

    log.info(
      "%d/%d records are properly paired: %d firsts, %d seconds".format(
        numPairedRecords,
        numRecords,
        numFirstInPairRecords,
        numSecondInPairRecords
      )
    )

    assert(
      numFirstInPairRecords == numSecondInPairRecords,
      "Different numbers of first- and second-reads: %d vs. %d".format(numFirstInPairRecords, numSecondInPairRecords)
    )

    val arc = new AlignmentRecordConverter

    firstInPairRecords
      .sortBy(_.getReadName)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true, outputOriginalBaseQualities = outputOriginalBaseQualities))
      .saveAsTextFile(fileName1)

    secondInPairRecords
      .sortBy(_.getReadName)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true, outputOriginalBaseQualities = outputOriginalBaseQualities))
      .saveAsTextFile(fileName2)

    maybeUnpersist(firstInPairRecords)
    maybeUnpersist(secondInPairRecords)
  }

  /**
   * Saves reads in FASTQ format.
   *
   * @param fileName Path to save files at.
   * @param outputOriginalBaseQualities Output the original base qualities (OQ) if available as opposed to those from BQSR
   * @param sort Whether to sort the FASTQ files by read name or not. Defaults
   *             to false. Sorting the output will recover pair order, if desired.
   */
  def saveAsFastq(
    fileName: String,
    fileName2Opt: Option[String] = None,
    outputOriginalBaseQualities: Boolean = false,
    sort: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT,
    persistLevel: Option[StorageLevel] = None) {
    log.info("Saving data in FASTQ format.")
    fileName2Opt match {
      case Some(fileName2) =>
        saveAsPairedFastq(
          fileName,
          fileName2,
          outputOriginalBaseQualities = outputOriginalBaseQualities,
          validationStringency = validationStringency,
          persistLevel = persistLevel
        )
      case _ =>
        val arc = new AlignmentRecordConverter

        // sort the rdd if desired
        val outputRdd = if (sort || fileName2Opt.isDefined) {
          rdd.sortBy(_.getReadName)
        } else {
          rdd
        }

        // convert the rdd and save as a text file
        outputRdd
          .map(record => arc.convertToFastq(record, outputOriginalBaseQualities = outputOriginalBaseQualities))
          .saveAsTextFile(fileName)
    }
  }

  /**
   * Reassembles read pairs from two sets of unpaired reads. The assumption is that the two sets
   * were _originally_ paired together.
   *
   * @note The RDD that this is called on should be the RDD with the first read from the pair.
   * @param secondPairRdd The rdd containing the second read from the pairs.
   * @param validationStringency How stringently to validate the reads.
   * @return Returns an RDD with the pair information recomputed.
   */
  def reassembleReadPairs(
    secondPairRdd: RDD[AlignmentRecord],
    validationStringency: ValidationStringency = ValidationStringency.LENIENT): RDD[AlignmentRecord] = {
    // cache rdds
    val firstPairRdd = rdd.cache()
    secondPairRdd.cache()

    val firstRDDKeyedByReadName = firstPairRdd.keyBy(_.getReadName.dropRight(2))
    val secondRDDKeyedByReadName = secondPairRdd.keyBy(_.getReadName.dropRight(2))

    // all paired end reads should have the same name, except for the last two
    // characters, which will be _1/_2
    val joinedRDD: RDD[(String, (AlignmentRecord, AlignmentRecord))] =
      if (validationStringency == ValidationStringency.STRICT) {
        firstRDDKeyedByReadName.cogroup(secondRDDKeyedByReadName).map {
          case (readName, (firstReads, secondReads)) =>
            (firstReads.toList, secondReads.toList) match {
              case (firstRead :: Nil, secondRead :: Nil) =>
                (readName, (firstRead, secondRead))
              case _ =>
                throw new Exception(
                  "Expected %d first reads and %d second reads for name %s; expected exactly one of each:\n%s\n%s".format(
                    firstReads.size,
                    secondReads.size,
                    readName,
                    firstReads.map(_.getReadName).mkString("\t", "\n\t", ""),
                    secondReads.map(_.getReadName).mkString("\t", "\n\t", "")
                  )
                )
            }
        }

      } else {
        firstRDDKeyedByReadName.join(secondRDDKeyedByReadName)
      }

    val finalRdd = joinedRDD
      .flatMap(kv => Seq(
        AlignmentRecord.newBuilder(kv._2._1)
          .setReadPaired(true)
          .setProperPair(true)
          .setReadInFragment(0)
          .build(),
        AlignmentRecord.newBuilder(kv._2._2)
          .setReadPaired(true)
          .setProperPair(true)
          .setReadInFragment(1)
          .build()
      ))

    // uncache temp rdds
    firstPairRdd.unpersist()
    secondPairRdd.unpersist()

    // return
    finalRdd
  }

  def toFragments: RDD[Fragment] = {
    groupReadsByFragment.map(_.toFragment)
  }
}
