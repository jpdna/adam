// Needs ADAM repo with class htsjdk.samtools.BAMStreamWriter
// such as: 

// Launch adam-shell and :paste script below
// Current versions of ADAM require --jars parameter to adam-shell  

// $ADAM_HOME/adam-shell --jars ../adam/adam/adam-assembly/target/adam_2.10-0.19.1-SNAPSHOT.jar

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.{ SAMFileHeaderWritable, VariantContext => ADAMVariantContext, ReferencePosition }
import org.bdgenomics.adam.rdd.GenomicPositionPartitioner
import org.bdgenomics.adam.models.{ SAMFileHeaderWritable, VariantContext => ADAMVariantContext, ReferencePosition }
import org.seqdoop.hadoop_bam.{ SAMRecordWritable, VariantContextWritable }
import java.nio.file.Files
import java.util.concurrent.{ ExecutorService, Executors }
import htsjdk.samtools.{ BAMStreamWriter, SAMFileHeader, SAMRecord }
import java.io.{ File, OutputStream }
import java.lang.Runnable
import org.apache.spark.{ SparkContext, Partitioner }
import htsjdk.variant.vcf.VCFFileReader
import scala.io.Source
import org.bdgenomics.adam.converters._
import org.apache.spark.SparkFiles

//Broadcast reference genome fasta file to each node for use by Freebayes
// will be stored in a temp directory, once per node, accessbile by SparkFiles.get()
sc.addFile("/jpr1/work/freebayes/v1/data/hs37d5.fa")

// Temorarily hard-coded region paritioning for chr20, currently region size of 
// todo: generalize for all chr and variable region sizes

// regions size currently hardcoded 100000 bp for testing, realistic sizes likely 10MB f
// for whole genome

val binToPartition = Map(("20", 0) -> 0,
  ("20", 1) -> 1,
  ("20", 2) -> 2,
  ("20", 3) -> 3,
  ("20", 4) -> 4,
  ("20", 5) -> 5,
  ("20", 6) -> 6,
  ("20", 7) -> 7,
  ("20", 8) -> 8,
  ("20", 9) -> 9,
  ("20", 10) -> 10,
  ("20", 11) -> 11)

val partitionToBin = Map(0 -> ("20", 0),
  1 -> ("20", 1),
  2 -> ("20", 2),
  3 -> ("20", 3),
  4 -> ("20", 4),
  5 -> ("20", 5),
  6 -> ("20", 6),
  7 -> ("20", 7),
  8 -> ("20", 8),
  9 -> ("20", 9),
  10 -> ("20", 10),
  11 -> ("20", 11))

// intraChrBinWithRefPos wraps a ReferencePosition with an intra-chromosome bin label
// Note: Scala compiler had  trouble using Ordering defined on RerferencePosition, so comparison done manually  
case class BinnedRefPos(intraChrBin: Int, refPos: ReferencePosition) extends Ordered[BinnedRefPos] {
  import scala.math.Ordered.orderingToOrdered   // scala seemed to need this
  def compare(that: BinnedRefPos): Int = {
    if (this.intraChrBin < that.intraChrBin) { -1 }
    else if (this.intraChrBin > that.intraChrBin) { 1 }
    else if (this.refPos.referenceName < that.refPos.referenceName) { -1 }
    else if (this.refPos.referenceName > that.refPos.referenceName) { 1 }
    else if (this.refPos.pos < that.refPos.pos) { -1 }
    else if (this.refPos.pos > that.refPos.pos) { 1 }
    else { 0 }
  }
}

//Custom Partitioner which uses predefined (chr, withinChrBin) -> parition_num mapping 
class AlignmentBinPartitioner(binToPartition: Map[(String, Int), Int]) extends Partitioner {
  def numPartitions = 11
  def getPartition(key: Any): Int = key match {
    case x: BinnedRefPos => binToPartition((x.refPos.referenceName, x.intraChrBin))
  }
  override def equals(other: Any): Boolean = other.isInstanceOf[AlignmentBinPartitioner]
  override def hashCode: Int = 0
}

// ExternalWriter Pipes a BAM header defiend in param "header", followed by a stream of BAM records (SAMRecordsWritable) 
//from the second member of Tuple from param "records" Iterator

// This wrapper is needed only for spark-shell, can be removed in application code
object ExternalWriterWrapper {  
  class ExternalWriter(records: Iterator[(BinnedRefPos, SAMRecordWritable)],
                       header: SAMFileHeader,
                       stream: OutputStream) extends Runnable {
    def run() {
      val writer = new BAMStreamWriter(stream)
      writer.setHeader(header)
      //println("Here in External Writer")
      var count = 0
      records.foreach(r => {
        if (count % 10000 == 0) {
          stream.flush()
          println("Have written " + count + " reads.")
        }
        count += 1
        writer.writeHadoopAlignment(r._2)
      })
      writer.close()
      stream.flush()
    }
  }
}

def callVariants(x: (Int, Iterator[(BinnedRefPos, SAMRecordWritable)]),
                 header: SAMFileHeaderWritable): Iterator[String] = {

  // construct the "target_region" for variant calling for the current parition, based on the predefined bin region
  // This is important because this partition may contain reads which began in the previous region, but we don't want to attempt calls
  // on the portion of  those reads fall outside the target region as not all the reads overlapping those marginal regions are present
  // Note: another strategy would be to allow the caller to attempt all calls but to filter "non-target" calls later
  val currPartitionNum = x._1
  val region_step1 = partitionToBin(currPartitionNum)
  val currChr = region_step1._1
  val curr_pos_start = region_step1._2 * 100000
  val curr_pos_stop = (region_step1._2 * 100000) + 100000
  val target_region_string = currChr + ":" + curr_pos_start.toString + "-" + curr_pos_stop.toString

  val iter = x._2

  if (!iter.hasNext) {
    // can't write a bam file of length 0 
    Iterator()
  } else {
    val tempDir = Files.createTempDirectory("vcf_out")

    println("tempdir is: " + tempDir)

    val refFile = SparkFiles.get("hs37d5.fa")
    val cmd = "/jpr1/work/freebayes/v1/freebayes/bin/freebayes --stdin -f " + refFile + " --region " + target_region_string + " --vcf " + tempDir.toAbsolutePath.resolve("calls.vcf").toString

    println("Running cmd" + cmd)
    println("CurrPartition: " + currPartitionNum + " region_Step1: " + region_step1)

    val pb = new ProcessBuilder(cmd.split(" ").toList)
    pb.redirectError(ProcessBuilder.Redirect.INHERIT)
    val process = pb.start()
    val inp = process.getOutputStream()

    val pool: ExecutorService = Executors.newFixedThreadPool(2)
    pool.submit(new ExternalWriterWrapper.ExternalWriter(iter, header.header, inp))
    val result = process.waitFor()

    if (result != 0) {
      println("Process " + cmd + " exited with " + result)
      throw new Exception("Process " + cmd + " exited with " + result)
    }

    pool.shutdown()

    val vcfWholeFileString = Source.fromFile(tempDir.toAbsolutePath.resolve("calls.vcf").toString).mkString
    List(vcfWholeFileString).toIterator

    //// Currently this entire Parition is returing the whole result VCF from this partition
    //// as a single String for debugging purposes
    //// It is also possible to return an Iterator[ADAMVariantContext] as follows.
    //// However, reconstructing an appropriate header to populate Sample/Seq dict metadata for this re-merged data
    //// needs some futher effort

    // val vcfFile = new VCFFileReader(new File(tempDir.toAbsolutePath.resolve("calls.vcf").toString), false)
    // val iterator = vcfFile.iterator()
    // var records = List[VariantContextWritable]()

    // while (iterator.hasNext()) {
    // val record = iterator.next()

    // val vcw = new VariantContextWritable()
    //  vcw.set(record)
    // records = vcw :: records
    // }

    // try {
    //    iterator.close()
    // } catch {
    //   case ex: Exception =>
    //  println("VCFReader exited with error " + ex)
    //}

    // val vcc = new VariantContextConverter()
    // val convertedRecords = records.flatMap(x => vcc.convert(x.get()))
    // val convertedRecords = records.flatMap(x => vcc.convert(x.get()))
    // convertedRecords.toIterator

  }
}


// begin processing a BAM file for variant calling
val aRdd = sc.loadBam("file:////jpr1/work/freebayes/v1/data/test1_0_1000000.bam")
val (reads_orig, header) = aRdd.convertToSam()

val reads = reads_orig.repartition(8)
val headerWritable = SAMFileHeaderWritable(header)

// This flatMap performs two activities
// 1) adds a key of type BinWithRefPos used in the custom genomic range paritioner
// 2) replicates reads which begin in one parition range and extend across the pre-defined boundary
//    into the next partition.  Such reads thus will be present in both future partitions.
def computeBins(x: SAMRecordWritable): List[(BinnedRefPos, SAMRecordWritable)] = {
  val alignmentStart = x.get.getAlignmentStart
  val alignmentEnd = x.get.getAlignmentEnd
  val rbin_start = (alignmentStart / 100000) * 100000
  val rbin_end = rbin_start + 100000
  val rbin = rbin_start / 100000

  if (x.get.getReferenceName.toString != "*") {
    if (alignmentEnd > rbin_end) {
      List((BinnedRefPos(rbin, ReferencePosition(x.get.getReferenceName.toString, x.get.getAlignmentStart)), x),
        (BinnedRefPos(rbin + 1, ReferencePosition(x.get.getReferenceName.toString, x.get.getAlignmentStart)), x))
    } else {
      List((BinnedRefPos(rbin, ReferencePosition(x.get.getReferenceName.toString, x.get.getAlignmentStart)), x))
    }
  } else {
    List()
  }
}

val myParitioner = new AlignmentBinPartitioner(binToPartition)
val variantCalls = reads.flatMap(x => computeBins(x))
                        .repartitionAndSortWithinPartitions(myParitioner)
                        .mapPartitionsWithIndex((partNum, myiter) => callVariants((partNum, myiter), headerWritable))

variantCalls.saveAsTextFile("callingResults1")


