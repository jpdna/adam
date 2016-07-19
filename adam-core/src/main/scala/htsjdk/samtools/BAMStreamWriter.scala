package htsjdk.samtools

import java.io.OutputStream
import org.seqdoop.hadoop_bam.SAMRecordWritable

class BAMStreamWriter(stream: OutputStream) extends BAMFileWriter(stream, null) {

  def writeHadoopAlignment(record: SAMRecordWritable) {
    writeAlignment(record.get)
  }
}