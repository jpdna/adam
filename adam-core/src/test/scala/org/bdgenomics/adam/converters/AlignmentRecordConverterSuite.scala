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
package org.bdgenomics.adam.converters

import java.io.File

import htsjdk.samtools.{ SamReaderFactory, SAMRecord }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  RecordGroup,
  SAMFileHeaderWritable,
  SequenceDictionary,
  SequenceRecord
}
import org.scalatest.FunSuite

import scala.collection.JavaConversions._

class AlignmentRecordConverterSuite extends FunSuite {

  // allocate converters
  val adamRecordConverter = new AlignmentRecordConverter

  def makeRead(start: Long, cigar: String, mdtag: String, length: Int, id: Int = 0, nullQuality: Boolean = false): AlignmentRecord = {
    val sequence: String = "A" * length
    val builder = AlignmentRecord.newBuilder()
      .setReadName("read" + id.toString)
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setMismatchingPositions(mdtag)
      .setOldPosition(12)
      .setOldCigar("2^AAA3")

    if (!nullQuality) {
      builder.setQual(sequence) // no typo, we just don't care
    }

    builder.build()
  }

  test("testing the fields in a converted ADAM Read") {
    val adamRead = makeRead(3L, "2M3D2M", "2^AAA2", 4)

    // add reference details
    adamRead.setRecordGroupName("record_group")
    adamRead.setRecordGroupSample("sample")
    adamRead.setContig(Contig.newBuilder()
      .setContigName("referencetest")
      .build())
    adamRead.setMateContig(Contig.newBuilder()
      .setContigName("matereferencetest")
      .setContigLength(6L)
      .setReferenceURL("test://chrom1")
      .build())
    adamRead.setMateAlignmentStart(6L)

    // make sequence dictionary
    val seqRecForDict = SequenceRecord("referencetest", 5, "test://chrom1")
    val dict = SequenceDictionary(seqRecForDict)

    //make read group dictionary
    val readGroup = new RecordGroup(adamRead.getRecordGroupSample(), adamRead.getRecordGroupName())
    val readGroups = new RecordGroupDictionary(Seq(readGroup))

    // convert read
    val toSAM = adamRecordConverter.convert(adamRead,
      SAMFileHeaderWritable(adamRecordConverter.createSAMHeader(dict,
        readGroups)))

    // validate conversion
    val sequence = "A" * 4
    assert(toSAM.getReadName === ("read" + 0.toString))
    assert(toSAM.getAlignmentStart === 4)
    assert(toSAM.getReadUnmappedFlag === false)
    assert(toSAM.getCigarString === "2M3D2M")
    assert(toSAM.getReadString === sequence)
    assert(toSAM.getReadNegativeStrandFlag === false)
    assert(toSAM.getMappingQuality === 60)
    assert(toSAM.getBaseQualityString === sequence)
    assert(toSAM.getAttribute("MD") === "2^AAA2")
    assert(toSAM.getIntegerAttribute("OP") === 13)
    assert(toSAM.getStringAttribute("OC") === "2^AAA3")
    //make sure that we didn't set the SM attribute.
    //issue #452 https://github.com/bigdatagenomics/adam/issues/452
    assert(toSAM.getAttribute("SM") === null)
    assert(toSAM.getHeader().getReadGroup("record_group").getSample() === "sample")
  }

  test("converting a read with null quality is OK") {
    val adamRead = makeRead(3L, "2M3D2M", "2^AAA2", 4, nullQuality = true)

    // add reference details
    adamRead.setRecordGroupName("record_group")
    adamRead.setRecordGroupSample("sample")
    adamRead.setContig(Contig.newBuilder()
      .setContigName("referencetest")
      .build())
    adamRead.setMateContig(Contig.newBuilder()
      .setContigName("matereferencetest")
      .setContigLength(6L)
      .setReferenceURL("test://chrom1")
      .build())
    adamRead.setMateAlignmentStart(6L)

    // make sequence dictionary
    val seqRecForDict = SequenceRecord("referencetest", 5, "test://chrom1")
    val dict = SequenceDictionary(seqRecForDict)

    //make read group dictionary
    val readGroup = new RecordGroup(adamRead.getRecordGroupSample(), adamRead.getRecordGroupName())
    val readGroups = new RecordGroupDictionary(Seq(readGroup))

    // convert read
    val toSAM = adamRecordConverter.convert(adamRead,
      SAMFileHeaderWritable(adamRecordConverter.createSAMHeader(dict,
        readGroups)))

    // validate conversion
    val sequence = "A" * 4
    assert(toSAM.getReadName === ("read" + 0.toString))
    assert(toSAM.getAlignmentStart === 4)
    assert(toSAM.getReadUnmappedFlag === false)
    assert(toSAM.getCigarString === "2M3D2M")
    assert(toSAM.getReadString === sequence)
    assert(toSAM.getReadNegativeStrandFlag === false)
    assert(toSAM.getMappingQuality === 60)
    assert(toSAM.getBaseQualityString === "*")
    assert(toSAM.getAttribute("MD") === "2^AAA2")
    assert(toSAM.getIntegerAttribute("OP") === 13)
    assert(toSAM.getStringAttribute("OC") === "2^AAA3")
    //make sure that we didn't set the SM attribute.
    //issue #452 https://github.com/bigdatagenomics/adam/issues/452
    assert(toSAM.getAttribute("SM") === null)
    assert(toSAM.getHeader().getReadGroup("record_group").getSample() === "sample")
  }

  test("convert a read to fastq") {
    val adamRead = AlignmentRecord.newBuilder()
      .setSequence("ACACCAACATG")
      .setQual(".+**.+;:**.")
      .setReadName("thebestread")
      .build()

    val fastq = adamRecordConverter.convertToFastq(adamRead)
      .toString
      .split('\n')

    assert(fastq(0) === "@thebestread")
    assert(fastq(1) === "ACACCAACATG")
    assert(fastq(2) === "+")
    assert(fastq(3) === ".+**.+;:**.")
  }

  def getSAMRecordFromReadName(readName: String): (AlignmentRecord, AlignmentRecord) = {
    val samToADAMConverter = new SAMRecordConverter
    val SAMTestFile = new File(getClass.getClassLoader.getResource("bqsr1.sam").getFile)
    val newSAMReader = SamReaderFactory.makeDefault().open(SAMTestFile)

    // Obtain SAMRecord
    val newSAMRecord = newSAMReader.iterator().dropWhile(r => r.getReadName != readName)
    val newSequenceRecord = new SequenceRecord("22", 51304566)
    val newSequenceDictionary = SequenceDictionary(newSequenceRecord)
    val firstRecord = samToADAMConverter.convert(newSAMRecord.next(), newSequenceDictionary, new RecordGroupDictionary(Seq()))
    val secondRecord = samToADAMConverter.convert(newSAMRecord.next(), newSequenceDictionary, new RecordGroupDictionary(Seq()))
    (firstRecord, secondRecord)
  }

  test("reverse complement reads when converting to fastq") {

    // SRR062634.10022079      83      22      16082719        0       5S95M   =       16082635        -179    
    // AAGTAGCTGGGACTACACGCACGCACCACCATGCCTGGCTAATTTTTGTATTTTTAGTAGAGATGAGGTTTCACCATATTGGCCAGGCTGGTTTTGAATT    
    // #####EB5BB<840&:2?>A?-AC8=,5@AABCB?CEDBDC@6BB,CA0CB,B-DEDEDEDEA:D?DE5EBEC?E?5?D:AEEEDEDDEEE=BEEBDD-?    
    // RG:Z:SRR062634  XC:i:95 XT:A:R  NM:i:2  SM:i:0  AM:i:0  X0:i:3  X1:i:0  XM:i:2  XO:i:0  XG:i:0  MD:Z:15G0T78    
    // XA:Z:GL000244.1,+31092,100M,2;14,+19760216,100M,2;

    val (firstRecord, secondRecord) = getSAMRecordFromReadName("SRR062634.10022079")

    val firstRecordFastq = adamRecordConverter.convertToFastq(firstRecord, maybeAddSuffix = true)
      .toString
      .split('\n')

    assert(firstRecordFastq(0) === "@SRR062634.10022079/2")
    assert(firstRecordFastq(1) === "CTGGAGTGCAGTGGCATGATTTCAGCTCACTGTCGTCTCTGCCTCCCTGACTCAAGTGATTCTCCTGCCTCAGCCTCCCACGTCGCTCGGACTCCACGCC")
    assert(firstRecordFastq(2) === "+")
    assert(firstRecordFastq(3) === "A:=D5D5E?D?DDD:.@@@@=?EE=DADDB@D=DD??ED=:CCCC?D:E=EEB=-C>C=@=EEEEB5EC-?A>=C-C?DC+34+4A>-?5:=/-A=@>>:")

    val secondRecordFastq = adamRecordConverter.convertToFastq(secondRecord, maybeAddSuffix = true)
      .toString
      .split('\n')

    assert(secondRecordFastq(0) === "@SRR062634.10022079/1")
    assert(secondRecordFastq(1) === "AATTCAAAACCAGCCTGGCCAATATGGTGAAACCTCATCTCTACTAAAAATACAAAAATTAGCCAGGCATGGTGGTGCGTGCGTGTAGTCCCAGCTACTT")
    assert(secondRecordFastq(2) === "+")
    assert(secondRecordFastq(3) === "?-DDBEEB=EEEDDEDEEEA:D?5?E?CEBE5ED?D:AEDEDEDED-B,BC0AC,BB6@CDBDEC?BCBAA@5,=8CA-?A>?2:&048<BB5BE#####")

  }

  test("converting to fastq with unmapped reads") {
    //SRR062634.10448889      117     22      16079761        0       *       =       16079761        0       
    // TTTCTTTCTTTTATATATATATACACACACACACACACACACACACATATATGTATATATACACGTATATGTATGTATATATGTATATATACACGTATAT    
    // @DF>C;FDC=EGEGGEFDGEFDD?DFDEEGFGFGGGDGGGGGGGEGGGGFGGGFGGGGGGFGGFGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG    
    // RG:Z:SRR062634

    val (secondRecord, firstRecord) = getSAMRecordFromReadName("SRR062634.10448889")

    val firstRecordFastq = adamRecordConverter.convertToFastq(firstRecord, maybeAddSuffix = true)
      .toString
      .split('\n')

    assert(firstRecord.getReadMapped)
    assert(firstRecord.getReadNegativeStrand)
    assert(firstRecordFastq(0) === "@SRR062634.10448889/2")
    assert(firstRecordFastq(1) === "ACCTGTCTCAGCCTCCCAAAGTGCTGCGATTACAGTCATGAGCCACCGCACTTGGCTGGGTTTTCGTTTTCTTTCTTTTATATATATATACACACACACA")
    assert(firstRecordFastq(2) === "+")
    assert(firstRecordFastq(3) === "GGGGGGGGGGGGGGGGGGGGGEGGGGGGGGGGGGGGGGGGGGGGGGGFGEGEEDGGFDF?AEEEBDADEEDEEE;DFC@'B:B=B=B=BADCBCBCA=DA")

    val secondRecordFastq = adamRecordConverter.convertToFastq(secondRecord, maybeAddSuffix = true)
      .toString
      .split('\n')

    assert(!secondRecord.getReadMapped)
    assert(!secondRecord.getReadNegativeStrand)
    assert(secondRecordFastq(0) === "@SRR062634.10448889/1")
    assert(secondRecordFastq(1) === secondRecord.getSequence)
    assert(secondRecordFastq(2) === "+")
    assert(secondRecordFastq(3) === secondRecord.getQual)
  }
}

