package org.bdgenomics.adam.dataset

import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Created by jp on 4/14/16.
 */

object AlignmentRecordLimitProjDStoAlignmentRecord {
  def alignmentRecordLimitProjDStoAlignmentRecord(c: AlignmentRecordLimitProjDS): AlignmentRecord = {
    new AlignmentRecord(c.readInFragment,
      c.contigName,
      c.start,
      c.oldPosition,
      c.end,
      c.mapq,
      c.readName,
      c.sequence,
      c.qual,
      c.cigar,
      c.oldCigar,
      c.basesTrimmedFromStart,
      c.basesTrimmedFromEnd,
      c.readPaired,
      c.properPair,
      c.readMapped,
      c.mateMapped,
      c.failedVendorQualityChecks,
      c.duplicateRead,
      c.readNegativeStrand,
      c.mateNegativeStrand,
      c.primaryAlignment,
      c.secondaryAlignment,
      c.supplementaryAlignment,
      c.mismatchingPositions,
      null,
      null,
      c.recordGroupName,
      c.recordGroupSample,
      c.mateAlignmentStart,
      c.mateAlignmentEnd,
      c.mateContigName,
      c.inferredInsertSize)

  }
}

