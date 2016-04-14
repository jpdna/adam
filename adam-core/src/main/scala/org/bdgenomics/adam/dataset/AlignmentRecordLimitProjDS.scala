package org.bdgenomics.adam.dataset

/**
 * Created by jp on 4/13/16.
 */

case class AlignmentRecordLimitProjDS(readInFragment: java.lang.Integer,
                                      contigName: String,
                                      start: java.lang.Long,
                                      oldPosition: java.lang.Long,
                                      end: java.lang.Long,
                                      mapq: java.lang.Integer,
                                      readName: String,
                                      sequence: String,
                                      qual: String,
                                      cigar: String,
                                      oldCigar: String,
                                      basesTrimmedFromStart: java.lang.Integer,
                                      basesTrimmedFromEnd: java.lang.Integer,
                                      readPaired: Boolean,
                                      properPair: Boolean,
                                      readMapped: Boolean,
                                      mateMapped: Boolean,
                                      failedVendorQualityChecks: Boolean,
                                      duplicateRead: Boolean,
                                      readNegativeStrand: Boolean,
                                      mateNegativeStrand: Boolean,
                                      primaryAlignment: Boolean,
                                      secondaryAlignment: Boolean,
                                      supplementaryAlignment: Boolean,
                                      mismatchingPositions: String,
                                      recordGroupName: String,
                                      recordGroupSample: String,
                                      mateAlignmentStart: java.lang.Long,
                                      mateAlignmentEnd: java.lang.Long,
                                      mateContigName: String,
                                      inferredInsertSize: java.lang.Long) extends Serializable