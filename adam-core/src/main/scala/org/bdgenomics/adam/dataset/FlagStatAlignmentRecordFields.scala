package org.bdgenomics.adam.dataset

/**
 * Created by jp on 4/11/16.
 */
case class FlagStatAlignmentRecordFields(readMapped: Boolean,
                                         mateMapped: Boolean,
                                         readPaired: Boolean,
                                         contigName: String,
                                         mateContigName: String,
                                         primaryAlignment: Boolean,
                                         duplicateRead: Boolean,
                                         readInFragment: Int,
                                         properPair: Boolean,
                                         mapq: Int,
                                         failedVendorQualityChecks: Boolean,
                                         supplementaryAlignment: Boolean) extends Serializable

