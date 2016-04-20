package org.bdgenomics.adam.dataset

/**
 * Created by jp on 4/14/16.
 */

//This case class is necessary for Dataset API becuase the existing ReferencePosition class is not a case class
// with primitive members, as required currently by the Dataset API Encoder
case class ReferencePositionDS(referenceName: String,
                               pos: Long,
                               orientation: Boolean)

