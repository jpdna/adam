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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.dataset.FlagStatAlignmentRecordFields
import org.bdgenomics.adam.util.Util._
import org.bdgenomics.formats.avro.AlignmentRecord

object FlagStatMetrics {
  val emptyFailedQuality = new FlagStatMetrics(0, DuplicateMetrics.empty, DuplicateMetrics.empty, 0, 0, 0, 0, 0, 0, 0, 0, 0, true)
  val emptyPassedQuality = new FlagStatMetrics(0, DuplicateMetrics.empty, DuplicateMetrics.empty, 0, 0, 0, 0, 0, 0, 0, 0, 0, false)
}

object DuplicateMetrics {
  val empty = new DuplicateMetrics(0, 0, 0, 0)

  def apply(record: AlignmentRecord): (DuplicateMetrics, DuplicateMetrics) = {
    import FlagStat.b2i

    def isPrimary(record: AlignmentRecord): Boolean = {
      record.getDuplicateRead && record.getPrimaryAlignment
    }
    def isSecondary(record: AlignmentRecord): Boolean = {
      record.getDuplicateRead && !record.getPrimaryAlignment
    }

    def duplicateMetrics(f: (AlignmentRecord) => Boolean) = {
      new DuplicateMetrics(
        b2i(f(record)),
        b2i(f(record) && record.getReadMapped && record.getMateMapped),
        b2i(f(record) && record.getReadMapped && !record.getMateMapped),
        b2i(f(record) && (!isSameContig(record.getContigName, record.getMateContigName)))
      )
    }
    (duplicateMetrics(isPrimary), duplicateMetrics(isSecondary))
  }
}

case class DuplicateMetrics(total: Long, bothMapped: Long, onlyReadMapped: Long, crossChromosome: Long) {
  def +(that: DuplicateMetrics): DuplicateMetrics = {
    new DuplicateMetrics(
      total + that.total,
      bothMapped + that.bothMapped,
      onlyReadMapped + that.onlyReadMapped,
      crossChromosome + that.crossChromosome
    )
  }
}

case class FlagStatMetrics(total: Long, duplicatesPrimary: DuplicateMetrics, duplicatesSecondary: DuplicateMetrics,
                           mapped: Long, pairedInSequencing: Long,
                           read1: Long, read2: Long, properlyPaired: Long, withSelfAndMateMapped: Long,
                           singleton: Long, withMateMappedToDiffChromosome: Long,
                           withMateMappedToDiffChromosomeMapQ5: Long, failedQuality: Boolean) {
  def +(that: FlagStatMetrics): FlagStatMetrics = {
    assert(failedQuality == that.failedQuality, "Can't reduce passedVendorQuality with different failedQuality values")
    new FlagStatMetrics(
      total + that.total,
      duplicatesPrimary + that.duplicatesPrimary,
      duplicatesSecondary + that.duplicatesSecondary,
      mapped + that.mapped,
      pairedInSequencing + that.pairedInSequencing,
      read1 + that.read1,
      read2 + that.read2,
      properlyPaired + that.properlyPaired,
      withSelfAndMateMapped + that.withSelfAndMateMapped,
      singleton + that.singleton,
      withMateMappedToDiffChromosome + that.withMateMappedToDiffChromosome,
      withMateMappedToDiffChromosomeMapQ5 + that.withMateMappedToDiffChromosomeMapQ5,
      failedQuality
    )
  }
}

object FlagStat {

  def b2i(boolean: Boolean) = if (boolean) 1 else 0

  def b(boolean: java.lang.Boolean) = Option(boolean).exists(x => x)

  def i(int: java.lang.Integer): Int = Option(int).map(Integer2int).getOrElse(-1)

  def apply(rdd: RDD[AlignmentRecord]) = {
    rdd.map {
      p =>
        val mateMappedToDiffChromosome =
          p.getReadPaired && p.getReadMapped && p.getMateMapped && !isSameContig(p.getContigName, p.getMateContigName)
        val (primaryDuplicates, secondaryDuplicates) = DuplicateMetrics(p)
        new FlagStatMetrics(
          1,
          primaryDuplicates, secondaryDuplicates,
          b2i(b(p.getReadMapped)),
          b2i(b(p.getReadPaired) && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(p.getReadPaired) && p.getReadInFragment == 0 && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(p.getReadPaired) && p.getReadInFragment == 1 && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(p.getReadPaired) && b(p.getProperPair) && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(p.getReadPaired) && b(p.getReadMapped) && b(p.getMateMapped) && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(p.getReadPaired) && b(p.getReadMapped) && b(!p.getMateMapped) && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(mateMappedToDiffChromosome) && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          b2i(b(mateMappedToDiffChromosome && i(p.getMapq) >= 5) && b(!p.getSupplementaryAlignment) && p.getPrimaryAlignment),
          p.getFailedVendorQualityChecks
        )
    }.aggregate((FlagStatMetrics.emptyFailedQuality, FlagStatMetrics.emptyPassedQuality))(
      seqOp = {
        (a, b) =>
          if (b.failedQuality) {
            (a._1 + b, a._2)
          } else {
            (a._1, a._2 + b)
          }
      },
      combOp = {
        (a, b) =>
          (a._1 + b._1, a._2 + b._2)
      }
    )
  }
}
/*
  def apply(dataset: Dataset[FlagStatAlignmentRecordFields]) = {
    dataset.map {
      p =>
        val mateMappedToDiffChromosome =
          p.readPaired && p.readMapped && p.mateMapped && !isSameContig(p.contigName, p.mateContigName)
        val (primaryDuplicates, secondaryDuplicates) = DuplicateMetrics(p)
        new FlagStatMetrics(
          1,
          primaryDuplicates, secondaryDuplicates,
          b2i(b(p.readMapped)),
          b2i(b(p.readPaired) && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(p.readPaired) && p.readInFragment == 0 && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(p.readPaired) && p.readInFragment == 1 && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(p.readPaired) && b(p.properPair) && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(p.readPaired) && b(p.readMapped) && b(p.mateMapped) && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(p.readPaired) && b(p.readMapped) && b(!p.mateMapped) && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(mateMappedToDiffChromosome) && b(!p.supplementaryAlignment) && p.primaryAlignment),
          b2i(b(mateMappedToDiffChromosome && i(p.mapq) >= 5) && b(!p.supplementaryAlignment) && p.primaryAlignment),
          p.failedVendorQualityChecks
        )
    }.rdd.aggregate((FlagStatMetrics.emptyFailedQuality, FlagStatMetrics.emptyPassedQuality))(
      seqOp = {
        (a, b) =>
          if (b.failedQuality) {
            (a._1 + b, a._2)
          } else {
            (a._1, a._2 + b)
          }
      },
      combOp = {
        (a, b) =>
          (a._1 + b._1, a._2 + b._2)
      }
    )
  }

}
*/ 