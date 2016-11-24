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
package org.bdgenomics.adam.hbase

import java.util
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.spark._
import org.bdgenomics.adam.rdd.variation.{ GenotypeRDD, VariantContextRDD }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro._
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.io._
import java.io.ByteArrayOutputStream
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.avro.specific.SpecificDatumReader
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.fs.{ FSDataInputStream, FileSystem, Path }
import org.bdgenomics.adam.models.{ ReferenceRegion, ReferencePosition, SequenceDictionary, VariantContext }
import scala.io.Source

import scala.collection.mutable.ListBuffer
import sys.process._

import scala.collection.JavaConverters._

object HBaseFunctions {

  abstract class KeyStrategy[T, U] {
    def getKey(rowKeyInfo: T): Array[Byte]
    def getKeyRangePrefix(rangePrefixInfo: U): (Array[Byte], Array[Byte])
  }

  case class KeyStrategy1rowKeyInfo(contigName: String,
                                    start: Int,
                                    end: Int,
                                    refAllele: String,
                                    altAllele: String)

  case class KeyStrategy1RangePrefixInfo(queryRegion: ReferenceRegion)

  object KeyStrategy1 extends KeyStrategy[KeyStrategy1rowKeyInfo, KeyStrategy1RangePrefixInfo] {
    def getKey(rowKeyInfo: KeyStrategy1rowKeyInfo): Array[Byte] = {
      Bytes.toBytes(rowKeyInfo.contigName + "_" +
        String.format("%10s", rowKeyInfo.start.toString).replace(' ', '0') + "_" +
        rowKeyInfo.refAllele + "_" +
        rowKeyInfo.altAllele + "_" +
        (rowKeyInfo.end - rowKeyInfo.start).toString)
    }

    def getKeyRangePrefix(rangePrefixInfo: KeyStrategy1RangePrefixInfo): (Array[Byte], Array[Byte]) = {
      val start = Bytes.toBytes(rangePrefixInfo.queryRegion.referenceName + "_" +
        String.format("%10s", rangePrefixInfo.queryRegion.start.toString).replace(' ', '0'))
      val stop = Bytes.toBytes(rangePrefixInfo.queryRegion.referenceName + "_" +
        String.format("%10s", rangePrefixInfo.queryRegion.end.toString).replace(' ', '0'))
      (start, stop)
    }
  }

  def saveSequenceDictionaryToHBase(hbaseTableName: String,
                                    sequences: SequenceDictionary,
                                    sequenceDictionaryId: String): Unit = {

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table: Table = connection.getTable(TableName.valueOf(hbaseTableName))
    val contigs = sequences.toAvro

    val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)

    contigs.foreach((x) => {
      val contigDatumWriter: DatumWriter[Contig] =
        new SpecificDatumWriter[Contig](scala.reflect.classTag[Contig].runtimeClass.asInstanceOf[Class[Contig]])
      contigDatumWriter.write(x, encoder)
    })

    encoder.flush()
    baos.flush()

    val put: Put = new Put(Bytes.toBytes(sequenceDictionaryId))
    put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("contig"), baos.toByteArray)
    table.put(put)
  }

  def loadSequenceDictionaryFromHBase(HbaseTableName: String,
                                      sequenceDictionaryId: String): SequenceDictionary = {

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(HbaseTableName))

    val myGet = new Get(Bytes.toBytes(sequenceDictionaryId))
    val result = table.get(myGet)
    val dataBytes = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("contig"))

    val contigDatumReader: DatumReader[Contig] =
      new SpecificDatumReader[Contig](scala.reflect.classTag[Contig]
        .runtimeClass
        .asInstanceOf[Class[Contig]])

    val decoder = DecoderFactory.get().binaryDecoder(dataBytes, null)
    var resultList = new ListBuffer[Contig]
    while (!decoder.isEnd) {
      resultList += contigDatumReader.read(null, decoder)
    }

    SequenceDictionary.fromAvro(resultList)

  }

  def saveSampleMetadataToHBase(hbaseTableName: String,
                                samples: Seq[Sample]): Unit = {

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(hbaseTableName))

    samples.foreach((x) => {
      val baos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)

      val sampleDatumWriter: DatumWriter[Sample] =
        new SpecificDatumWriter[Sample](scala.reflect.classTag[Sample].runtimeClass.asInstanceOf[Class[Sample]])
      sampleDatumWriter.write(x, encoder)
      encoder.flush()
      baos.flush()

      val curr_sampleid = x.getSampleId

      val put = new Put(Bytes.toBytes(curr_sampleid))
      put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("sampledata"), baos.toByteArray)
      table.put(put)

    })
  }

  def loadSampleMetadataFromHBase(hbaseTableName: String,
                                  sampleIds: List[String]): Seq[Sample] = {

    val conf = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(hbaseTableName))

    var resultList = new ListBuffer[Sample]

    sampleIds.foreach((sampleId) => {
      val myGet = new Get(Bytes.toBytes(sampleId))
      val result = table.get(myGet)
      val dataBytes = result.getValue(Bytes.toBytes("meta"), Bytes.toBytes("sampledata"))

      val sampleDatumReader: DatumReader[Sample] =
        new SpecificDatumReader[Sample](scala.reflect.classTag[Sample]
          .runtimeClass
          .asInstanceOf[Class[Sample]])

      val decoder = DecoderFactory.get().binaryDecoder(dataBytes, null)

      while (!decoder.isEnd) {
        resultList += sampleDatumReader.read(null, decoder)
      }

    })

    resultList

  }

  def loadVariantContextsFromHBase(sc: SparkContext,
                                   hbaseTableName: String,
                                   sampleIds: Option[List[String]] = None,
                                   sampleListFile: Option[String] = None,
                                   queryRegion: Option[ReferenceRegion] = None,
                                   partitions: Option[Int] = None): RDD[VariantContext] = {

    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    queryRegion.foreach { (currQueryRegion) =>
      {

        val (start, stop) = KeyStrategy1.getKeyRangePrefix(KeyStrategy1RangePrefixInfo(currQueryRegion))

        scan.setStartRow(start)
        scan.setStopRow(stop)
      }
    }

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    val sampleIdsFinal: List[String] =
      if (sampleListFile.isDefined) {
        val sampleIdsLst: List[String] = Source.fromFile(sampleListFile.get).getLines.toList
        sampleIdsLst.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIdsLst
      } else {
        sampleIds.get.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIds.get
      }

    val resultHBaseRDD = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

    val resultHBaseRDDrepar = if (partitions.isDefined) resultHBaseRDD.repartition(partitions.get)
    else resultHBaseRDD

    val result: RDD[VariantContext] = resultHBaseRDDrepar.mapPartitions((iterator) => {

      val genotypeDatumReader: DatumReader[Genotype] = new SpecificDatumReader[Genotype](scala.reflect.classTag[Genotype]
        .runtimeClass
        .asInstanceOf[Class[Genotype]])

      iterator.map((curr) => {
        var resultList = new ListBuffer[Genotype]
        sampleIdsFinal.foreach((sample) => {
          val sampleVal = curr._2.getColumnCells(Bytes.toBytes("g"), Bytes.toBytes(sample))
          val decoder = DecoderFactory.get().binaryDecoder(CellUtil.cloneValue(sampleVal.get(0)), null)
          while (!decoder.isEnd) {
            resultList += genotypeDatumReader.read(null, decoder)
          }
        })

        resultList
        val x = resultList.toList

        val firstGenotype: Genotype = x.head
        val currVar: RichVariant = RichVariant.genotypeToRichVariant(firstGenotype)
        val currRefPos = ReferencePosition(currVar.variant.getContigName, currVar.variant.getStart)

        val currVariantContext: VariantContext = new VariantContext(currRefPos, currVar, x)
        currVariantContext
      })

    })

    result
  }

  def createHBaseGenotypeTable(hbaseTableName: String, splitsFileName: String) {
    val conf = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(conf)

    val admin = connection.getAdmin

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.addFamily(new HColumnDescriptor("g".getBytes()).setCompressionType(Algorithm.GZ)
      .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      .setMaxVersions(1))

    val splits: Array[Array[Byte]] = Source.fromFile(splitsFileName).getLines.toArray
      .map(x => x.split(" "))
      .map(g => Bytes.toBytes(g(0) + "_"
        + String.format("%10s", g(1).toString).replace(' ', '0')))

    admin.createTable(tableDescriptor, splits)

    val hbaseTableNameMeta = hbaseTableName + "_meta"

    val tableDescriptorMeta = new HTableDescriptor(TableName.valueOf(hbaseTableNameMeta))
    tableDescriptorMeta.addFamily(new HColumnDescriptor("meta".getBytes())
      .setCompressionType(Algorithm.GZ)
      .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      .setMaxVersions(1))
    admin.createTable(tableDescriptorMeta)
  }

  def saveVariantContextRDDToHBase(sc: SparkContext,
                                   vcRdd: VariantContextRDD,
                                   hbaseTableName: String,
                                   sequenceDictionaryId: String,
                                   saveSequenceDictionary: Boolean = true): Unit = {

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    saveSampleMetadataToHBase(hbaseTableName + "_meta", vcRdd.samples)

    if (saveSequenceDictionary) saveSequenceDictionaryToHBase(hbaseTableName + "_meta", vcRdd.sequences, sequenceDictionaryId)

    val data = vcRdd.rdd.mapPartitions((iterator) => {
      val genotypebaos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val genotypeEncoder = EncoderFactory.get().binaryEncoder(genotypebaos, null)

      val genotypeDatumWriter: DatumWriter[Genotype] = new SpecificDatumWriter[Genotype](
        scala.reflect.classTag[Genotype]
          .runtimeClass
          .asInstanceOf[Class[Genotype]])

      val genotypesForHbase: Iterator[(Array[Byte], List[(String, Array[Byte])])] = iterator.map((putRecord) => {

        val myRowKey = KeyStrategy1.getKey(KeyStrategy1rowKeyInfo(putRecord.variant.variant.getContigName,
          putRecord.variant.variant.getStart.toInt,
          putRecord.variant.variant.getEnd.toInt,
          putRecord.variant.variant.getReferenceAllele,
          putRecord.variant.variant.getAlternateAllele))

        val genotypes: List[(String, Array[Byte])] = putRecord.genotypes.map((geno) => {
          genotypebaos.reset()
          genotypeDatumWriter.write(geno, genotypeEncoder)
          genotypeEncoder.flush()
          genotypebaos.flush()
          (geno.getSampleId, genotypebaos.toByteArray)
        }).toList

        (myRowKey, genotypes)

      })

      genotypesForHbase

    })

    data.hbaseBulkPut(hbaseContext,
      TableName.valueOf(hbaseTableName),
      (putRecord) => {
        val put = new Put(putRecord._1)

        putRecord._2.foreach((x) => {
          val sampleId: String = x._1
          val genoBytes: Array[Byte] = x._2
          put.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId), genoBytes)
        })
        put
      })
  }

  def saveVariantContextRDDToHBaseBulk(sc: SparkContext,
                                       vcRdd: VariantContextRDD,
                                       hbaseTableName: String,
                                       sequenceDictionaryId: String,
                                       saveSequenceDictionary: Boolean = true,
                                       numPartitions: Int = 0,
                                       stagingFolder: String): Unit = {

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    saveSampleMetadataToHBase(hbaseTableName + "_meta", vcRdd.samples)

    if (saveSequenceDictionary) saveSequenceDictionaryToHBase(hbaseTableName + "_meta", vcRdd.sequences, sequenceDictionaryId)

    val rddMaybeRepar = if (numPartitions > 0) vcRdd.rdd.repartition(numPartitions)
    else vcRdd.rdd

    val data: RDD[VariantContext] = vcRdd.rdd

    val genodata = rddMaybeRepar.mapPartitions((iterator) => {
      val genotypebaos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream()
      val genotypeEncoder = EncoderFactory.get().binaryEncoder(genotypebaos, null)

      val genotypeDatumWriter: DatumWriter[Genotype] = new SpecificDatumWriter[Genotype](
        scala.reflect.classTag[Genotype]
          .runtimeClass
          .asInstanceOf[Class[Genotype]])

      val genotypesForHbase: Iterator[(Array[Byte], List[(String, Array[Byte])])] = iterator.map((putRecord) => {

        val myRowKey = KeyStrategy1.getKey(KeyStrategy1rowKeyInfo(putRecord.variant.variant.getContigName,
          putRecord.variant.variant.getStart.toInt,
          putRecord.variant.variant.getEnd.toInt,
          putRecord.variant.variant.getReferenceAllele,
          putRecord.variant.variant.getAlternateAllele))

        val genotypes: List[(String, Array[Byte])] = putRecord.genotypes.map((geno) => {
          genotypebaos.reset()
          genotypeDatumWriter.write(geno, genotypeEncoder)
          genotypeEncoder.flush()
          genotypebaos.flush()
          (geno.getSampleId, genotypebaos.toByteArray)
        }).toList

        (myRowKey, genotypes)
      })
      genotypesForHbase
    })

    val familyHBaseWriterOptions = new java.util.HashMap[Array[Byte], FamilyHFileWriteOptions]
    val f1Options = new FamilyHFileWriteOptions("GZ", "ROW", 128, "FAST_DIFF")

    familyHBaseWriterOptions.put(Bytes.toBytes("g"), f1Options)

    genodata.hbaseBulkLoad(hbaseContext,
      TableName.valueOf(hbaseTableName),
      t => {
        val data = new ListBuffer[(KeyFamilyQualifier, Array[Byte])]
        val rowKey = t._1
        val family = Bytes.toBytes("g")

        t._2.foreach((x) => {

          val qualifier: String = x._1
          val value: Array[Byte] = x._2

          val mykeyFamilyQualifier = new KeyFamilyQualifier(rowKey, family, Bytes.toBytes(qualifier))

          data += Tuple2(mykeyFamilyQualifier, value)

        })

        data.iterator
      },
      stagingFolder, familyHBaseWriterOptions,
      compactionExclude = false,
      HConstants.DEFAULT_MAX_FILE_SIZE)
    ("hadoop fs -chmod -R 777 " + stagingFolder) !
    val conn = ConnectionFactory.createConnection(conf)
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, conn.getTable(TableName.valueOf(hbaseTableName)), conn.getRegionLocator(TableName.valueOf(hbaseTableName)))

  }

  def deleteGenotypeSamplesFromHBase(sc: SparkContext,
                                     hbaseTableName: String,
                                     sampleIds: Option[List[String]] = None,
                                     sampleListFile: Option[String] = None,
                                     partitions: Option[Int] = None): Unit = {

    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    val sampleIdsFinal: List[String] =
      if (sampleListFile.isDefined) {
        val sampleIdsLst: List[String] = Source.fromFile(sampleListFile.get).getLines.toList
        sampleIdsLst.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIdsLst
      } else {
        sampleIds.get.foreach((sampleId) => { scan.addColumn(Bytes.toBytes("g"), Bytes.toBytes(sampleId)) })
        sampleIds.get
      }

    val resultHBaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)

    hbaseContext.bulkDelete[(ImmutableBytesWritable, Result)](resultHBaseRDD,
      TableName.valueOf(hbaseTableName),
      putRecord => {
        val currDelete = new Delete(putRecord._2.getRow)
        sampleIdsFinal.foreach((sample) => {
          currDelete.addColumns(Bytes.toBytes("g"), Bytes.toBytes(sample))
        })
        currDelete
      },
      4)

  }

  def loadGenotypesFromHBaseToVariantContextRDD(sc: SparkContext,
                                                hbaseTableName: String,
                                                sampleIds: List[String],
                                                sequenceDictionaryId: String,
                                                queryRegion: Option[ReferenceRegion] = None, //one-based
                                                partitions: Option[Int] = None): VariantContextRDD = {

    val sequenceDictionary = loadSequenceDictionaryFromHBase(hbaseTableName + "_meta", sequenceDictionaryId)
    val sampleMetadata = loadSampleMetadataFromHBase(hbaseTableName + "_meta", sampleIds)

    val genotypes = loadVariantContextsFromHBase(sc,
      hbaseTableName,
      Option(sampleIds),
      queryRegion = queryRegion,
      partitions = partitions)

    VariantContextRDD(genotypes, sequenceDictionary, sampleMetadata)

  }

}