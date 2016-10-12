package org.bdgenomics.adam.rdd.variation

import java.io.Serializable
import java.util
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.InnerShuffleRegionJoin
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.formats.avro.Variant
import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype, Variant }
import scala.collection.JavaConversions._
import org.bdgenomics.adam.models.{ ReferenceRegion, VariantContext }
import org.bdgenomics.adam.rdd.variation.VariantContextRDD
import org.bdgenomics.adam.rdd.InnerShuffleRegionJoin
import org.bdgenomics.adam.rdd.variation.GenotypeRDD

/**
 * Created by jp on 10/11/16.
 */

object GVCFutils {

  /**
   * Takes as input a genotypes from a set of gVCF files and a set of variant sites
   * Returns a GenotypeRDD
   * containing genotypes which were either a) called as explicit non reference variant sites in the gVCF data or
   * b) inferred reference homozygote genotypes based on gVCF reference blocks
   *
   *
   * @param gvcf input gVCF data
   * @param sites Sites over which to project genotypes from the gVCF file
   * @param sc
   * @return
   */

  def projectGVCFatSites(gvcf: VariantContextRDD, sites: VariantRDD, sc: SparkContext): GenotypeRDD =
    {

      // Convert input data from
      val geno: GenotypeRDD = gvcf.toGenotypeRDD
      //val sites: VariantRDD = sitesvcf.toVariantRDD

      val geno_rdd_keyed = geno.rdd.keyBy(v => ReferenceRegion(v.getContigName, v.getStart, v.getEnd))
      val sites_rdd_keyed = sites.rdd.keyBy(ReferenceRegion(_))
      val joinresult: RDD[(Genotype, Variant)] = InnerShuffleRegionJoin[Genotype, Variant](geno.sequences, 5000000, sc).partitionAndJoin(geno_rdd_keyed, sites_rdd_keyed)

      val projected_result = joinresult.map(x => {
        val projectedGeno: Genotype = if (x._1.getVariant.getAlternateAllele != null) { x._1 }
        else {
          val geno = new Genotype()
          val myVar = new Variant()
          myVar.setReferenceAllele(x._2.getReferenceAllele)
          myVar.setAlternateAllele(x._2.getAlternateAllele)
          geno.setVariant(myVar)
          geno.setSampleId(x._1.getSampleId)
          geno.setContigName(x._2.getContigName)
          geno.setStart(x._2.getStart)
          geno.setEnd(x._2.getEnd)
          geno.setAlleles(List(GenotypeAllele.Ref, GenotypeAllele.Ref))
          geno.setSampleId(x._1.getSampleId)
          geno
        }
        projectedGeno
      })

      val result_genotypeRDD = GenotypeRDD(projected_result, geno.sequences, geno.samples)
      result_genotypeRDD

    }

  def test1(sc: SparkContext): GenotypeRDD = {
    import org.bdgenomics.adam.rdd.ADAMContext
    import org.bdgenomics.adam.rdd.ADAMContext._
    import org.apache.spark.rdd.RDD
    import org.bdgenomics.adam.models.VariantContext
    import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
    import org.bdgenomics.adam.rdd.variation.GVCFutils

    val x = sc.loadVcf("test1.g.vcf")
    val y = sc.loadVcf("sites2.vcf").toVariantRDD

    val result = GVCFutils.projectGVCFatSites(x, y, sc)
    result.rdd.foreach(println)
    result

  }
  // Finds unique called (non-reference-block) sites from a GentypeRDD which was loaded
  // with one or more (using a glob) gVCF files

  // return values is a VariantRDD

  //def gVCFUnionSites

  /*
  def gvcfSitesProject ( r: RDD[(Genotype, Variant)] ): RDD[Genotype] =
  {
    r.map( x => {
      val projectedGeno: Genotype = if(x._1.getVariant.getAlternateAllele != null) { x._1 }
      else{  val geno = new Genotype()
        val myVar = new Variant()
        myVar.setReferenceAllele(x._2.getReferenceAllele)
        myVar.setAlternateAllele(x._2.getAlternateAllele)
        geno.setVariant(myVar)
        geno.setSampleId(x._1.getSampleId)
        geno.setContigName(x._2.getContigName )
        geno.setStart(x._2.getStart)
        geno.setEnd(x._2.getEnd)

        val hom_ref = List(GenotypeAllele.Ref, GenotypeAllele.Ref)
        geno.setAlleles(hom_ref)
        geno.setSampleId(x._1.getSampleId)
        geno

      }
      projectedGeno
    })
  }
*/

  /*
  def gvcfProject ( r: RDD[(VariantContext,VariantContext)] ): RDD[Genotype] =
  {
    r.map( x => {
      val projectedGeno = if(x._1.variant.getAlternateAllele != null) { x._1.genotypes.head }
      else{  val geno = new Genotype()
        val myVar = new Variant()
        myVar.setReferenceAllele(x._2.variant.getReferenceAllele)
        myVar.setAlternateAllele(x._2.variant.getAlternateAllele)
        geno.setVariant(myVar)
        geno.setSampleId(x._1.genotypes.head.getSampleId)
        geno.setContigName(x._2.variant.getContigName )
        geno.setStart(x._2.variant.getStart)
        geno.setEnd(x._2.variant.getEnd)

        val hom_ref = List(GenotypeAllele.Ref, GenotypeAllele.Ref)
        geno.setAlleles(hom_ref)
        geno.setSampleId(x._1.genotypes.head.getSampleId)
        geno

      }
      projectedGeno
    })
  }


  // assume gVCF contains data from a single sample gVCF file ( does not work with glob )
  def gvcfProjectSites( gvcf: RDD[org.bdgenomics.adam.models.VariantContext]): RDD[Int] =
  {
    val site_list = List(1,2,3)

    gvcf.mapPartitions(myIter => {
      val gvcf_list = myIter.toList

      val y: List[Int] = site_list.map(x => x)
      y.toIterator

    } )

  }

*/

}
