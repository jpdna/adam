/**
 * Created by jp on 7/24/16.
 */
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

@namespace("org.bdgenomics.formats.avro")
protocol BDG {

    /**
     Record for describing a reference assembly. Not used for storing the contents
     of said assembly.
     */
    record Contig {
        /**
         The name of this contig in the assembly (e.g., "chr1").
         */
        union { null, string } contigName = null;

        /**
         The length of this contig.
         */
        union { null, long } contigLength = null;

        /**
         The MD5 checksum of the assembly for this contig.
         */
        union { null, string } contigMD5 = null;

        /**
         The URL at which this reference assembly can be found.
         */
        union { null, string } referenceURL = null;

        /**
         The name of this assembly (e.g., "hg19").
         */
        union { null, string } assembly = null;

        /**
         The species that this assembly is for.
         */
        union { null, string } species = null;

        /**
         Optional 0-based index of this contig in a SAM file header that it was read
         from; helps output SAMs/BAMs with headers in the same order as they started
         with, before a conversion to ADAM.
         */
        union { null, int } referenceIndex = null;
    }

    record RecordGroupMetadata {
        /**
         Record group identifier.
         */
        union { null, string } name = null;

        /**
         Name of the sample that the record group is from.
         */
        union { null, string } sample = null;

        union { null, string } sequencingCenter = null;
        union { null, string } description = null;
        union { null, long } runDateEpoch = null;
        union { null, string } flowOrder = null;
        union { null, string } keySequence = null;
        union { null, string } library = null;
        union { null, int } predictedMedianInsertSize = null;
        union { null, string } platform = null;
        union { null, string } platformUnit = null;
    }

    record AlignmentRecord {

        /**
         Read number within the array of fragment reads.
         */
        union { int, null } readInFragment = 0;

        /**
         The reference sequence details for the reference chromosome that
         this read is aligned to. If the read is unaligned, this field should
         be null.
         */
        union { null, string } contigName = null;

        /**
         0 based reference position for the start of this read's alignment.
         Should be null if the read is unaligned.
         */
        union { null, long } start = null;

        /**
         0 based reference position where this read used to start before
         local realignment.
         Stores the same data as the OP field in the SAM format.
         */
        union { null, long } oldPosition = null;

        /**
         0 based reference position for the end of this read's alignment.
         Should be null if the read is unaligned.
         */
        union { null, long } end = null;

        /**
         The global mapping quality of this read.
         */
        union { null, int } mapq = null;

        /**
         The name of this read. This should be unique within the read group
         that this read is from, and can be used to identify other reads that
         are derived from a single fragment.
         */
        union { null, string } readName = null;

        /**
         The bases in this alignment. If the read has been hard clipped, this may
         not represent all the bases in the original read.
         */
        union { null, string } sequence = null;

        /**
         The per-base quality scores in this alignment. If the read has been hard
         clipped, this may not represent all the bases in the original read.
         Additionally, if the error scores have been recalibrated, this field
         will not contain the original base quality scores.
         */
        union { null, string } qual = null;

        /**
         The Compact Ideosyncratic Gapped Alignment Report (CIGAR) string that
         describes the local alignment of this read. Contains {length, operator}
         pairs for all contiguous alignment operations. The operators include:

         * M, ALIGNMENT_MATCH: An alignment match indicates that a sequence can be
         aligned to the reference without evidence of an INDEL. Unlike the
         SEQUENCE_MATCH and SEQUENCE_MISMATCH operators, the ALIGNMENT_MATCH
         operator does not indicate whether the reference and read sequences are an
         exact match.
         * I, INSERT: The insert operator indicates that the read contains evidence of
         bases being inserted into the reference.
         * D, DELETE: The delete operator indicates that the read contains evidence of
         bases being deleted from the reference.
         * N, SKIP: The skip operator indicates that this read skips a long segment of
         the reference, but the bases have not been deleted. This operator is
         commonly used when working with RNA-seq data, where reads may skip long
         segments of the reference between exons.
         * S, CLIP_SOFT: The soft clip operator indicates that bases at the start/end
         of a read have not been considered during alignment. This may occur if the
         majority of a read maps, except for low quality bases at the start/end of
         a read. Bases that are soft clipped will still be stored in the read.
         * H, CLIP_HARD: The hard clip operator indicates that bases at the start/end of
         a read have been omitted from this alignment. This may occur if this linear
         alignment is part of a chimeric alignment, or if the read has been trimmed
         (e.g., during error correction, or to trim poly-A tails for RNA-seq).
         * P, PAD: The pad operator indicates that there is padding in an alignment.
         * =, SEQUENCE_MATCH: This operator indicates that this portion of the aligned
         sequence exactly matches the reference (e.g., all bases are equal to the
         reference bases).
         * X, SEQUENCE_MISMATCH: This operator indicates that this portion of the
         aligned sequence is an alignment match to the reference, but a sequence
         mismatch (e.g., the bases are not equal to the reference). This can
         indicate a SNP or a read error.
         */
        union { null, string } cigar = null;

        /**
         Stores the CIGAR string present before local indel realignment.
         Stores the same data as the OC field in the SAM format.
         */
        union { null, string } oldCigar = null;

        /**
         The number of bases in this read/alignment that have been trimmed from the
         start of the read. By default, this is equal to 0. If the value is non-zero,
         that means that the start of the read has been hard-clipped.
         */
        union { int, null } basesTrimmedFromStart = 0;

        /**
         The number of bases in this read/alignment that have been trimmed from the
         end of the read. By default, this is equal to 0. If the value is non-zero,
         that means that the end of the read has been hard-clipped.
         */
        union { int, null } basesTrimmedFromEnd = 0;

        // Read flags (all default to false)
        union { boolean, null } readPaired = false;
        union { boolean, null } properPair = false;
        union { boolean, null } readMapped = false;
        union { boolean, null } mateMapped = false;
        union { boolean, null } failedVendorQualityChecks = false;
        union { boolean, null } duplicateRead = false;

        /**
         True if this alignment is mapped as a reverse compliment. This field
         defaults to false.
         */
        union { boolean, null } readNegativeStrand = false;

        /**
         True if the mate pair of this alignment is mapped as a reverse compliment.
         This field defaults to false.
         */
        union { boolean, null } mateNegativeStrand = false;

        /**
         True if this alignment is either the best linear alignment,
         or the first linear alignment in a chimeric alignment. Defaults to false.
         */
        union { boolean, null } primaryAlignment = false;

        /**
         True if this alignment is a lower quality linear alignment
         for a multiply-mapped read. Defaults to false.
         */
        union { boolean, null } secondaryAlignment = false;

        /**
         True if this alignment is a non-primary linear alignment in
         a chimeric alignment. Defaults to false.
         */
        union { boolean, null } supplementaryAlignment = false;

        // Commonly used optional attributes
        union { null, string } mismatchingPositions = null;
        union { null, string } origQual = null;

        // Remaining optional attributes flattened into a string
        union { null, string } attributes = null;

        // record group identifer from sequencing run
        union { null, string } recordGroupName = null;
        union { null, string } recordGroupSample = null;

        /**
         The start position of the mate of this read. Should be set to null if the
         mate is unaligned, or if the mate does not exist.
         */
        union { null, long } mateAlignmentStart = null;

        /**
         The end position of the mate of this read. Should be set to null if the
         mate is unaligned, or if the mate does not exist.
         */
        union { null, long } mateAlignmentEnd = null;

        /**
         The reference contig of the mate of this read. Should be set to null if the
         mate is unaligned, or if the mate does not exist.
         */
        union { null, string } mateContigName = null;

        /**
         The distance between this read and it's mate as inferred from alignment.
         */
        union { null, long } inferredInsertSize = null;
    }

    /**
     The DNA fragment that is was targeted by the sequencer, resulting in
     one or more reads.
     */
    record Fragment {
        /**
         The name of this Fragment.
         */
        union { null, string } readName = null;

        union { null, string } instrument = null;
        union { null, string } runId = null;

        /**
         Fragment's insert size derived from alignment, if the reads have been
         aligned.
         */
        union { null, int } fragmentSize = null;

        /**
         The sequences read from this fragment.
         */
        array<AlignmentRecord> alignments = [];
    }

    /**
     Stores a contig of nucleotides; this may be a reference chromosome, may be an
     assembly, may be a BAC. Very long contigs (>1Mbp) need to be split into fragments.
     It seems that they are too long to load in a single go. For best performance,
     it seems like 10kbp is a good point at which to start splitting contigs into
     fragments.
     */
    record NucleotideContigFragment {

        /**
         The contig identification descriptor for this contig.
         */
        union { null, Contig } contig = null;

        /**
         A description for this contig. When importing from FASTA, the FASTA header
         description line should be stored here.
         */
        union { null, string } description = null;

        /**
         The sequence of bases in this fragment.
         */
        union { null, string } fragmentSequence = null;

        /**
         In a fragmented contig, the position of this fragment in the set of fragments.
         Can be null if the contig is not fragmented.
         */
        union { null, int } fragmentNumber = null;

        /**
         The position of the first base of this fragment in the overall contig. E.g.,
         if all fragments are 10kbp and this is the third fragment in the contig,
         the start position would be 20000L.
         */
        union { null, long } fragmentStartPosition = null;

        /**
         The position of the last base of this fragment in the overall contig. E.g.,
         if all fragments are 10kbp and this is the third fragment in the contig,
         the end position would be 29999L.
         */
        union { null, long } fragmentEndPosition = null;

        /**
         The length of this fragment.
         */
        union { null, long } fragmentLength = null;

        /**
         The total count of fragments that this contig has been broken into. Can be
         null if the contig is not fragmented.
         */
        union { null, int } numberOfFragmentsInContig = null; // total number of fragments in contig
    }

    /**
     Descriptors for the type of a structural variant. The most specific descriptor
     should be used, if possible. E.g., duplication should be used instead of
     insertion if the inserted sequence is not novel. Tandem duplication should
     be used instead of duplication if the duplication is known to follow the
     duplicated sequence.
     */
enum StructuralVariantType {
        DELETION,
            INSERTION,
            INVERSION,
            MOBILE_INSERTION,
            MOBILE_DELETION,
            DUPLICATION,
            TANDEM_DUPLICATION
    }

    /**
     Structural variant.
     */
    record StructuralVariant {

        /**
         The type of this structural variant.
         */
        union { null, StructuralVariantType } type = null;

        /**
         The URL of the FASTA/NucleotideContig assembly for this structural variant,
         if one is available.
         */
        union { null, string } assembly = null;

        /**
         Whether this structural variant call has precise breakpoints or not. Default
         value is true. If the call is imprecise, confidence intervals should be provided.
         */
        union { boolean, null } precise = true;

        /**
         The size of the confidence window around the start of the structural variant.
         */
        union { null, int } startWindow = null;

        /**
         The size of the confidence window around the end of the structural variant.
         */
        union { null, int } endWindow = null;
    }

    /**
     Variant.
     */
    record Variant {

        /**
         The Phred scaled error probability of a variant, given the probabilities of
         the variant in a population.
         */
        union { null, int } variantErrorProbability = null;

        /**
         The reference contig that this variant exists on.
         */
        union { null, string } contigName = null;

        /**
         The 0-based start position of this variant on the reference contig.
         */
        union { null, long } start = null;

        /**
         The 0-based, exclusive end position of this variant on the reference contig.
         */
        union { null, long } end = null;

        /**
         A string describing the reference allele at this site.
         */
        union { null, string } referenceAllele = null;

        /**
         A string describing the variant allele at this site. Should be left null if
         the site is a structural variant.
         */
        union { null, string } alternateAllele = null;

        /**
         The structural variant at this site, if the alternate allele is a structural
         variant. If the site is not a structural variant, this field should be left
         null.
         */
        union { null, StructuralVariant } structuralVariant = null;

        /**
         A boolean describing whether this variant call is somatic; in this case, the
         `referenceAllele` will have been observed in another sample.  VCF INFO header line
         key SOMATIC.
         */
        union { boolean, null } somatic = false;
    }

    /**
     An enumeration that describes the allele that corresponds to a genotype. Can take
     the following values:

     * REF: The genotype is the reference allele
     * ALT: The genotype is the alternate allele
     * OTHER_ALT: The genotype is an unspecified other alternate allele. This occurs
     in our schema when we have split a multi-allelic genotype into two genotype
     records.
     * NO_CALL: The genotype could not be called.
     */
enum GenotypeAllele {
        REF,
            ALT,
            OTHER_ALT,
            NO_CALL
    }

    /**
     An enumeration that describes the characteristics of a genotype at a site. Can
     take the following values:

     * HOM_REF: All genotypes at this site were called as the reference allele.
     * HET: Genotypes at this site were called as multiple different alleles. This
     most commonly occurs if a diploid sample's genotype contains one reference
     and one variant allele, but can also occur if the genotype contains multiple
     alternate alleles.
     * HOM_ALT: All genotypes at this site were called as a single alternate allele.
     * NO_CALL: The genotype could not be called at this site.
     */
enum GenotypeType {
        HOM_REF,
            HET,
            HOM_ALT,
            NO_CALL
    }

    /**
     Genotype annotation; all stats that, inside a VCF, are stored outside of the
     sample but are computed based on the samples.  For instance,  MAPQ0 is an aggregate
     stat computed from all samples and stored inside the INFO line.
     */
    record GenotypeAnnotation {

        /**
         A list of filters (normally quality filters) this variant has failed.
         */
        array<string> filtersFailed = [];

        /**
         A list of filters (normally quality filters) this variant has passed.
         */
        array<string> filtersPassed = [];

        /**
         True if the reads covering this site were randomly downsampled to reduce coverage.
         */
        union { null, boolean } downsampled = null;

        /**
         The Wilcoxon rank-sum test statistic of the base quality scores. The base quality
         scores are separated by whether or not the base supports the reference or the
         alternate allele.
         */
        union { null, float } baseQualityRankSum = null;

        /**
         The Fisher's exact test score for the strand bias of the reference and alternate
         alleles. Stored as a phred scaled probability. Thus, if:

         * a = The number of positive strand reads covering the reference allele
         * b = The number of positive strand reads covering the alternate allele
         * c = The number of negative strand reads covering the reference allele
         * d = The number of negative strand reads covering the alternate allele

         This value takes the score:

         -10 log((a + b)! * (c + d)! * (a + c)! * (b + d)! / (a! b! c! d! n!)

         Where n = a + b + c + d.
         */
        union { null, float } fisherStrandBiasPValue = null;

        /**
         The root mean square of the mapping qualities of reads covering this site.
         */
        union { null, float } rmsMappingQuality = null;

        /**
         The number of reads at this site with mapping quality equal to 0.
         */
        union { null, int } mappingQualityZeroReads = null;

        /**
         The Wilcoxon rank-sum test statistic of the mapping quality scores. The mapping
         quality scores are separated by whether or not the read supported the reference or the
         alternate allele.
         */
        union { null, float } mappingQualityRankSum = null;

        /**
         The Wilcoxon rank-sum test statistic of the position of the base in the read at this site.
         The positions are separated by whether or not the base supports the reference or the
         alternate allele.
         */
        union { null, float } readPositionRankSum = null;

        /**
         The log scale prior probabilities of the various genotype states at this site.
         The number of elements in this array should be equal to the ploidy at this
         site, plus 1.
         */
        array<float> genotypePriors = [];

        /**
         The log scaled posterior probabilities of the various genotype states at this site,
         in this sample. The number of elements in this array should be equal to the ploidy at
         this site, plus 1.
         */
        array<float> genotypePosteriors = [];

        /**
         The log-odds ratio of being a true vs. false variant under a trained statistical model.
         This model can be a multivariate Gaussian mixture, support vector machine, etc.
         */
        union { null, float } vqslod = null;

        /**
         If known, the feature that contributed the most to this variant being classified as
         a false variant.
         */
        union { null, string } culprit = null;

        /**
         Additional genotype attributes that do not fit into the standard fields above.
         */
        map<string> attributes = {};
    }

    /**
     Genotype.
     */
    record Genotype {

        /**
         The variant called at this site.
         */
        union { null, Variant } variant = null;

        /**
         The reference contig that this genotype's variant exists on.
         */
        union { null, string } contigName = null;

        /**
         The 0-based start position of this genotype's variant on the reference contig.
         */
        union { null, long } start = null;

        /**
         The 0-based, exclusive end position of this genotype's variant on the reference contig.
         */
        union { null, long } end = null;

        /**
         Statistics collected at this site, if available.
         */
        union { null, GenotypeAnnotation } genotypeAnnotation = null;

        /**
         The unique identifier for this sample.
         */
        union { null, string } sampleId = null;

        /**
         An array describing the genotype called at this site. The length of this
         array is equal to the ploidy of the sample at this site. This array may
         reference OtherAlt alleles if this site is multi-allelic in this sample.
         */
        array<GenotypeAllele> alleles = [];

        /**
         The expected dosage of the alternate allele in this sample.
         */
        union { null, float } expectedAlleleDosage = null;

        /**
         The number of reads that show evidence for the reference at this site.
         */
        union { null, int } referenceReadDepth = null;

        /**
         The number of reads that show evidence for this alternate allele at this site.
         */
        union { null, int } alternateReadDepth = null;

        /**
         The total number of reads at this site. May not equal (alternateReadDepth +
         referenceReadDepth) if this site shows evidence of multiple alternate alleles.

         Analogous to VCF's DP.
         */
        union { null, int } readDepth = null;

        /**
         The minimum number of reads seen at this site across samples when joint
         calling variants.

         Analogous to VCF's MIN_DP.
         */
        union { null, int } minReadDepth = null;

        /**
         The phred-scaled probability that we're correct for this genotype call.

         Analogous to VCF's GQ.
         */
        union { null, int } genotypeQuality = null;

        /**
         Log scaled likelihoods that we have n copies of this alternate allele.
         The number of elements in this array should be equal to the ploidy at this
         site, plus 1.

         Analogous to VCF's PL.
         */
        array<float> genotypeLikelihoods = [];

        /**
         Log scaled likelihoods that we have n non-reference alleles at this site.
         The number of elements in this array should be equal to the ploidy at this
         site, plus 1.
         */
        array<float> nonReferenceLikelihoods = [];

        /**
         Component statistics which comprise the Fisher's Exact Test to detect strand bias.
         If populated, this element should have length 4.
         */
        array<int> strandBiasComponents = [];

        /**
         We split multi-allelic VCF lines into multiple
         single-alternate records.  This bit is set if that happened for this
         record.
         */
        union { boolean, null } splitFromMultiAllelic = false;

        /**
         True if this genotype is phased.
         */
        union { boolean, null } phased = false;

        /**
         The ID of this phase set, if this genotype is phased. Should only be populated
         if isPhased == true; else should be null.
         */
        union { null, int } phaseSetId = null;

        /**
         Phred scaled quality score for the phasing of this genotype, if this genotype
         is phased. Should only be populated if isPhased == true; else should be null.
         */
        union { null, int } phaseQuality = null;
    }

    /**
     Errors, warnings, or informative messages regarding variant annotation accuracy.
     */
enum VariantAnnotationMessage {

        /**
         Chromosome does not exist in reference genome database. Typically indicates
         a mismatch between the chromosome names in the input file and the chromosome
         names used in the reference genome.  Message code E1.
         */
        ERROR_CHROMOSOME_NOT_FOUND,

        /**
         The variant's genomic coordinate is greater than chromosome's length.
         Message code E2.
         */
            ERROR_OUT_OF_CHROMOSOME_RANGE,

        /**
         The 'REF' field in the input VCF file does not match the reference genome.
         This warning may indicate a conflict between input data and data from
         reference genome (for instance is the input VCF was aligned to a different
         reference genome).  Message code W1.
         */
            WARNING_REF_DOES_NOT_MATCH_GENOME,

        /**
         Reference sequence is not available, thus no inference could be performed.
         Message code W2.
         */
            WARNING_SEQUENCE_NOT_AVAILABLE,

        /**
         A protein coding transcript having a non­multiple of 3 length. It indicates
         that the reference genome has missing information about this particular
         transcript.  Message code W3.
         */
            WARNING_TRANSCRIPT_INCOMPLETE,

        /**
         A protein coding transcript has two or more STOP codons in the middle of
         the coding sequence (CDS). This should not happen and it usually means the
         reference genome may have an error in this transcript.  Message code W4.
         */
            WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS,

        /**
         A protein coding transcript does not have a proper START codon. It is
         rare that a real transcript does not have a START codon, so this probably
         indicates an error or missing information in the reference genome.
         Message code W5.
         */
            WARNING_TRANSCRIPT_NO_START_CODON,

        /**
         Variant has been realigned to the most 3­prime position within the
         transcript. This is usually done to to comply with HGVS specification
         to always report the most 3­prime annotation.  Message code I1.
         */
            INFO_REALIGN_3_PRIME,

        /**
         This effect is a result of combining more than one variants (e.g. two
         consecutive SNPs that conform an MNP, or two consecutive frame_shift
         variants that compensate frame).  Message code I2.
         */
            INFO_COMPOUND_ANNOTATION,

        /**
         An alternative reference sequence was used to calculate this annotation
         (e.g. cancer sample comparing somatic vs. germline).  Message code I3.
         */
            INFO_NON_REFERENCE_ANNOTATION
    }

    /**
     Annotation of a variant in the context of a feature, typically a transcript.
     */
    record TranscriptEffect {

        /**
         One or more annotations (also referred to as effects or consequences) of the
         variant in the context of the feature identified by featureId.  Must be
         Sequence Ontology (SO, see http://www.sequenceontology.org) term names, e.g.
         stop_gained, missense_variant, synonymous_variant, upstream_gene_variant.
         */
        array<string> effects = [];

        /**
         Common gene name (HGNC), e.g. BRCA2.  May be closest gene if annotation
         is intergenic.
         */
        union { null, string } geneName = null;

        /**
         Gene identifier, e.g. Ensembl Gene identifier, ENSG00000139618.  May be
         closest gene if annotation is intergenic.
         */
        union { null, string } geneId = null;

        /**
         Feature type, may use Sequence Ontology term names.  Typically transcript.
         */
        union { null, string } featureType = null;

        /**
         Feature identifier, e.g. Ensembl Transcript identifier and version, ENST00000380152.7.
         */
        union { null, string } featureId = null;

        /**
         Feature biotype, e.g. Protein coding or Non coding.  See http://vega.sanger.ac.uk/info/about/gene_and_transcript_types.html.
         */
        union { null, string } biotype = null;

        /**
         Intron or exon rank.
         */
        union { null, int } rank = null;

        /**
         Total number of introns or exons.
         */
        union { null, int } total = null;

        /**
         HGVS.g description of the variant.  See http://www.hgvs.org/mutnomen/recs-DNA.html.
         */
        union { null, string } genomicHgvs = null;

        /**
         HGVS.c description of the variant.  See http://www.hgvs.org/mutnomen/recs-DNA.html.
         */
        union { null, string } transcriptHgvs = null;

        /**
         HGVS.p description of the variant, if coding.  See http://www.hgvs.org/mutnomen/recs-prot.html.
         */
        union { null, string } proteinHgvs = null;

        /**
         cDNA sequence position (one based).
         */
        union { null, int } cdnaPosition = null;

        /**
         cDNA sequence length in base pairs (one based).
         */
        union { null, int } cdnaLength = null;

        /**
         Coding sequence position (one based, includes START and STOP codons).
         */
        union { null, int } cdsPosition = null;

        /**
         Coding sequence length in base pairs (one based, includes START and STOP codons).
         */
        union { null, int } cdsLength = null;

        /**
         Protein sequence position (one based, includes START but not STOP).
         */
        union { null, int } proteinPosition = null;

        /**
         Protein sequence length in amino acids (one based, includes START but not STOP).
         */
        union { null, int } proteinLength = null;

        /**
         Distance in base pairs to the feature.
         */
        union { null, int } distance = null;

        /**
         Zero or more errors, warnings, or informative messages regarding variant annotation accuracy.
         */
        array<VariantAnnotationMessage> messages = [];
    }

    /**
     Variant annotation.
     */
    record VariantAnnotation {

        /**
         Variant for this annotation.
         */
        union { null, Variant } variant = null;

        /**
         Ancestral allele, VCF INFO header line reserved key AA.
         */
        union { null, string } ancestralAllele = null;

        /**
         Allele count, VCF INFO header line reserved key AC.
         */
        union { null, string } alleleCount = null;

        /**
         Total read depth, VCF INFO header line reserved key AD.
         */
        union { null, int } readDepth = null;

        /**
         Forward strand read depth, VCF INFO header line reserved key ADF.
         */
        union { null, int } forwardReadDepth = null;

        /**
         Reverse strand read depth, VCF INFO header line reserved key ADR.
         */
        union { null, int } reverseReadDepth = null;

        /**
         Minor allele frequency, VCF INFO header line reserved key AF.
         */
        union { null, string } alleleFrequency = null;

        /**
         RMS base quality, VCF INFO header line reserved key BQ.
         */
        union { null, float } rmsBaseQuality = null;

        /**
         CIGAR string describing how to align an alternate allele to the reference
         allele, VCF INFO header line reserved key CIGAR.
         */
        union { null, string } cigar = null;

        /**
         Membership in dbSNP, VCF info header line reserved key DB.
         */
        union { null, boolean } dbSnp = null;

        /**
         Combined depth across samples, VCF INFO header line reserved key DP.
         */
        union { null, int } combinedDepth = null;

        /**
         Membership in HapMap2, VCF INFO header line reserved key H2.
         */
        union { null, boolean } hapMap2 = null;

        /**
         Membership in HapMap3, VCF INFO header line reserved key H3.
         */
        union { null, boolean } hapMap3 = null;

        /**
         RMS mapping quality, VCF INFO header line reserved key MQ.
         */
        union { null, float } rmsMappingQuality = null;

        /**
         Number of MAPQ == 0 reads covering this record, VCF INFO header line reserved key MQ0.
         */
        union { null, int } mappingQualityZeroReads = null;

        /**
         Number of samples with data, VCF INFO header line reserved key NS.
         */
        union { null, int } samplesWithData = null;

        /**
         Strand bias, VCF INFO header line reserved key SB.
         */
        union { null, float } strandBias = null;

        /**
         Validated by follow up experiment, VCF INFO header line reserved key VALIDATED.
         */
        union { null, boolean } validated = null;

        /**
         Membership in 1000 Genomes, VCF INFO header line reserved key 1000G.
         */
        union { null, boolean } thousandGenomes = null;

        /**
         Zero or more transcript effects, predicted by a tool such as SnpEff or Ensembl VEP,
         one per transcript (or other feature).
         */
        array<TranscriptEffect> transcriptEffects = [];

        /**
         Additional variant attributes that do not fit into the standard fields above.
         */
        map<string> attributes = {};
    }

    /**
     Strand of an alignment or feature.
     */
enum Strand {

        /**
         Forward ("+") strand.
         */
        FORWARD,

        /**
         Reverse ("-") strand.
         */
            REVERSE,

        /**
         Independent or not stranded (".").
         */
            INDEPENDENT,

        /**
         Strandedness is relevant, but unknown ("?").
         */
            UNKNOWN
    }

    /**
     Database cross reference in GFF3 style DBTAG:ID format.
     */
    record Dbxref {

        /**
         Database tag in GFF3 style DBTAG:ID format, e.g. EMBL in EMBL:AA816246.
         */
        union { null, string } db = null;

        /**
         Accession number in GFF3 style DBTAG:ID format, e.g. AA816246 in EMBL:AA816246.
         */
        union { null, string } accession = null;
    }

    /**
     Ontology term cross reference in GFF3 style DBTAG:ID format.
     */
    record OntologyTerm {

        /**
         Ontology abbreviation in GFF3 style DBTAG:ID format, e.g. GO in GO:0046703.
         */
        union { null, string } db = null;

        /**
         Ontology term accession number or identifer in GFF3 style DBTAG:ID format,
         e.g. 0046703 in GO:0046703.
         */
        union { null, string } accession = null;
    }

    /**
     Feature, such as those represented in native file formats BED, GFF2/GTF,
     GFF3, IntervalList, and NarrowPeak.
     */
    record Feature {

        /**
         Identifier for this feature.  ID tag in GFF3.
         */
        union { null, string } featureId = null;

        /**
         Display name for this feature, e.g. DVL1.  Name tag in GFF3, optional column 4 "name"
         in BED format.
         */
        union { null, string } name = null;

        /**
         Source of this feature, typically the algorithm or operating procedure that generated
         this feature, e.g. GeneWise.  Column 2 "source" in GFF3.
         */
        union { null, string } source = null;

        /**
         Feature type, constrained by some formats to a term from the Sequence Ontology (SO),
         e.g. gene, mRNA, exon, or a SO accession number (SO:0000704, SO:0000234, SO:0000147,
         respectively).  Column 3 "type" in GFF3.
         */
        union { null, string } featureType = null;

        /**
         Contig this feature is located on.  Column 1 "seqid" in GFF3, column 1 "chrom"
         in BED format.
         */
        union { null, string } contigName = null;

        /**
         Start position for this feature, in 0-based coordinate system with closed-open
         intervals.  This may require conversion from the coordinate system of the native
         file format.  Column 4 "start" in GFF3, column 2 "chromStart" in BED format.
         */
        union { null, long } start = null;

        /**
         End position for this feature, in 0-based coordinate system with closed-open
         intervals.  This may require conversion from the coordinate system of the native
         file format.  Column 5 "end" in GFF3, column 3 "chromEnd" in BED format.
         */
        union { null, long } end = null;

        /**
         Strand for this feature.  Column 7 "strand" in GFF3, optional column 6 "strand"
         in BED format.
         */
        union { null, Strand } strand = null;

        /**
         For features of type "CDS", the phase indicates where the feature begins with reference
         to the reading frame.  The phase is one of the integers 0, 1, or 2, indicating the number
         of bases that should be removed from the beginning of this feature to reach the first base
         of the next codon.  Column 8 "phase" in GFF3.
         */
        union { null, int } phase = null;

        /**
         For features of type "CDS", the frame indicates whether the first base of the CDS segment is
         the first (frame 0), second (frame 1) or third (frame 2) in the codon of the ORF.  Column 8
         "frame" in GFF2/GTF format.
         */
        union { null, int } frame = null;

        /**
         Score for this feature.  Column 6 "score" in GFF3, optional column 5
         "score" in BED format.
         */
        union { null, double } score = null;

        /**
         Gene identifier, e.g. ENSG00000107404.  gene_id tag in GFF2/GTF.
         */
        union { null, string } geneId = null;

        /**
         Transcript identifier, e.g. ENST00000378891.  transcript_id tag in GFF2/GTF.
         */
        union { null, string } transcriptId = null;

        /**
         Exon identifier, e.g. ENSE00001479184.  exon_id tag in GFF2/GTF.
         */
        union { null, string } exonId = null;

        /**
         Secondary names or identifiers for this feature.  Alias tag in GFF3.
         */
        array<string> aliases = [];

        /**
         Parent feature identifiers.  Parent tag in GFF3.
         */
        array<string> parentIds = [];

        /**
         Target of a nucleotide-to-nucleotide or protein-to-nucleotide alignment
         feature.  The format of the value is "target_id start end [strand]", where
         strand is optional and may be "+" or "-".  Target tag in GFF3.
         */
        union { null, string } target = null;

        /**
         Alignment of the feature to the target in CIGAR format.  Gap tag in GFF3.
         */
        union { null, string } gap = null;

        /**
         Used to disambiguate the relationship between one feature and another when
         the relationship is a temporal one rather than a purely structural "part of"
         one.  Derives_from tag in GFF3.
         */
        union { null, string } derivesFrom = null;

        /**
         Notes or comments for this feature.  Note tag in GFF3.
         */
        array<string> notes = [];

        /**
         Database cross references for this feature.  Dbxref tag in GFF3.
         */
        array<Dbxref> dbxrefs = [];

        /**
         Ontology term cross references for this feature.  Ontology_term tag in GFF3.
         */
        array<OntologyTerm> ontologyTerms = [];

        /**
         True if this feature is circular.  Is_circular tag in GFF3.
         */
        union { null, boolean } circular = null;

        /**
         Additional feature attributes.  Column 9 "attributes" in GFF3, excepting those
         reserved tags parsed into other fields, such as parentIds, dbxrefs, and ontologyTerms.
         */
        map<string> attributes = {};
    }

    /**
     Sample.
     */
    record Sample {

        /**
         Identifier for this sample, e.g. IDENTIFIERS &rarr; PRIMARY_ID or other
         subelements of IDENTIFERS in SRA metadata, sample tag SM in read group @RG header lines
         in SAM/BAM files, or sample ID from the header or ##SAMPLE=&lt;ID=S_ID meta-information
         lines in VCF files.
         */
        union { null, string } sampleId = null;

        /**
         Descriptive name for this sample, e.g. SAMPLE_NAME &rarr; TAXON_ID, COMMON_NAME,
         INDIVIDUAL_NAME, or other subelements of SAMPLE_NAME in SRA metadata.
         */
        union { null, string } name = null;

        /**
         Map of attributes.  Common attributes may include: SRA metadata not mentioned above,
         e.g. SAMPLE &rarr; TITLE, SAMPLE &rarr; DESCRIPTION, and SAMPLE_ATTRIBUTES; ENA default
         sample checklist attributes such as cell_type, dev_stage, and germline; and Genomes,
         Mixture, and Description from sample meta-information lines in VCF files.
         */
        map<string> attributes = {};
    }
}