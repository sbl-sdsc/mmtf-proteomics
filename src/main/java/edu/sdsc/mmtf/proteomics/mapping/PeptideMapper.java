package edu.sdsc.mmtf.proteomics.mapping;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.proteomics.utils.PeptideMappingUtils;
import edu.sdsc.mmtf.proteomics.utils.SubstringJoin;
import edu.sdsc.mmtf.spark.datasets.PdbToUniProt;
import edu.sdsc.mmtf.spark.datasets.PolymerSequenceExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.webfilters.PdbjMineSearch;


public class PeptideMapper {

	public static void main(String[] args) throws IOException {
		
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(PeptideMapper.class.getSimpleName())
				.getOrCreate();
		
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

	    long start = System.nanoTime();

	    // file with modified peptides
	    String peptideFile = args[0];
	    String outputFile = peptideFile.substring(0, peptideFile.lastIndexOf(".")) + "_pdb.parquet";

	    System.out.println("Peptide file: " + peptideFile);
	    System.out.println("Output file : " + outputFile);
	    
	    String taxonomyQuery = "SELECT * FROM sifts.pdb_chain_taxonomy WHERE scientific_name = 'Homo sapiens'";

	    JavaPairRDD<String, StructureDataInterface> pdb = 
	            MmtfReader.readReducedSequenceFile(sc)
//	            .sample(false,  0.05, 123)
	            .flatMapToPair(new StructureToPolymerChains())
	            .filter(new ContainsLProteinChain())
	            .filter(new PdbjMineSearch(taxonomyQuery));
        
	    Dataset<Row> pdbSeq = PolymerSequenceExtractor.getDataset(pdb); 
	    
	    // create a unique set of PDB sequences for matching the peptides
	    Dataset<Row> sequences = pdbSeq.select(col("sequence").as("refSequence")).distinct().cache();
	    System.out.println("Unique PDB sequences    : " + sequences.count());
		    
		Dataset<Row> peptides = spark.read().option("header", "true").csv(peptideFile); 
		peptides = PeptideMappingUtils.removeSpacesFromColumnNames(peptides).cache();
		peptides.show(10, false);
		
		System.out.println("Unique modified peptides: " + peptides.count());
	
		// match peptide sequences with unique set of PDB sequences and join data
		SubstringJoin join = new SubstringJoin(peptides, "PeptideSequence", sequences, "refSequence");
		Dataset<Row> results = join.join();

		// TODO seems to timeout in broadcast
//		results = pdbSeq.join(broadcast(results), pdbSeq.col("sequence").equalTo((results.col("refSequence"))));
		results = pdbSeq.join(results, pdbSeq.col("sequence").equalTo((results.col("refSequence")))).drop("refSequence");

		// add pdb sequence fragment that matches the peptide, include flanking residues
		results = PeptideMappingUtils.addPdbSequence(results);

		// filter out non-tryptic peptides (left-flanking K/R and not right-flanking P)
//		results = results.filter("(pdbSeq LIKE 'K.%' OR pdbSeq LIKE 'R.%') AND NOT pdbSeq LIKE '%.P'").cache();
		
		// add pdb sequence positions for the modified residues	
		results = PeptideMappingUtils.addModPositions(results);  
        results = results.drop("sequence","refSequence");

        results = PeptideMappingUtils.flattenDataset(results);
        
        // change positions from a zero-based index to a one-based index to be consistent 
        // with UniProt mapping below
        results = results.withColumn("modPosition", col("modPositions").plus(lit(1)));
        
        Dataset<Row> unp = PdbToUniProt.getCachedResidueMappings();
        unp = unp.withColumnRenamed("structureChainId", "id");
        
        results = results.join(unp, (results.col("structureChainId").equalTo(unp.col("id")).and(results.col("modPosition").equalTo(unp.col("pdbSeqNum")))), "inner").drop("id", "modPositions");
 
        results = results.filter("pdbResNum IS NOT NULL");
        
        // remove redundant records
        results = results.dropDuplicates("modPeptide","pdbSeq","uniprotId","uniprotNum").cache();

		// summary
		System.out.println("Total matches    : " + results.count());
		System.out.println("Unique PDB chains: " + results.select("structureChainId").distinct().count());
//        results = results.filter("structureChainId LIKE '%1Q8G%'");
		
		results.show(10, false);
        
		results.coalesce(4).write().mode("overwrite").parquet(outputFile);
		
		sc.close();
		
		long end = System.nanoTime();
		System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	}
}
