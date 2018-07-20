package edu.sdsc.mmtf.proteomics.io;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.regexp_replace;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MaestroReader {
    
    public static Dataset<Row> readPeptideFile(String fileName) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();
        
        String delimiter = "";
        if (fileName.endsWith(".csv")) {
            delimiter = ",";
        } else if (fileName.endsWith(".tsv")) {
            delimiter = "\t";
        }
        
        Dataset<Row> peptides =  spark.read()
                .format("csv")
                .option("delimiter", delimiter)
                .option("header", "true")
                .option("inferSchema", "true")
                .load(fileName);
        
        peptides = peptides.select("Peptide").distinct();
        
        // remove carbamidomethylated cysteine modification (C,57.021), this is an experimental artifact
        peptides = peptides.withColumn("modPeptide", regexp_replace(col("Peptide"), "\\(C,57.021\\)", "C"));
        
        // keep only modified peptides (X,deltaMass) and peptides with N-terminal acetylation [42.011]
        peptides = peptides.filter("modPeptide LIKE '%(%' OR modPeptide LIKE '%[42.011]%'");
 
        // remove N-terminal modification annotation, e.g. [42.011]AA -> AA
        peptides = peptides.withColumn("pepSeq", regexp_replace(col("modPeptide"), "\\[(.*?)\\]", ""));

        // remove modification annotation, e.g., F(C,57.021)P -> FCP
        peptides = peptides.withColumn("pepSeq", regexp_replace(col("modPeptide"), "[(]|[,](.*?)[)]", ""));

        // extract the peptide sequence between the flanking residues
        // e.g., R.DSLLQDGEFSMDLR.T -> DSLLQDGEFSMDLR
        peptides = peptides.withColumn("pepSeq", regexp_extract(col("pepSeq"), "[.](.*?)[.]", 1));
        
        return peptides;
    }
}
