package edu.sdsc.mmtf.proteomics.io;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Special reader for PTM S-Sulfenylation data in
 * Nat Commun. (2014), 5:4776. Site-specific mapping and 
 * quantification of protein S-sulphenylation in cells.
 * Yang J1, Gupta V2, Carroll KS2, Liebler DC1.
 * [DOI: 10.1038/ncomms5776](https://doi.org/10.1038/ncomms5776)
 * 
 * @author Peter Rose
 *
 */
public class Ncomm5776Reader {
    
    public static Dataset<Row> readPeptideFile(String fileName) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();
        
        String mod = "";
        if (fileName.contains("ncomms5776-s3_SulfenM.csv")) {
            mod = "(C,333)";
        } else if (fileName.contains("ncomms5776-s3_SulfenQ.csv")) {
            mod = "(C,339)";
        }
        
        Dataset<Row> peptides =  spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(fileName);
        
        // TODO standardize representation of PTMs
        
        return peptides;
    }
}
