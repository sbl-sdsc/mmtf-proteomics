package edu.sdsc.mmtf.proteomics.io;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PtmFileConverter {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName(PtmFileConverter.class.getSimpleName())
                .getOrCreate();
        
        String fileName = args[0];
        Dataset<Row> ds = null;
        
        if (fileName.contains("ncomms5776")) {
            ds = Ncomm5776Reader.readPeptideFile(fileName);
        } else if (fileName.contains("MAESTRO")) {
            ds = MaestroReader.readPeptideFile(fileName);
        }
        ds.show(false);
        
        spark.stop();
    }
}
