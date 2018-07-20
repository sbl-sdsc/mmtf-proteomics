package edu.sdsc.mmtf.proteomics.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

public class PeptideMappingUtils {
    // N-terminal acetylation indicates chain break
    private static final String N_TERMINAL_ACETYLATION = "[42.011]";
    
    public static Dataset<Row> addPdbSequence(Dataset<Row> data) {
        SparkSession session = data.sparkSession();
        session.udf().register("pdbSeq", (Integer a, Integer b, String c) -> ((a == 0 ? "": c.charAt(a-1))+"."+c.substring(a,b+1)+"."+(b+1 >= c.length() ? "": c.charAt(b+1))), DataTypes.StringType);
        return data.withColumn("pdbSeq",callUDF("pdbSeq", col("start"), col("end"), col("sequence")) );
    }
    
    public static Dataset<Row> addPdbSecondaryStructure(Dataset<Row> data) {
        SparkSession session = data.sparkSession();
        session.udf().register("q3", (Integer a, Integer b, String c) -> c.substring(a,b+1), DataTypes.StringType);

        data.createOrReplaceTempView("table");
        data = session.sql("SELECT *, q3(start, end, dsspQ3Code) AS q3segment from table");
        return data;
    }
    
    public static Dataset<Row> addModPositions(Dataset<Row> data) {
        SparkSession session = data.sparkSession();
        session.udf().register("getModPositions", (String peptide, Integer start) -> getModPositions(peptide, start), DataTypes.createArrayType(DataTypes.IntegerType));
        return data.withColumn("modPositions",callUDF("getModPositions", col("modPeptide"), col("start")) );
    }
    
    /**
     * G(C,333)DVVVIPAGVPR -> [1]
     * 
     * K.(C,40)LHPLANETFVAK.D -> [0]
     * R.ILN(N,0.984)GHAFNVEFDDSQDK.A -> [3]
     * -.[42.011](M,15.995)DKNELVQK.A -> [0]
     * -.[42.011]MDKNELVQK.A -> [0]
     * -.SGTASVVCLL(N,39)NFYPR.E -> [10]
     * 
     * @param peptide
     * @param start
     * @return
     */
    private static Integer[] getModPositions(String peptide, int start) {
        if (peptide.charAt(1) == '.') {
            // TODO combine these two functions into one
            return getModPositionsWithFlanking(peptide, start);
        }
        List<Integer> positions = new ArrayList<>();
        
        // track position of modifications
        for (int i = 0; i < peptide.length(); i++) {
            char c = peptide.charAt(i);
            if (Character.isAlphabetic(c)) {
                start++;
            } else if (c == '(') {
                positions.add(start);
            }
        }
        return positions.toArray((new Integer[0]));          
    }

    /**
     * K.(C,40)LHPLANETFVAK.D -> [0]
     * R.ILN(N,0.984)GHAFNVEFDDSQDK.A -> [3]
     * -.[42.011](M,15.995)DKNELVQK.A -> [0]
     * -.[42.011]MDKNELVQK.A -> [0]
     * -.SGTASVVCLL(N,39)NFYPR.E -> [10]
     * 
     * @param peptide
     * @param start
     * @return
     */
    private static Integer[] getModPositionsWithFlanking(String peptide, int start) {
        List<Integer> positions = new ArrayList<>(1);
        char[] aa = peptide.toCharArray();

        if (aa[1] != '.' || aa[aa.length-2] != '.') System.out.println("Invalid peptide: " + peptide);
        
        // Mark N-terminal acetylation [42.011]
        if (peptide.substring(2,10).equals(N_TERMINAL_ACETYLATION)) {
                positions.add(start);
        }
        
        // track position of modifications
        int pos = start-1;
        for (int i = 2; i < aa.length-2; i++) {
            if (Character.isAlphabetic(aa[i])) pos++;
            if (aa[i] == '(') positions.add(pos+1);
        }
        return positions.toArray((new Integer[positions.size()]));          
    }
    
    public static Dataset<Row> intersectWithBindingSites(Dataset<Row> data) {
        SparkSession session = data.sparkSession();
        session.udf().register("matchBindingSites", (WrappedArray<Integer> a, WrappedArray<Integer> b) -> intersection(a, b), DataTypes.createArrayType(DataTypes.IntegerType));

        data.createOrReplaceTempView("table");
        data = session.sql("SELECT *, matchBindingSites(modPositions, fingerprint) AS matchSeqPositions from table");
        return data;
    }
    
    public static Integer[] intersection(WrappedArray<Integer> a, WrappedArray<Integer> b) {
        if (a == null || b == null || a.length() == 0 || b.length() == 0) return null;
                
        Set<Integer> aList = new HashSet<>(JavaConversions.seqAsJavaList(a));
        Set<Integer> bList = new HashSet<>(JavaConversions.seqAsJavaList(b));
        // intersection between the two sets
        bList.retainAll(aList);
        
        if (bList.isEmpty()) return null;
        
        return bList.toArray(new Integer[bList.size()]);
    }
    
    public static Dataset<Row> intersectGroupsWithBindingSites(Dataset<Row> data) {
        SparkSession session = data.sparkSession();
        session.udf().register("matchGroups", (WrappedArray<Integer> a, WrappedArray<Integer> b, WrappedArray<String> c) -> getGroupModPositions(a, b, c), DataTypes.createArrayType(DataTypes.StringType));

        data.createOrReplaceTempView("table");
        data = session.sql("SELECT *, matchGroups(modPositions, fingerprint, groupNumber) AS matchStrPositions from table");
        return data;
    }
    
    public static String[] getGroupModPositions(WrappedArray<Integer> mods, WrappedArray<Integer> fingerprint, WrappedArray<String> groupNumbers) {
        if (mods == null || fingerprint == null || groupNumbers == null
                || mods.length() == 0 || fingerprint.length() == 0 || groupNumbers.length() == 0) return null;
                
        List<Integer> modsList = JavaConversions.seqAsJavaList(mods);
        List<Integer> fingerprintList = JavaConversions.seqAsJavaList(fingerprint);
        List<String> groupNumberList = JavaConversions.seqAsJavaList(groupNumbers);
//        System.out.println("mods: " + modsList);
//        System.out.println("fprt: " + fingerprintList);
        // intersection between the two sets
        List<String> groupMods = new ArrayList<>(modsList.size());
        for (int i = 0; i < modsList.size(); i++) {
            int index = fingerprintList.indexOf(modsList.get(i));
            if (index >= 0) {
                groupMods.add(groupNumberList.get(index));
            }
        }

        return groupMods.toArray(new String[0]);
    }
    
    /**
     * Removes spaces from column names to ensure compatibility with parquet
     * files.
     *
     * @param original
     *            dataset
     * @return dataset with columns renamed
     */
    public static Dataset<Row> removeSpacesFromColumnNames(Dataset<Row> original) {

        for (String existingName : original.columns()) {
            String newName = existingName.replaceAll(" ", "");
            original = original.withColumnRenamed(existingName, newName);
        }

        return original;
    }
    
    public static Dataset<Row> flattenDataset(Dataset<Row> ds) {
        Dataset<Row> tmp = ds;
        for (StructField field : ds.schema().fields()) {
            if (field.dataType() instanceof ArrayType) {
                tmp = tmp.withColumn(field.name(), org.apache.spark.sql.functions.explode(tmp.col(field.name())));
                tmp.show();
            }
        }
        return tmp;
    }
}
