package edu.sdsc.mmtf.proteomics.utils;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.monotonicallyIncreasingId;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.proteomics.utils.SuffixArraySearch2.Match;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;
import scala.Tuple2;

public class SubstringJoin {
	private Dataset<Row> substringDataSet;
	private String substringColumn;
	private Dataset<Row> fullstringDataSet;
    private String fullstringColumn;
	
	public SubstringJoin(Dataset<Row> substringDataSet, String substringColumn, Dataset<Row> fullstringDataSet, String fullstringColumn) {
		this.substringDataSet = substringDataSet;
		this.substringColumn = substringColumn;
		this.fullstringDataSet = fullstringDataSet;
		this.fullstringColumn = fullstringColumn;
	}
	
	public Dataset<Row> join() {
		JavaRDD<Row> substringCol = substringDataSet.select(substringColumn).distinct().toJavaRDD();
	    
		Dataset<Row> tmp = fullstringDataSet.withColumn("tmp_id_col", monotonicallyIncreasingId()).cache();
		
		JavaPairRDD<Long, String> indexedData = tmp.select("tmp_id_col", fullstringColumn).toJavaRDD().mapToPair(r -> new Tuple2<Long,String>(r.getLong(0), r.getString(1)));
		Map<Long, String> mappedData = indexedData.collectAsMap();
		
	    SuffixArraySearch2<Long> suffixArray = new SuffixArraySearch2<>(mappedData, "$");
	    
	    @SuppressWarnings("resource") // sc will be closed elsewhere
		JavaSparkContext sc = new JavaSparkContext(SparkSession.builder().getOrCreate().sparkContext());
	    
	    // run parallel suffix array search
	    JavaRDD<Row> resultRows = substringCol.flatMap(new FlatMapper(sc.broadcast(suffixArray)));
	    Dataset<Row> mapping = JavaRDDToDataset.getDataset(resultRows, "tmp_id_col", "tmp_substring" ,"start","end");
	    tmp = mapping.join(broadcast(tmp), mapping.col("tmp_id_col").equalTo(tmp.col("tmp_id_col"))).drop("tmp_id_col");
	    
		if (substringDataSet.columns().length > 1) {
			tmp = tmp.join(broadcast(substringDataSet), tmp.col("tmp_substring").equalTo(substringDataSet.col(substringColumn))).drop("tmp_substring");
		} else {
			tmp = tmp.withColumnRenamed("tmp_substring", substringColumn);
		}
        return tmp;
	}
	
	static class FlatMapper implements FlatMapFunction<Row, Row> {
		private static final long serialVersionUID = 8723970557913154267L;
		private SuffixArraySearch2<Long> search;

		public FlatMapper(Broadcast<SuffixArraySearch2<Long>> broadcast) {
			this.search = broadcast.getValue();
		}

		@Override
		public Iterator<Row> call(Row t) throws Exception {
			List<Row> results = new ArrayList<>();
			String substring = t.getString(0);
			for (Match<Long> m : search.search(substring)) {
				results.add(RowFactory.create(m.getKey(), substring, m.getPosition(), m.getPosition()+substring.length()-1));
			}
			return results.iterator();
		}
	}
}
