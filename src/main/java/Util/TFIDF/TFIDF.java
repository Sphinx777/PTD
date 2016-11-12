package Util.TFIDF;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.execution.columnar.COMPACT_DECIMAL;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.sql.execution.streaming.state.KeyRemoved;
import scala.collection.mutable.WrappedArray;
import topicDerivation.TopicMain;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

public class TFIDF implements Serializable{
	private int numFeatures = 100;
	private Dataset<Row> tweetDataset;
	private Dataset<Row> tfidfDataSet;
	private CoordinateMatrix CoorMatOfTFIDF;

	private HashMap<String,String> tweetIDMap = new HashMap<String,String>();

	public Dataset<Row> getTfidfDataSet() {
		return tfidfDataSet;
	}

	public TFIDF(Dataset<Row> rdd){
		tweetDataset = rdd;
	}
	
	//tweet:original message
	//words:the tokened tweet
	//rawFeatures:the token id and the token times
	//features: the tfidf value
	public void buildModel(){
		Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("token");
		RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("tweet").setOutputCol("token").setPattern("\\W").setMinTokenLength(3);
		//tokenizer version
		//Dataset<Row> wordsData = tokenizer.transform(tweetDataset);
		//regexTokenizer version
		Dataset<Row> wordsData = regexTokenizer.transform(tweetDataset);

		StopWordsRemover remover = new StopWordsRemover().setInputCol("token").setOutputCol("words");
		Dataset<Row> filterData = remover.transform(wordsData);

//		JavaRDD<Row> filterRDD = filterData.toJavaRDD().map(new Function<Row, Row>() {
//			@Override
//			public Row call(Row row) throws Exception {
//				ArrayList<String> resultList = new ArrayList<String>();
//				List<String> arrayList = row.getList(3);
//				for(String str:arrayList){
//					if(str.length()>=3){
//						resultList.add(str);
//					}
//				}
//				return RowFactory.create(resultList);
//			}
//		});

		HashingTF hashingTF = new HashingTF()
								  .setInputCol("words")
								  .setOutputCol("rawFeatures")
								  .setNumFeatures(numFeatures);
								  //.setNumFeatures(tweetDataset.collectAsList().size());
		
		Dataset<Row> featurizedData = hashingTF.transform(filterData);

		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);

		for(Row row:rescaledData.select("tweetId","words","features","rawFeatures").collectAsList()){
			Vector feature = row.getAs("features");
			String id = row.getAs("tweetId");
			//sparseVector
			System.out.println("feature class:"+row.getAs("features").getClass().getName());
			//wrappedArray
			System.out.println("words class:"+row.getAs("words").getClass().getName());
			System.out.println("id:"+id+",words:"+row.getAs("words")+",feature:"+feature);
			LinkedList<Object> list = new LinkedList<Object>(row.getList(1));
			for(int idx:feature.toSparse().indices()){
				System.out.println("id:"+idx+" , value:"+feature.toArray()[idx]);
				String wrd = list.poll().toString();
				tweetIDMap.put(String.valueOf(idx),wrd);
			}
		}

		tfidfDataSet = rescaledData;

		JavaRDD<MatrixEntry> entryJavaRDD = tfidfDataSet.toJavaRDD().flatMap((FlatMapFunction<Row,MatrixEntry>) row ->{
			List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
			String id = row.getAs("tweetId");
			Vector feature = row.getAs("features");
			for(int i : feature.toSparse().indices()){
				//System.out.println("i:"+i+":"+feature.toArray()[i]);
				arrayList.add(new MatrixEntry(Double.valueOf(id).longValue(),(long)i,feature.toArray()[i]));
			}
			return arrayList.iterator();
		});
		entryJavaRDD.count();
//		JavaRDD<MatrixEntry> entryJavaRDD = tfidfDataSet.toJavaRDD().flatMap(new FlatMapFunction<Row, MatrixEntry>() {
//			@Override
//			public Iterator<MatrixEntry> call(Row row) throws Exception {
//				List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//				Vector feature = row.getAs("features");
//				for(int i=0;i<feature.toArray().length;i++){
//					System.out.println("i:"+i+":"+feature.toArray()[i]);
//				}
//				return arrayList.iterator();
//			}
//		});

		CoorMatOfTFIDF = new CoordinateMatrix(entryJavaRDD.rdd());
		CoorMatOfTFIDF.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
			@Override
			public void call(MatrixEntry matrixEntry) throws Exception {
				System.out.println(matrixEntry.i()+","+matrixEntry.j()+":"+matrixEntry.value());
			}
		});
		System.out.println(CoorMatOfTFIDF.entries().count());
	}
	//x:tweet Id , y:term id(features--vector) , value:tfidf(features--vector)
	public CoordinateMatrix getCoorMatOfTFIDF(){
		return CoorMatOfTFIDF;
	}
	public HashMap<String, String> getTweetIDMap() {
		return tweetIDMap;
	}
}
