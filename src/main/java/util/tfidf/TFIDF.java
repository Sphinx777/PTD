package util.tfidf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.feature.*;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import util.TopicConstant;

import java.io.Serializable;
import java.util.*;

public class TFIDF implements Serializable{
	private Dataset<Row> tweetDataset;
	private Dataset<Row> tfidfDataSet;
	private CoordinateMatrix CoorMatOfTFIDF;
	private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
	public Dataset<Row> getTfidfDataSet() {
		return tfidfDataSet;
	}
	private Broadcast<HashMap<String,String>> brTweetIDMap;
	private Broadcast<HashSet<String>> brHashSet;
	private LongAccumulator tweetIDAccumulator;

	public TFIDF(Dataset<Row> rdd, Broadcast<HashMap<String,String>> paraTweetIDMap , Broadcast<HashSet<String>> paraHashSet , LongAccumulator paraLongAccumulator){
		tweetDataset = rdd;
		brTweetIDMap = paraTweetIDMap;
		brHashSet = paraHashSet;
		tweetIDAccumulator = paraLongAccumulator;
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

		//get tokenHashSet
		filterData.select("words").toJavaRDD().foreach(new filterRowFunction(brHashSet));

		org.apache.spark.mllib.feature.HashingTF tf = new org.apache.spark.mllib.feature.HashingTF();
		//tfidf for ml start

//		JavaRDD<List<String>> listJavaRDD = filterData.select("words").toJavaRDD().map(new Function<Row, List<String>>() {
//			@Override
//			public List<String> call(Row v1) throws Exception {
//				List<String> stringList = v1.getList(0);
//				return stringList;
//			}
//		});
//
//		JavaRDD<org.apache.spark.mllib.linalg.Vector> termFreqs = tf.transform(listJavaRDD);
//		for(Object obj:brHashSet.getValue().toArray()){
//			System.out.println("index:"+tf.indexOf(obj.toString())+",word:"+obj.toString());
//			brTweetIDMap.getValue().put(String.valueOf(tf.indexOf(obj.toString())), obj.toString());
//		}
//		tweetIDMap = brTweetIDMap.getValue();
//
//		org.apache.spark.mllib.feature.IDF idfMllib = new org.apache.spark.mllib.feature.IDF();
//		JavaRDD<org.apache.spark.mllib.linalg.Vector> tfIdfs = idfMllib.fit(termFreqs).transform(termFreqs);
//		JavaRDD<MatrixEntry> entryJavaRDD = tfIdfs.flatMap(new FlatMapFunction<org.apache.spark.mllib.linalg.Vector, MatrixEntry>() {
//															   @Override
//															   public Iterator<MatrixEntry> call(org.apache.spark.mllib.linalg.Vector vector) throws Exception {
//																   List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//																   tweetIDAccumulator.add(1);
//																   for (int i : vector.toSparse().indices()) {
//																	   arrayList.add(new MatrixEntry(Double.valueOf(tweetIDAccumulator.value().toString()).longValue(), (long) i, vector.toArray()[i]));
//																   }
//																   return arrayList.iterator();
//															   }
//														   });
//		entryJavaRDD.collect();

		//tfidf for ml end

		//tfidf for mllib start

		HashingTF hashingTF = new HashingTF()
								  .setInputCol("words")
								  .setOutputCol("rawFeatures")
								  .setNumFeatures(TopicConstant.numFeatures);

		Dataset<Row> featurizedData = hashingTF.transform(filterData);

		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);

		rescaledData.select("tweetId","words","features","rawFeatures").toJavaRDD().foreach(new RowVoidFunction(brTweetIDMap));


		tfidfDataSet = rescaledData;

		JavaRDD<MatrixEntry> entryJavaRDD = tfidfDataSet.toJavaRDD().flatMap((FlatMapFunction<Row,MatrixEntry>) row ->{
			List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
			String id = row.getAs("tweetId");
			Vector feature = row.getAs("features");
			for(int i : feature.toSparse().indices()){
				arrayList.add(new MatrixEntry(Double.valueOf(id).longValue(),(long)i,feature.toArray()[i]));
			}
			return arrayList.iterator();
		});
		entryJavaRDD.count();

		//tfidf for mllib end

		//print the element of ml tfidf
//		for(Row row:rescaledData.select("tweetId","words","features","rawFeatures").collectAsList()){
//			Vector feature = row.getAs("features");
//			String id = row.getAs("tweetId");
//			//sparseVector
//			System.out.println("feature class:"+row.getAs("features").getClass().getName());
//			//wrappedArray
//			System.out.println("words class:"+row.getAs("words").getClass().getName());
//			System.out.println("id:"+id+",words:"+row.getAs("words")+",feature:"+feature);
//			LinkedList<Object> list = new LinkedList<Object>(row.getList(1));
//			for(int idx:feature.toSparse().indices()){
//				System.out.println("id:"+idx+" , value:"+feature.toArray()[idx]);
//				String wrd = list.poll().toString();
//				tweetIDMap.put(String.valueOf(idx),wrd);
//			}
//		}

		//print the tfidf value array
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

		//print the matrixEntry array
//		CoorMatOfTFIDF.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
//			@Override
//			public void call(MatrixEntry matrixEntry) throws Exception {
//				System.out.println(matrixEntry.i()+","+matrixEntry.j()+":"+matrixEntry.value());
//			}
//		});
		CoorMatOfTFIDF = new CoordinateMatrix(entryJavaRDD.rdd());
		System.out.println(CoorMatOfTFIDF.entries().count());
	}
	//x:tweet Id , y:term id(features--vector) , value:tfidf(features--vector)
	public CoordinateMatrix getCoorMatOfTFIDF(){
		return CoorMatOfTFIDF;
	}
	public HashMap<String, String> getTweetIDMap() {
		return tweetIDMap;
	}

	private class RowVoidFunction implements VoidFunction<Row> {
		private Broadcast<HashMap<String,String>> tweetIDMap;

		public RowVoidFunction(Broadcast<HashMap<String,String>> srcMap) {
			tweetIDMap = srcMap;
		}

		@Override
        public void call(Row row) throws Exception {
            String id = row.getAs("tweetId");
            Vector feature = row.getAs("features");
            LinkedList<Object> list = new LinkedList<Object>(row.getList(1));
            for(int idx:feature.toSparse().indices()){
                System.out.println("id:"+idx+" , value:"+feature.toArray()[idx]);
                String wrd = list.poll().toString();
                tweetIDMap.getValue().put(String.valueOf(idx),wrd);
            }
        }
	}

	private class filterRowFunction implements VoidFunction<Row> {
		private Broadcast<HashSet<String>> tokenHashSet;

		public filterRowFunction(Broadcast<HashSet<String>> paraBr){
			tokenHashSet = paraBr;
		}

		@Override
        public void call(Row row) throws Exception {
			List<Object> rowList = row.getList(0);
			for(Object obj:rowList){
				String wrd = obj.toString();
				tokenHashSet.getValue().add(wrd);
			}
		}
	}
}