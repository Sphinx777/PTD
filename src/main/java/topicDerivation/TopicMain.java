package topicDerivation;

import Util.*;
import Util.NMF.NMF;
import Util.TFIDF.TFIDF;
import VO.tweetInfo;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.filter.MeasurementModel;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import scala.util.parsing.combinator.testing.Str;

import java.io.StringReader;
import java.util.*;

public class TopicMain {
	public static SparkSession  sparkSession;
	public static Date currentTime = new Date();
	static Logger logger = Logger.getLogger(TopicMain.class.getName());

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "D:\\JetBrains\\IntelliJ IDEA Community Edition 2016.2.4");
		logger.info("start");
		sparkSession = SparkSession.builder().master("local")
									.appName("TopicDerivation")
									.config("spark.sql.warehouse.dir","file:///")
									.getOrCreate();
		//System.out.println("current array max length: "+(Integer.MAX_VALUE - 8));

		StructType schema = new StructType().add("polarity","string").add("oldTweetId", "int").add("date","string")
				                            .add("noUse","string").add("userName","string").add("tweet","string")
				                            .add("mentionMen","string").add("userInteraction","string");
		
		Dataset<Row> csvDataset = sparkSession.read().schema(schema).csv(args[0]);

		String outFilePath = args[1];

		//drop the no use column
		Dataset<Row> currDataset = csvDataset.drop("polarity","noUse");
		//new TFIDF(currDataset).buildModel();

		//encode string to index
		StringIndexer indexer = new StringIndexer()
				.setInputCol("oldTweetId")
				.setOutputCol("tweetId");
		Dataset<Row> indexed = indexer.fit(currDataset).transform(currDataset);

		//set custom javaRDD and compute the mentionMen
		JavaRDD<tweetInfo> tweetJavaRDD= indexed.javaRDD().map(new Function<Row, tweetInfo>() {
			public tweetInfo call(Row row)throws Exception{
				tweetInfo tweet = new tweetInfo();
				tweet.setTweetId(row.getAs("tweetId").toString());
				tweet.setDateString(row.getAs("date").toString());
				tweet.setUserName(row.getAs("userName").toString());
				tweet.setTweet(row.getAs("tweet").toString());
				tweet.setMentionMen(setMentionMen(tweet.getUserName(),tweet.getTweet()));
				tweet.setUserInteraction(setUserInteraction(tweet.getUserName(),tweet.getTweet()));
				return tweet;
			}
			
			//for po:interaction based on people
			private String setMentionMen(String userName,String tweet){
				ArrayList<String> arr = new ArrayList<String>();
				arr.add(userName.trim());
				String[] strings = tweet.split("\\s+");
				for(String str:strings){
					if(str.indexOf("@")!=-1){
						arr.add(str.trim().replace("@", ""));
					}
				}
				return String.join(TopicConstant.COMMA_DELIMITER, arr);
			}
			
			//for act:interaction based on user actions
			private String setUserInteraction(String userName,String tweetString){
				ArrayList<String> arr = new ArrayList<String>();
				String[] strings = tweetString.split("\\s+");
				String returnStr = userName;
				//find begin with @xxx
				//find begin with RT empty @XXX empty or :
				if(strings.length>0){
					if(strings[0].indexOf("@")!=-1 && strings[0].length()>1){
						returnStr += TopicConstant.COMMA_DELIMITER + strings[0].replace("@", ""); 
					}else if(strings[0].equals("RT") && strings.length>1){
						returnStr += TopicConstant.COMMA_DELIMITER + strings[1].replace(":", "").replace("@", "");
					}
				}
				
				return returnStr;
			}
		});
		
		Encoder<tweetInfo> encoder = Encoders.bean(tweetInfo.class);
		Dataset<tweetInfo> ds = sparkSession.createDataset(tweetJavaRDD.rdd(), encoder);
		ds.show();

		List<Row> mentionDataset = ds.select("userName","dateString","tweet","tweetId","mentionMen","userInteraction").collectAsList();
		//compute mention string 1: 1,1,1;1,2,2;
		//                       2: 2,1,1;2,2,2;
		JavaRDD<String> computePORDD = tweetJavaRDD.map(new JoinMentionString(mentionDataset));
		//put into coordinateMatrix
		JavaRDD<MatrixEntry> poRDD = computePORDD.flatMap(new GetMentionEntry());

		//normal
		CoordinateMatrix poMatrix = new CoordinateMatrix(poRDD.rdd());

		//test
//		List<Double> matrixEntryList = new ArrayList<Double>();
//		matrixEntryList.add(5.0);
//		JavaRDD<Double> tmpMat = sparkSession.createDataset(matrixEntryList,Encoders.DOUBLE()).toJavaRDD();
//		JavaRDD<MatrixEntry> poMatrixTest = tmpMat.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
//			@Override
//			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
//				List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//				//2*2
//				arrayList.add(new MatrixEntry(0,0,1));
//				arrayList.add(new MatrixEntry(0,1,2));
//				arrayList.add(new MatrixEntry(1,0,3));
//				arrayList.add(new MatrixEntry(1,1,4));
//
//				// 3*3
////				arrayList.add(new MatrixEntry(0,0,1));
////				arrayList.add(new MatrixEntry(0,1,2));
////				arrayList.add(new MatrixEntry(0,2,3));
////				arrayList.add(new MatrixEntry(1,0,4));
////				arrayList.add(new MatrixEntry(1,1,5));
////				arrayList.add(new MatrixEntry(1,2,6));
////				arrayList.add(new MatrixEntry(2,0,7));
////				arrayList.add(new MatrixEntry(2,1,8));
////				arrayList.add(new MatrixEntry(2,2,9));
//				return arrayList.iterator();
//			}
//		});
//		CoordinateMatrix interactionMatrix = new CoordinateMatrix(poMatrixTest.rdd());
//		NMF interactionNMF = new NMF(interactionMatrix,true);
//		interactionNMF.buildNMFModel();

		//System.out.println(poMatrix.count());

		//interaction
		NMF interactionNMF = new NMF(poMatrix,true);
		interactionNMF.buildNMFModel();
		CoordinateMatrix W = interactionNMF.getW();

		//test
//		List<Row> data = Arrays.asList(
//				RowFactory.create(1, "Hi I heard about Spark"),
//				RowFactory.create(2, "I wish Java could use case classes"),
//				RowFactory.create(3, "Logistic regression models are neat")
//		);



//		StructType schemaTFIDF = new StructType(new StructField[]{
//				new StructField("tweetId", DataTypes.IntegerType, false, Metadata.empty()),
//				new StructField("tweet", DataTypes.StringType, false, Metadata.empty())
//		});


//		Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schemaTFIDF);

		List<Row> tfidfDataset = ds.select("tweetId","tweet").collectAsList();
		StructType schemaTFIDF = new StructType(new StructField[]{
				new StructField("tweetId", DataTypes.StringType, false, Metadata.empty()),
				new StructField("tweet", DataTypes.StringType, false, Metadata.empty())
		});
		Dataset<Row> sentenceData = sparkSession.createDataFrame(tfidfDataset,schemaTFIDF);

		//TFIDF
		TFIDF tfidf = new TFIDF(sentenceData);
		tfidf.buildModel();
		NMF tfidfNMF = new NMF(tfidf.getCoorMatOfTFIDF(),false);
		System.out.println(W.entries().count());
		tfidfNMF.setW(W);
		tfidfNMF.buildNMFModel();
		CoordinateMatrix W1 = tfidfNMF.getW();
		CoordinateMatrix H1 = tfidfNMF.getH();

		System.out.println(H1.entries().count());
		H1.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
			@Override
			public void call(Vector vector) throws Exception {
				System.out.println(vector);
			}
		});

		JavaRDD<LinkedHashMap<Integer,Double>> cmpRDD = H1.toRowMatrix().rows().toJavaRDD().map(new Function<Vector, LinkedHashMap<Integer, Double>>() {
			@Override
			public LinkedHashMap<Integer, Double> call(Vector vector) throws Exception {
				double[] tmpDBArray = vector.toArray();
				HashMap<Integer,Double> resultMap= new HashMap<Integer, Double>();
				for(int i=0;i< tmpDBArray.length;i++){
					if(tmpDBArray[i]>0){
						resultMap.put(i,tmpDBArray[i]);
					}
				}
				List<Map.Entry<Integer, Double>> list =
						new LinkedList<Map.Entry<Integer, Double>>( resultMap.entrySet() );
				Collections.sort( list, new Comparator<Map.Entry<Integer, Double>>()
				{
					@Override
					public int compare( Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2 )
					{
						return ( o2.getValue() ).compareTo( o1.getValue() );
					}
				} );

				LinkedHashMap<Integer, Double> result = new LinkedHashMap<Integer, Double>();
				for (Map.Entry<Integer, Double> entry : list)
				{
					result.put( entry.getKey(), entry.getValue() );
				}
				return result;
			}
		});

		System.out.println(cmpRDD.count());
		JavaRDD<String> jsonRDD = cmpRDD.map(new WriteToJSON(tfidf.getTweetIDMap()));
		System.out.println(jsonRDD.count());
		jsonRDD.saveAsTextFile(outFilePath);

		//get the topic coherence value
		List<String[]> topicWordList = cmpRDD.map(new GetTopTopicWord(tfidf.getTweetIDMap())).collect();
		JavaRDD<String> tweetStrRDD = tweetJavaRDD.map(new Function<tweetInfo, String>() {
			@Override
			public String call(tweetInfo v1) throws Exception {
				return v1.getTweet();
			}
		});
		DoubleAccumulator dbAccumulator = TopicMain.sparkSession.sparkContext().doubleAccumulator();

		for(String[] strings:topicWordList){
			dbAccumulator.add(MeasureUtil.getTopicCoherenceValue(strings,tweetStrRDD));
		}
		System.out.println("Topic coherence value:"+dbAccumulator.value() / (double)topicWordList.size());
		//Object vs = poMatrix.toRowMatrix().rows().take(1);
		//Encoders.STRING()
		//Dataset<Vector> dss = sparkSession.createDataset(rdd.rdd(), Encoders.bean(Double.class));
		//dss.show();
		
		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		//Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
	}
	
	public static class ParseLine implements Function<String,String[]>{
	    public	String[] call(String line)throws Exception{
			CSVReader reader = new CSVReader(new StringReader(line));
			return reader.readNext();
		}
	}

}
