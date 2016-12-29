package topicDerivation;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import util.*;
import util.nmf.NMF;
import util.tfidf.TFIDF;
import vo.TweetInfo;

import java.text.SimpleDateFormat;
import java.util.*;

public class TopicMain {
	//10 dataset:
	//300 dataset:D:\Oracle\VirtualBox\sharefolder\testData.csv D:\MySyncFolder\Java\workspace\app\out\production\main\result
	//input argument:-iters 10 -factor 5 -top 10 -input D:\Oracle\VirtualBox\sharefolder\testData.csv -output D:\MySyncFolder\Java\workspace\app\out\production\main\result -model intjnmf
	public static SparkSession  sparkSession;
	static Logger logger = Logger.getLogger(TopicMain.class.getName());

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//parse the arguments
		TopicConstant cmdArgs = new TopicConstant();
		CmdLineParser parser = new CmdLineParser(cmdArgs);
		try {
			parser.parseArgument(args);
			System.out.println(TopicConstant.numIters);
		} catch (CmdLineException e) {
			// handling of wrong arguments
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			logger.info(e.getMessage());
		}

		//for local build
		System.setProperty("hadoop.home.dir", "D:\\JetBrains\\IntelliJ IDEA Community Edition 2016.2.4");
		logger.info("start");
		StructType schemaTFIDF = new StructType(new StructField[]{
				new StructField("tweetId", DataTypes.StringType, false, Metadata.empty()),
				new StructField("tweet", DataTypes.StringType, false, Metadata.empty())
		});
		sparkSession = SparkSession.builder()
									//for local build
									.master("local")
									.appName("TopicDerivation")
									.config("spark.sql.warehouse.dir","file:///")
									.getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

		final Broadcast<Date> broadcastCurrDate = sc.broadcast(new Date());

		StructType schema = new StructType().add("polarity","string").add("oldTweetId", "int").add("date","string")
									.add("noUse","string").add("userName","string").add("tweet","string")
									.add("mentionMen","string").add("userInteraction","string");

		Dataset<Row> csvDataset = sparkSession.read().schema(schema).csv(TopicConstant.inputFilePath);

		//test start
//		logger.info("iters:"+TopicConstant.numIters);
//		logger.info("factor:"+TopicConstant.numFactors);
//		logger.info("top:"+TopicConstant.numTopWords);
//		logger.info("input:"+TopicConstant.inputFilePath);
//		logger.info("output:"+TopicConstant.outputFilePath);
//		logger.info("model:"+TopicConstant.model);
//
//		if (!TopicConstant.model.trim().equals("")) {
//			logger.info("The model is not empty~");
//			return;
//		}
		//test end

		SimpleDateFormat sdf = new SimpleDateFormat(TopicConstant.OUTPUT_FILE_DATE_FORMAT);
		String outFilePath = TopicConstant.outputFilePath+"_"+sdf.format((Date) broadcastCurrDate.getValue());
		double maxCoherenceValue=-Double.MAX_VALUE;

		//drop the no use column
		Dataset<Row> currDataset = csvDataset.drop("polarity","noUse");
		//new tfidf(currDataset).buildModel();

		//no need , encode string to index
//		StringIndexer indexer = new StringIndexer()
//				.setInputCol("oldTweetId")
//				.setOutputCol("tweetId");
//		Dataset<Row> indexed = indexer.fit(currDataset).transform(currDataset);

		//add accumulator
		LongAccumulator tweetIDAccumulator = sc.sc().longAccumulator();

		//set custom javaRDD and compute the mentionMen
		JavaRDD<TweetInfo> tweetJavaRDD= currDataset.javaRDD().map(new Function<Row, TweetInfo>() {
			public TweetInfo call(Row row)throws Exception{
				TweetInfo tweet = new TweetInfo();
				tweetIDAccumulator.add(1);
				tweet.setTweetId(tweetIDAccumulator.value().toString());
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

		Encoder<TweetInfo> encoder = Encoders.bean(TweetInfo.class);
		Dataset<TweetInfo> ds = sparkSession.createDataset(tweetJavaRDD.rdd(), encoder);

		//transform to word vector
		if(TopicConstant.model.equals("vector")){
			String corpusFilePath = TopicConstant.outputFilePath+"_corpus_"+sdf.format((Date) broadcastCurrDate.getValue());
			String wordVectorFilePath = TopicConstant.outputFilePath+"_wordVector_"+sdf.format((Date) broadcastCurrDate.getValue());
			Dataset<Row> tweetDS = ds.select("tweetId","tweet");
			Dataset<Row> filterDS = sparkSession.createDataFrame(tweetDS.toJavaRDD(),schemaTFIDF);
			TopicUtil.transformToVector(filterDS,corpusFilePath,wordVectorFilePath);
			return;
		}

		List<String[]> topicWordList = new ArrayList<String[]>();
		if(!TopicConstant.model.equals("coherence")) {
			ds.show();

			List<Row> mentionDataset = ds.select("userName", "dateString", "tweet", "tweetId", "mentionMen", "userInteraction").collectAsList();
			//compute mention string 1: 1,1,1;1,2,2;
			//                       2: 2,1,1;2,2,2;
			JavaRDD<String> computePORDD = tweetJavaRDD.map(new JoinMentionString(mentionDataset, (Date) broadcastCurrDate.getValue()));
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
//		nmf interactionNMF = new nmf(interactionMatrix,true);
//		interactionNMF.buildNMFModel();

			//System.out.println(poMatrix.count());

			//interaction
			NMF interactionNMF = new NMF(poMatrix, true, sparkSession);
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

			//normal
//		List<Row> tfidfDataset = ds.select("tweetId","tweet").collectAsList();


			Dataset<Row> tfidfDataset = ds.select("tweetId", "tweet");
			Dataset<Row> sentenceData = sparkSession.createDataFrame(tfidfDataset.toJavaRDD(), schemaTFIDF);

			//tfidf
			Broadcast<HashMap<String, String>> brTweetIDMap = sc.broadcast(new HashMap<String, String>());
			Broadcast<HashMap<String, String>> brTweetWordMap = sc.broadcast(new HashMap<String, String>());
			Broadcast<HashSet<String>> brHashSet = sc.broadcast(new HashSet<String>());
			tweetIDAccumulator.reset();

			TFIDF tfidf = new TFIDF(sentenceData, brTweetIDMap, brTweetWordMap, brHashSet, sc.sc().longAccumulator());
			tfidf.buildModel();
			NMF tfidfNMF = new NMF(tfidf.getCoorMatOfTFIDF(), false, sparkSession);
			System.out.println(W.entries().count());
			tfidfNMF.setW(W);
			tfidfNMF.buildNMFModel();
			CoordinateMatrix W1 = tfidfNMF.getW();
			CoordinateMatrix H1 = tfidfNMF.getH();

			System.out.println(H1.entries().count());
//		H1.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
//			@Override
//			public void call(Vector vector) throws Exception {
//				System.out.println(vector);
//			}
//		});

			JavaRDD<LinkedHashMap<Integer, Double>> cmpRDD = H1.toRowMatrix().rows().toJavaRDD().map(new Function<Vector, LinkedHashMap<Integer, Double>>() {
				@Override
				public LinkedHashMap<Integer, Double> call(Vector vector) throws Exception {
					double[] tmpDBArray = vector.toArray();
					HashMap<Integer, Double> resultMap = new HashMap<Integer, Double>();
					for (int i = 0; i < tmpDBArray.length; i++) {
						if (tmpDBArray[i] > 0) {
							resultMap.put(i, tmpDBArray[i]);
						}
					}
					List<Map.Entry<Integer, Double>> list =
							new LinkedList<Map.Entry<Integer, Double>>(resultMap.entrySet());
					Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
						@Override
						public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
							return (o2.getValue()).compareTo(o1.getValue());
						}
					});

					LinkedHashMap<Integer, Double> result = new LinkedHashMap<Integer, Double>();
					for (Map.Entry<Integer, Double> entry : list) {
						result.put(entry.getKey(), entry.getValue());
					}
					return result;
				}
			});

			System.out.println(cmpRDD.count());
			JavaRDD<String> jsonRDD = cmpRDD.map(new WriteToJSON(tfidf.getTweetIDMap()));
			System.out.println(jsonRDD.count());
			jsonRDD.saveAsTextFile(outFilePath);
			topicWordList = cmpRDD.map(new GetTopTopicWord(tfidf.getTweetIDMap())).collect();
		}else{
			topicWordList = TopicUtil.readTopicWordList(TopicConstant.coherenceFilePath);
		}

		//get the topic coherence value
		JavaRDD<String> tweetStrRDD = tweetJavaRDD.map(new Function<TweetInfo, String>() {
			@Override
			public String call(TweetInfo v1) throws Exception {
				return v1.getTweet();
			}
		});
		DoubleAccumulator dbAccumulator = sparkSession.sparkContext().doubleAccumulator();

		double tmpCoherenceValue;
		Broadcast<HashMap<String,Integer>> brWordCntMap = sc.broadcast(new HashMap<String,Integer>());

		for(String[] strings:topicWordList){
			tmpCoherenceValue = MeasureUtil.getTopicCoherenceValue(strings,tweetStrRDD , brWordCntMap);
			if(Double.compare(tmpCoherenceValue,maxCoherenceValue)>0){
				maxCoherenceValue = tmpCoherenceValue;
			}
			dbAccumulator.add(tmpCoherenceValue);
		}
		System.out.println("Max Topic coherence value of top " + TopicConstant.numTopWords + " words:"+maxCoherenceValue);
		System.out.println("Average topic coherence value of top " + TopicConstant.numTopWords + " words:"+dbAccumulator.value()/(double)topicWordList.size());
		//Object vs = poMatrix.toRowMatrix().rows().take(1);
		//Encoders.STRING()
		//Dataset<Vector> dss = sparkSession.createDataset(rdd.rdd(), Encoders.bean(Double.class));
		//dss.show();
		
		// Apply a schema to an RDD of JavaBeans to get a DataFrame
		//Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
	}
}
