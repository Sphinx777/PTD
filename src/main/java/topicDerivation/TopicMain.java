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

            if(!cmdArgs.model.equals("")) {
                //for local build
                //System.setProperty("hadoop.home.dir", "D:\\JetBrains\\IntelliJ IDEA Community Edition 2016.2.4");
                logger.info("start");

                StructType schemaTFIDF = new StructType(new StructField[]{
                        new StructField("tweetId", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("tweet", DataTypes.StringType, false, Metadata.empty())
                });

                sparkSession = SparkSession.builder()
                        //for local build
                        //.master("local")
                        .appName("TopicDerivation")
                        .config("spark.sql.warehouse.dir", "file:///")
                        .getOrCreate();

                JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

                final Broadcast<Date> broadcastCurrDate = sc.broadcast(new Date());

                StructType schema = new StructType().add("polarity", "string").add("oldTweetId", "int").add("date", "string")
                        .add("noUse", "string").add("userName", "string").add("tweet", "string")
                        .add("mentionMen", "string").add("userInteraction", "string");

                //read csv file to dataSet
                Dataset<Row> csvDataset = sparkSession.read().schema(schema).csv(cmdArgs.inputFilePath);

                SimpleDateFormat sdf = new SimpleDateFormat(TopicConstant.OUTPUT_FILE_DATE_FORMAT);
                String outFilePath = cmdArgs.outputFilePath + "_" + sdf.format((Date) broadcastCurrDate.getValue());
                double maxCoherenceValue = -Double.MAX_VALUE;
                List<String[]> topicWordList = new ArrayList<String[]>();

                //drop the no use column
                Dataset<Row> currDataset = csvDataset.drop("polarity", "noUse");

                //add accumulator
                LongAccumulator tweetIDAccumulator = sc.sc().longAccumulator();

                //set custom javaRDD and compute the mentionMen
                JavaRDD<TweetInfo> tweetJavaRDD = currDataset.javaRDD().map(new Function<Row, TweetInfo>() {
                    public TweetInfo call(Row row) throws Exception {
                        TweetInfo tweet = new TweetInfo();
                        tweetIDAccumulator.add(1);
                        tweet.setTweetId(tweetIDAccumulator.value().toString());
                        tweet.setDateString(row.getAs("date").toString());
                        tweet.setUserName(row.getAs("userName").toString());
                        tweet.setTweet(row.getAs("tweet").toString());
                        tweet.setMentionMen(setMentionMen(tweet.getUserName(), tweet.getTweet()));
                        tweet.setUserInteraction(setUserInteraction(tweet.getUserName(), tweet.getTweet()));
                        return tweet;
                    }

                    //for po:interaction based on people
                    private String setMentionMen(String userName, String tweet) {
                        ArrayList<String> arr = new ArrayList<String>();
                        arr.add(userName.trim());
                        String[] strings = tweet.split("\\s+");
                        for (String str : strings) {
                            if (str.indexOf("@") != -1) {
                                arr.add(str.trim().replace("@", ""));
                            }
                        }
                        return String.join(TopicConstant.COMMA_DELIMITER, arr);
                    }

                    //for act:interaction based on user actions
                    private String setUserInteraction(String userName, String tweetString) {
                        ArrayList<String> arr = new ArrayList<String>();
                        String[] strings = tweetString.split("\\s+");
                        String returnStr = userName;
                        //find begin with @xxx
                        //find begin with RT empty @XXX empty or :
                        if (strings.length > 0) {
                            if (strings[0].indexOf("@") != -1 && strings[0].length() > 1) {
                                returnStr += TopicConstant.COMMA_DELIMITER + strings[0].replace("@", "");
                            } else if (strings[0].equals("RT") && strings.length > 1) {
                                returnStr += TopicConstant.COMMA_DELIMITER + strings[1].replace(":", "").replace("@", "");
                            }
                        }

                        return returnStr;
                    }
                });

                Encoder<TweetInfo> encoder = Encoders.bean(TweetInfo.class);
                Dataset<TweetInfo> ds = sparkSession.createDataset(tweetJavaRDD.rdd(), encoder);

                //transform to word vector
                if (cmdArgs.model.equals("vector")) {
                    String corpusFilePath = cmdArgs.outputFilePath + "_corpus_" + sdf.format((Date) broadcastCurrDate.getValue());
                    String wordVectorFilePath = cmdArgs.outputFilePath + "_wordVector_" + sdf.format((Date) broadcastCurrDate.getValue());
                    Dataset<Row> tweetDS = ds.select("tweetId", "tweet");
                    Dataset<Row> filterDS = sparkSession.createDataFrame(tweetDS.toJavaRDD(), schemaTFIDF);
                    TopicUtil.transformToVector(filterDS, corpusFilePath, wordVectorFilePath);
                    return;
                }

                //compute topic word list
                if (!cmdArgs.model.equals("coherence")) {
                    //DTTD , intJNMF...etc
                    ds.show();

                    List<Row> mentionDataset = ds.select("userName", "dateString", "tweet", "tweetId", "mentionMen", "userInteraction").collectAsList();

                    //compute mention string
                    JavaRDD<String> computePORDD = tweetJavaRDD.map(new JoinMentionString(mentionDataset, (Date) broadcastCurrDate.getValue() , cmdArgs.model));
                    //put into coordinateMatrix
                    JavaRDD<MatrixEntry> poRDD = computePORDD.flatMap(new GetMentionEntry());

                    //normal
                    CoordinateMatrix poMatrix = new CoordinateMatrix(poRDD.rdd());

                    //interaction
                    NMF interactionNMF = new NMF(poMatrix, true, sparkSession , cmdArgs.numFactors , cmdArgs.numIters);
                    interactionNMF.buildNMFModel();
                    CoordinateMatrix W = interactionNMF.getW();

                    Dataset<Row> tfidfDataset = ds.select("tweetId", "tweet");
                    Dataset<Row> sentenceData = sparkSession.createDataFrame(tfidfDataset.toJavaRDD(), schemaTFIDF);

                    //tfidf
                    Broadcast<HashMap<String, String>> brTweetIDMap = sc.broadcast(new HashMap<String, String>());
                    Broadcast<HashMap<String, String>> brTweetWordMap = sc.broadcast(new HashMap<String, String>());
                    Broadcast<HashSet<String>> brHashSet = sc.broadcast(new HashSet<String>());
                    tweetIDAccumulator.reset();

                    TFIDF tfidf = new TFIDF(sentenceData, brTweetIDMap, brTweetWordMap, brHashSet, sc.sc().longAccumulator());
                    tfidf.buildModel();
                    NMF tfidfNMF = new NMF(tfidf.getCoorMatOfTFIDF(), false, sparkSession , cmdArgs.numFactors , cmdArgs.numIters);
                    System.out.println(W.entries().count());
                    tfidfNMF.setW(W);
                    tfidfNMF.buildNMFModel();
                    CoordinateMatrix W1 = tfidfNMF.getW();
                    CoordinateMatrix H1 = tfidfNMF.getH();

                    System.out.println(H1.entries().count());

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
                    JavaRDD<String> jsonRDD = cmpRDD.map(new WriteToJSON(tfidf.getTweetIDMap(),cmdArgs.numTopWords));
                    System.out.println(jsonRDD.count());
                    jsonRDD.saveAsTextFile(outFilePath);
                    topicWordList = cmpRDD.map(new GetTopTopicWord(tfidf.getTweetIDMap(),cmdArgs.numTopWords)).collect();
                } else {
                    //model = "coherence"
                    topicWordList = TopicUtil.readTopicWordList(cmdArgs.coherenceFilePath);
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
                Broadcast<HashMap<String, Integer>> brWordCntMap = sc.broadcast(new HashMap<String, Integer>());

                for (String[] strings : topicWordList) {
                    tmpCoherenceValue = MeasureUtil.getTopicCoherenceValue(strings, tweetStrRDD, brWordCntMap);
                    if (Double.compare(tmpCoherenceValue, maxCoherenceValue) > 0) {
                        maxCoherenceValue = tmpCoherenceValue;
                    }
                    dbAccumulator.add(tmpCoherenceValue);
                }
                System.out.println("Max Topic coherence value of top " + cmdArgs.numTopWords + " words:" + maxCoherenceValue);
                System.out.println("Average topic coherence value of top " + cmdArgs.numTopWords + " words:" + dbAccumulator.value() / (double) topicWordList.size());

                //Object vs = poMatrix.toRowMatrix().rows().take(1);
                //Encoders.STRING()
                //Dataset<Vector> dss = sparkSession.createDataset(rdd.rdd(), Encoders.bean(Double.class));
                //dss.show();

                // Apply a schema to an RDD of JavaBeans to get a DataFrame
                //Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
            }
            sparkSession.stop();
		} catch (CmdLineException cle) {
			// handling of wrong arguments
			System.out.println(cle.getMessage());
			parser.printUsage(System.out);
			logger.info(cle.getMessage());
		} catch (Exception ex){
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
	}
}
