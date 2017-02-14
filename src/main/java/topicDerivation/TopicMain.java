package topicDerivation;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import edu.nju.pasalab.marlin.matrix.BlockMatrix;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.util.StringUtils;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;
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
            System.out.println("model:"+cmdArgs.model);
            logger.info("model:"+cmdArgs.model);

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
                        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                        .config("spark.kryo.registrator",TweetInfoKryoRegister.class.getName())
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

                logger.info("compute the mentionMen!");
                //set custom javaRDD and compute the mentionMen
                JavaRDD<TweetInfo> tweetJavaRDD = currDataset.javaRDD().map(new Function<Row, TweetInfo>() {
                    public TweetInfo call(Row row) throws Exception {
                        TweetInfo tweet = new TweetInfo();
                        tweetIDAccumulator.add(1);
                        tweet.setTweetId(tweetIDAccumulator.value().toString());
                        logger.info("TweetId:"+tweet.getTweetId());
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

                tweetJavaRDD.persist(StorageLevel.MEMORY_ONLY_SER());

                Encoder<TweetInfo> encoder = Encoders.bean(TweetInfo.class);
                Dataset<TweetInfo> ds = sparkSession.createDataset(tweetJavaRDD.rdd(), encoder);
                ds.persist(StorageLevel.MEMORY_ONLY_SER());

                //transform to word vector
                if (cmdArgs.model.equals("vector")) {
                    logger.info("compute vector!");
                    String corpusFilePath = cmdArgs.outputFilePath + "_corpus_" + sdf.format((Date) broadcastCurrDate.getValue());
                    String wordVectorFilePath = cmdArgs.outputFilePath + "_wordVector_" + sdf.format((Date) broadcastCurrDate.getValue());
                    Dataset<Row> tweetDS = ds.select("tweetId", "tweet");
                    Dataset<Row> filterDS = sparkSession.createDataFrame(tweetDS.toJavaRDD(), schemaTFIDF);
                    TopicUtil.transformToVector(filterDS, corpusFilePath, wordVectorFilePath);
                    return;
                }

                //compute topic word list
                if (!cmdArgs.model.equals("coherence")) {
                    logger.info("compute coherence start!");

                    //DTTD , intJNMF...etc
                    //ds.show();

                    List<Row> mentionDataset = ds.select("userName", "dateString", "tweet", "tweetId", "mentionMen", "userInteraction").collectAsList();

                    //compute mention string
                    JavaRDD<String> computePORDD = tweetJavaRDD.map(new JoinMentionString(mentionDataset, (Date) broadcastCurrDate.getValue() , cmdArgs.model));

                    logger.info("set mention entry!");

                    //put into coordinateMatrix
                    JavaRDD<MatrixEntry> poRDD = computePORDD.flatMap(new GetMentionEntry());
                    poRDD.persist(StorageLevel.MEMORY_ONLY_SER());
                    logger.info("put coordinate matrix!");

                    //normal
                    CoordinateMatrix poMatrix = new CoordinateMatrix(poRDD.rdd());

                    logger.info("compute interaction NMF!");
                    //interaction
                    NMF interactionNMF = new NMF(poMatrix, true, sparkSession , cmdArgs.numFactors , cmdArgs.numIters);

                    logger.info("build NMF model!");
                    interactionNMF.buildNMFModel();
                    CoordinateMatrix W = interactionNMF.getW();
                    //poRDD.unpersist();

                    Dataset<Row> tfidfDataset = ds.select("tweetId", "tweet");
                    Dataset<Row> sentenceData = sparkSession.createDataFrame(tfidfDataset.toJavaRDD(), schemaTFIDF);

                    //tfidf
                    Broadcast<HashMap<String, String>> brTweetIDMap = sc.broadcast(new HashMap<String, String>());
                    Broadcast<HashMap<String, String>> brTweetWordMap = sc.broadcast(new HashMap<String, String>());
                    //Broadcast<HashSet<String>> brHashSet = sc.broadcast(new HashSet<String>());
                    CollectionAccumulator<String> stringAccumulator = sc.sc().collectionAccumulator();

                    tweetIDAccumulator.reset();

                    sentenceData.persist(StorageLevel.MEMORY_ONLY_SER());
                    TFIDF tfidf = new TFIDF(sentenceData,sc, stringAccumulator);

                    logger.info("build tfidf model!");
                    tfidf.buildModel();
                    NMF tfidfNMF = new NMF(tfidf.getCoorMatOfTFIDF(), false, sparkSession , cmdArgs.numFactors , cmdArgs.numIters);
                    //System.out.println(W.entries().count());
                    tfidfNMF.setW(W);

                    logger.info("build tfidf NMF model!");
                    tfidfNMF.buildNMFModel();
                    //CoordinateMatrix W1 = tfidfNMF.getW();
                    CoordinateMatrix H1 = tfidfNMF.getH();
                    //sentenceData.unpersist();

                    //System.out.println(H1.entries().count());

                    logger.info("order NMF score!");
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
                                System.out.println("order NMF:"+entry.getKey()+"--"+entry.getValue());
                                logger.info("order NMF:"+entry.getKey()+"--"+entry.getValue());
                                result.put(entry.getKey(), entry.getValue());
                            }
                            return result;
                    }
                    });

                    cmpRDD.persist(StorageLevel.MEMORY_ONLY_SER());

                    //System.out.println("cmpRdd count:"+cmpRDD.count());
                    //logger.info("cmpRdd count:"+cmpRDD.count());
                    logger.info("transform to JSON!");

                    //test
                    //logger.info("tfidf.getTweetIDMap.entrySet.size:"+tfidf.getTweetIDMap().entrySet().size());
//                    for(Map.Entry<String,String> entry:tfidf.getTweetIDMap().entrySet()){
//                        logger.info("tfidf.getTweetIDMap():"+entry.getKey()+","+entry.getValue());
//                    }

                    JavaRDD<String> jsonRDD = cmpRDD.map(new WriteToJSON(tfidf.getTweetIDMap(),cmdArgs.numTopWords));
                    //System.out.println("jsonRDD count:"+jsonRDD.count());
                    //logger.info("jsonRDD count:"+jsonRDD.count());
                    System.out.println("outFilePath:"+outFilePath);
                    logger.info("outFilePath:"+outFilePath);
                    //jsonRDD.collect();
                    jsonRDD.coalesce(1,true).saveAsTextFile(outFilePath);

                    logger.info("get top topic word list!");
                    System.out.println("get top topic word list!");
                    JavaRDD<String[]> topicWordRDD = cmpRDD.map(new GetTopTopicWord(tfidf.getTweetIDMap(),cmdArgs.numTopWords));
                    topicWordList = topicWordRDD.collect();
                } else {
                    //model = "coherence"
                    logger.info("read topic word list!");
                    topicWordList = TopicUtil.readTopicWordList(cmdArgs.coherenceFilePath);
                }

                //get the topic coherence value
                JavaRDD<String> tweetStrRDD = tweetJavaRDD.map(new Function<TweetInfo, String>() {
                    @Override
                    public String call(TweetInfo v1) throws Exception {
                        return v1.getTweet();
                    }
                });
                tweetStrRDD.persist(StorageLevel.MEMORY_ONLY_SER());
                DoubleAccumulator dbAccumulator = sparkSession.sparkContext().doubleAccumulator();

                double tmpCoherenceValue;
                Broadcast<HashMap<String, Integer>> brWordCntMap = sc.broadcast(new HashMap<String, Integer>());

                logger.info("compute the topic coherence value!");
                System.out.println("compute the topic coherence value!");
                logger.info("the topicWordList length:"+topicWordList.size());
                System.out.println("the topicWordList length:"+topicWordList.size());

                for (String[] strings : topicWordList) {
                    logger.info("strings["+strings.length+"]:"+ String.join(TopicConstant.COMMA_DELIMITER,strings));
                    System.out.println("strings["+strings.length+"]:"+ String.join(TopicConstant.COMMA_DELIMITER,strings));

                    tmpCoherenceValue = MeasureUtil.getTopicCoherenceValue(strings, tweetStrRDD, brWordCntMap ,sparkSession);
                    if (Double.compare(tmpCoherenceValue, maxCoherenceValue) > 0) {
                        maxCoherenceValue = tmpCoherenceValue;
                    }
                    dbAccumulator.add(tmpCoherenceValue);
                }
                System.out.println("Max Topic coherence value of top " + cmdArgs.numTopWords + " words:" + maxCoherenceValue);
                logger.info("Max Topic coherence value of top " + cmdArgs.numTopWords + " words:" + maxCoherenceValue);
                System.out.println("Average topic coherence value of top " + cmdArgs.numTopWords + " words:" + dbAccumulator.value() / (double) topicWordList.size());
                logger.info("Average topic coherence value of top " + cmdArgs.numTopWords + " words:" + dbAccumulator.value() / (double) topicWordList.size());

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

    public static class TweetInfoKryoRegister implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(TweetInfo.class, new FieldSerializer(kryo, TweetInfo.class));
        }
    }
}
