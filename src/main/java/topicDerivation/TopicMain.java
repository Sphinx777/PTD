package topicDerivation;

import breeze.linalg.DenseVector;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import it.unimi.dsi.fastutil.ints.Int2DoubleLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.SizeEstimator;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import scala.Tuple2;
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
		CmdArgs cmdArgs = new CmdArgs();
		CmdLineParser parser = new CmdLineParser(cmdArgs);
		try {
			parser.parseArgument(args);
            System.out.println("model:"+cmdArgs.model);
            logger.info("model:"+cmdArgs.model);

            if(!cmdArgs.model.equals("")) {

                logger.info("start");

                StructType schemaTFIDF = new StructType(new StructField[]{
                        new StructField("tweetId", DataTypes.LongType, false, Metadata.empty()),
                        new StructField("tweet", DataTypes.StringType, false, Metadata.empty())
                });

                //for local build
//                System.setProperty("hadoop.home.dir", "D:\\JetBrains\\IntelliJ IDEA Community Edition 2016.2.4");

                sparkSession = SparkSession.builder()
                        //for local build
//                        .master("local")
                        .appName("TopicDerivation")
                        .config("spark.sql.warehouse.dir", "file:///")
                        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                        .config("spark.kryo.registrator",TweetInfoKryoRegister.class.getName())
                        //.config("spark.kryoserializer.buffer.max","2000m")
                        .getOrCreate();

                JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

//                double[][] dbArray = new double[2][2];
////
//                for(int x=0;x<2;x++){
//                    for(int y=0;y<2;y++){
//                        dbArray[x][y]=(x+y)*2;
//                    }
//                }
//                DenseVecMatrix dvm = new DenseVecMatrix(sc.sc(), dbArray, 2);
//

                final Broadcast<Date> broadcastCurrDate = sc.broadcast(new Date());

                StructType schema = new StructType().add("polarity", "string").add("oldTweetId", "double").add("date", "string")
                        .add("noUse", "string").add("userName", "string").add("tweet", "string")
                        .add("mentionMen", "string").add("userInteraction", "string");

                //read csv file to dataSet
                //Dataset<Row> csvDataset = sparkSession.read().schema(schema).csv(cmdArgs.inputFilePath);

                JavaRDD<String> stringJavaRDD = sc.textFile(cmdArgs.inputFilePath,6);
                //System.out.println("stringJavaRDD mem size:"+ SizeEstimator.estimate(stringJavaRDD));

                SimpleDateFormat sdf = new SimpleDateFormat(TopicConstant.OUTPUT_FILE_DATE_FORMAT);
                String outFilePath = cmdArgs.outputFilePath + "_" + sdf.format((Date) broadcastCurrDate.getValue());
                double maxCoherenceValue = -Double.MAX_VALUE;
                ObjectArrayList<String[]> topicWordList = new ObjectArrayList<>();

                //drop the no use column
               // Dataset<Row> currDataset = csvDataset.drop("polarity", "noUse");

                //add accumulator
                LongAccumulator tweetIDAccumulator = sc.sc().longAccumulator();

                logger.info("compute the mentionMen!");
                System.out.println("compute the mentionMen!");

                //the tweetInfo accumulator
                CollectionAccumulator<TweetInfo> tweetInfoAccumulator = sc.sc().collectionAccumulator();

                //set custom javaRDD and compute the mentionMen
                JavaRDD<TweetInfo> oldTweetJavaRDD = stringJavaRDD.map(new Function<String, TweetInfo>() {
                    @Override
                    public TweetInfo call(String v1) throws Exception {
                        String[] splitStrings = v1.replaceAll(TopicConstant.DOUBLE_QUOTE_DELIMITER,"").split(TopicConstant.COMMA_DELIMITER);
                        TweetInfo tweet = new TweetInfo();
                        //tweet.setTweetId(tweetIDAccumulator.value());
                        //logger.info("tweetIDAccumulator.value():"+tweetIDAccumulator.value());
                        //System.out.println("tweet id:"+tweetIDAccumulator.value());

                        tweetIDAccumulator.add(1);
                        tweet.setDateString(splitStrings[2]);
                        tweet.setUserName(splitStrings[4]);
                        tweet.setTweet(splitStrings[5].replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]",""));
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

                JavaPairRDD<TweetInfo,Long> tweetInfoZipPairRDD = oldTweetJavaRDD.zipWithIndex();

                JavaRDD<TweetInfo> tweetJavaRDD = tweetInfoZipPairRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<TweetInfo, Long>>, TweetInfo>() {
                    @Override
                    public Iterator<TweetInfo> call(Iterator<Tuple2<TweetInfo, Long>> tuple2Iterator) throws Exception {
                        ArrayList<TweetInfo> tweetInfoArrayList = new ArrayList<TweetInfo>();
                        while(tuple2Iterator.hasNext()){
                            Tuple2<TweetInfo,Long> tuple2 = tuple2Iterator.next();
                            TweetInfo tweetInfo = tuple2._1();
                            tweetInfo.setTweetId(tuple2._2().longValue());
                            tweetInfoArrayList.add(tweetInfo);
                        }

                        return tweetInfoArrayList.iterator();
                    }
                });

                logger.info("tweetJavaRDD compute finish!");

                System.out.println("tweetJavaRDD mem size:"+SizeEstimator.estimate(tweetJavaRDD));

                tweetJavaRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

                logger.info("tweetInfoAccumulator size:"+tweetInfoAccumulator.value().size());
                System.out.println("tweetInfoAccumulator mem size:"+SizeEstimator.estimate(tweetInfoAccumulator));

                Encoder<TweetInfo> encoder = Encoders.bean(TweetInfo.class);
                Dataset<TweetInfo> ds = sparkSession.createDataset(tweetJavaRDD.rdd(), encoder);

                tweetJavaRDD = TopicUtil.preProcessTweetRDD(ds);

                tweetJavaRDD.first();

                tweetJavaRDD.foreach(new VoidFunction<TweetInfo>() {
                    @Override
                    public void call(TweetInfo tweetInfo) throws Exception {
                        tweetInfoAccumulator.add(tweetInfo);
                    }
                });

                System.out.println("ds mem size:"+SizeEstimator.estimate(ds));
                ds.persist(StorageLevel.MEMORY_AND_DISK_SER());

                //transform to word vector
                if (cmdArgs.model.equals("vector")) {
                    logger.info("compute vector!");
                    System.out.println("compute vector!");
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
                    System.out.println("compute coherence start!");

                    //DTTD , intJNMF...etc
                    //ds.show();

                    //old
                    //List<Row> mentionDataset = ds.select("userName", "dateString", "tweet", "tweetId", "mentionMen", "userInteraction").collectAsList();

                    //compute mention string
                    //old
                    //JavaRDD<String> computePORDD = tweetJavaRDD.map(new JoinMentionString(mentionDataset, (Date) broadcastCurrDate.getValue() , cmdArgs.model));

                    logger.info("compute the interaction between tweets");
                    System.out.println("compute the interaction between tweets");

                    //old
                    //JavaPairRDD<Object,ArrayList<Object>> computePORDD = tweetJavaRDD.mapToPair(new JoinMentionString(mentionDataset,(Date) broadcastCurrDate.getValue() , cmdArgs.model));

                    //new
                    //JavaPairRDD<Object,ArrayList<Double>> computePORDD = tweetJavaRDD.mapToPair(new JoinMentionString(tweetInfoAccumulator.value(),(Date) broadcastCurrDate.getValue() , cmdArgs.model));

                    //JavaRDD<Tuple2<Object,DenseVector<Object>>> tuple2JavaRDD = tweetJavaRDD.map(new JoinMentionString(new ObjectArrayList<TweetInfo>(tweetInfoAccumulator.value()),(Date) broadcastCurrDate.getValue() , cmdArgs.model));

                    JavaRDD<Tuple2<Object,DenseVector<Object>>> tuple2JavaRDD = tweetJavaRDD.mapPartitions(new JoinMentionStringForPartition(new ObjectArrayList<TweetInfo>(tweetInfoAccumulator.value()),(Date) broadcastCurrDate.getValue() , cmdArgs.model));

                    //coordinate matrix version
//                    JavaRDD<Tuple2<Tuple2<Object,Object>,Object>> tweetTuple2RDD = computePORDD.flatMap(new FlatMapFunction<Tuple2<Object, ArrayList<Object>>, Tuple2<Tuple2<Object, Object>, Object>>() {
//                        @Override
//                        public Iterator<Tuple2<Tuple2<Object, Object>, Object>> call(Tuple2<Object, ArrayList<Object>> objectArrayListTuple2) throws Exception {
//
//                            ArrayList<Tuple2<Tuple2<Object,Object>,Object>> tuple2s = new ArrayList<Tuple2<Tuple2<Object, Object>, Object>>();
//                            for(int i=0;i<objectArrayListTuple2._2().size();i++){
//                                Tuple2<Object,Object> locTuple = new Tuple2<Object, Object>(objectArrayListTuple2._1(),Long.valueOf(Integer.valueOf(i)));
//                                tuple2s.add(new Tuple2<Tuple2<Object, Object>, Object>(locTuple,objectArrayListTuple2._2().toArray()[i]));
//                            }
//                            logger.info("compute flatmap tweet id:"+objectArrayListTuple2._1());
//                            System.out.println("compute flatmap tweet id:"+objectArrayListTuple2._1());
//                            return tuple2s.iterator();
//                        }
//                    });
//
//                    CoordinateMatrix coordinateMatrix = new CoordinateMatrix(tweetTuple2RDD.rdd());
                    //DenseVecMatrix tweetDVM = coordinateMatrix.toDenseVecMatrix();

                    //DenseVecMatrix version
//                    JavaRDD<Tuple2<Object,DenseVector<Object>>> tuple2JavaRDD = computePORDD.map(new Function<Tuple2<Object, ArrayList<Double>>, Tuple2<Object, DenseVector<Object>>>() {
//                        @Override
//                        public Tuple2<Object, DenseVector<Object>> call(Tuple2<Object, ArrayList<Double>> v1) throws Exception {
//                            //keep primitive array to Object
//                            double [] doubles = new double[v1._2().size()];
//                            for(int i=0;i<doubles.length;i++){
//                                doubles[i] = Double.valueOf((Double) v1._2().toArray()[i]).doubleValue();
//                            }
//                            List<double[]> list = Arrays.asList(doubles);
//                            DenseVector<Object> denseVector = new DenseVector<Object>(list.toArray()[0]);
//                            return new Tuple2<Object, DenseVector<Object>>(v1._1(),denseVector);
//                        }
//                    });

                    System.out.println("tuple2JavaRDD mem size:"+SizeEstimator.estimate(tuple2JavaRDD));
                    DenseVecMatrix tweetDVM = new DenseVecMatrix(tuple2JavaRDD.rdd());

                    System.out.println("tweetDVM mem size:"+SizeEstimator.estimate(tweetDVM));

                    logger.info("set mention entry!");
                    System.out.println("set mention entry!");

                    //put into coordinateMatrix
                    //JavaRDD<MatrixEntry> poRDD = computePORDD.flatMap(new GetMentionEntry());

                    //normal
                    //CoordinateMatrix poMatrix = new CoordinateMatrix(poRDD.rdd());

                    logger.info("compute interaction NMF!");
                    System.out.println("compute interaction NMF!");
                    //interaction
                    NMF interactionNMF = new NMF(tweetDVM, true, sc , cmdArgs.numFactors , cmdArgs.numIters , tweetIDAccumulator.value(),tweetIDAccumulator.value());

                    logger.info("build NMF model!");
                    System.out.println("build NMF model!");
                    interactionNMF.buildNMFModel();
                    DenseVecMatrix W = interactionNMF.getW();
                    //poRDD.unpersist();

                    Dataset<Row> tfidfDataset = ds.select("tweetId", "tweet");
                    System.out.println("tfidfDataset mem size:"+SizeEstimator.estimate(tfidfDataset));

                    Dataset<Row> sentenceData = sparkSession.createDataFrame(tfidfDataset.toJavaRDD(), schemaTFIDF);
                    System.out.println("sentenceData mem size:"+SizeEstimator.estimate(sentenceData));

                    //tfidf
                    //Broadcast<HashMap<String, String>> brTweetIDMap = sc.broadcast(new HashMap<String, String>());
                    //Broadcast<HashMap<String, String>> brTweetWordMap = sc.broadcast(new HashMap<String, String>());
                    //Broadcast<HashSet<String>> brHashSet = sc.broadcast(new HashSet<String>());
                    CollectionAccumulator<String> stringAccumulator = sc.sc().collectionAccumulator();

                    //tweetIDAccumulator.reset();

                    sentenceData.persist(StorageLevel.MEMORY_AND_DISK_SER());
                    TFIDF tfidf = new TFIDF(sentenceData,sc, stringAccumulator);

                    logger.info("build tfidf model!");
                    System.out.println("build tfidf model!");
                    tfidf.buildModel();
                    //old
                    //NMF tfidfNMF = new NMF(tfidf.getTfidfDVM(), false, sparkSession , cmdArgs.numFactors , cmdArgs.numIters);
                    NMF tfidfNMF = new NMF(tfidf.getTfidfDVM(), false, sc, cmdArgs.numFactors , cmdArgs.numIters,tweetIDAccumulator.value(), tfidf.getTweetIDMap().size());

                    //System.out.println(W.entries().count());
                    tfidfNMF.setW(W);

                    logger.info("build tfidf NMF model!");
                    System.out.println("build tfidf NMF model!");
                    tfidfNMF.buildNMFModel();
                    //CoordinateMatrix W1 = tfidfNMF.getOriginalW();
                    DenseVecMatrix H1 = tfidfNMF.getH();
                    //sentenceData.unpersist();

                    //System.out.println(H1.entries().count());

                    logger.info("order NMF score!");
                    System.out.println("order NMF score!");
                    JavaRDD<Int2DoubleLinkedOpenHashMap> cmpRDD = H1.rows().toJavaRDD().map(new Function<Tuple2<Object, DenseVector<Object>>, Int2DoubleLinkedOpenHashMap>() {
                        @Override
                        public Int2DoubleLinkedOpenHashMap call(Tuple2<Object, DenseVector<Object>> srcTuple) throws Exception {
                            ArrayList<Object> tmpList = new ArrayList<Object>();
                            Collections.addAll(tmpList,srcTuple._2().data());
                            double[] tmpDBArray = (double[])tmpList.get(0);
                            HashMap<Integer,Double> resultMap = new HashMap<Integer, Double>();
                            for(int i =0;i < tmpDBArray.length;i++){
                                if(tmpDBArray[i] > 0){
                                    resultMap.put(i, tmpDBArray[i]);
                                }
                            }

                            List<Map.Entry<Integer,Double>> list = new LinkedList<Map.Entry<Integer, Double>>(resultMap.entrySet());
                            Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
                                @Override
                                public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                                    return (o2.getValue()).compareTo(o1.getValue());
                                }
                            });

                            Int2DoubleLinkedOpenHashMap result = new Int2DoubleLinkedOpenHashMap();
                            for(Map.Entry<Integer,Double> entry:list){
                                //System.out.println("order NMF:"+entry.getKey()+"--"+entry.getValue());
                                //logger.info("order NMF:"+entry.getKey()+"--"+entry.getValue());
                                result.put(entry.getKey(), entry.getValue());
                            }
                            return result;
                        }
                    });

//                    JavaRDD<LinkedHashMap<Integer, Double>> cmpRDD = H1.toRowMatrix().rows().toJavaRDD().map(new Function<Vector, LinkedHashMap<Integer, Double>>() {
//                        @Override
//                        public LinkedHashMap<Integer, Double> call(Vector vector) throws Exception {
//                            double[] tmpDBArray = vector.toArray();
//                            HashMap<Integer, Double> resultMap = new HashMap<Integer, Double>();
//                            for (int i = 0; i < tmpDBArray.length; i++) {
//                                if (tmpDBArray[i] > 0) {
//                                    resultMap.put(i, tmpDBArray[i]);
//                                }
//                            }
//                            List<Map.Entry<Integer, Double>> list =
//                                    new LinkedList<Map.Entry<Integer, Double>>(resultMap.entrySet());
//                            Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
//                                @Override
//                                public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
//                                    return (o2.getValue()).compareTo(o1.getValue());
//                                }
//                            });
//
//                            LinkedHashMap<Integer, Double> result = new LinkedHashMap<Integer, Double>();
//                            for (Map.Entry<Integer, Double> entry : list) {
//                                System.out.println("order NMF:"+entry.getKey()+"--"+entry.getValue());
//                                logger.info("order NMF:"+entry.getKey()+"--"+entry.getValue());
//                                result.put(entry.getKey(), entry.getValue());
//                            }
//                            return result;
//                    }
//                    });
                    System.out.println("cmpRDD mem size:"+SizeEstimator.estimate(cmpRDD));
                    cmpRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

                    //System.out.println("cmpRdd count:"+cmpRDD.count());
                    //logger.info("cmpRdd count:"+cmpRDD.count());

                    //test
                    //logger.info("tfidf.getTweetIDMap.entrySet.size:"+tfidf.getTweetIDMap().entrySet().size());
//                    for(Map.Entry<String,String> entry:tfidf.getTweetIDMap().entrySet()){
//                        logger.info("tfidf.getTweetIDMap():"+entry.getKey()+","+entry.getValue());
//                    }
                    logger.info("transform to JSON!");
                    System.out.println("transform to JSON!");
                    CollectionAccumulator<String[]> topicWordAccumulator = sc.sc().collectionAccumulator();
                    JavaRDD<String> jsonRDD = cmpRDD.map(new WriteToJSON(new Object2ObjectOpenHashMap(tfidf.getTweetIDMap()),cmdArgs.numTopWords,topicWordAccumulator));

                    System.out.println("outFilePath:"+outFilePath);
                    logger.info("outFilePath:"+outFilePath);
                    System.out.println("jsonRDD mem size:"+SizeEstimator.estimate(jsonRDD));
                    jsonRDD.coalesce(1,true).saveAsTextFile(outFilePath);

                    logger.info("get top topic word list!");
                    System.out.println("get top topic word list!");

                    //map version
                    //JavaRDD<String[]> topicWordRDD = cmpRDD.map(new GetTopTopicWord(tfidf.getTweetIDMap(),cmdArgs.numTopWords,topicWordAccumulator));

                    //foreach version
                    //cmpRDD.foreach(new GetTopTopicWord(tfidf.getTweetIDMap(),cmdArgs.numTopWords,topicWordAccumulator));

                    topicWordList = new ObjectArrayList<String[]>(topicWordAccumulator.value());
                } else {
                    //model = "coherence"
                    logger.info("read topic word list!");
                    System.out.println("read topic word list!");
                    topicWordList = new ObjectArrayList<String[]>(TopicUtil.readTopicWordList(cmdArgs.coherenceFilePath));
                }

                //get the topic coherence value
                JavaRDD<String> tweetStrRDD = tweetJavaRDD.map(new Function<TweetInfo, String>() {
                    @Override
                    public String call(TweetInfo v1) throws Exception {
                        return v1.getTweet();
                    }
                });
                tweetStrRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
                DoubleAccumulator dbAccumulator = sparkSession.sparkContext().doubleAccumulator();

                double tmpCoherenceValue;
                Broadcast<Object2IntOpenHashMap<String>> brWordCntMap = sc.broadcast(new Object2IntOpenHashMap<String>());
                System.out.println("brWordCntMap mem size:"+SizeEstimator.estimate(brWordCntMap));

                logger.info("compute the topic coherence value!");
                System.out.println("compute the topic coherence value!");
                int topicWrdListSize = topicWordList.size();
                logger.info("the topicWordList length:"+topicWrdListSize);
                System.out.println("the topicWordList length:"+topicWrdListSize);

                for (String[] strings : topicWordList) {
                    logger.info("strings["+strings.length+"]:"+ String.join(TopicConstant.COMMA_DELIMITER,strings));
                    System.out.println("strings["+strings.length+"]:"+ String.join(TopicConstant.COMMA_DELIMITER,strings));

                    tmpCoherenceValue = MeasureUtil.getTopicCoherenceValue(strings, tweetStrRDD, brWordCntMap ,sparkSession , new ObjectArrayList<TweetInfo>(tweetInfoAccumulator.value()));
                    if (Double.compare(tmpCoherenceValue, maxCoherenceValue) > 0) {
                        maxCoherenceValue = tmpCoherenceValue;
                    }
                    dbAccumulator.add(tmpCoherenceValue);
                }
                System.out.println("Max Topic coherence value of top " + cmdArgs.numTopWords + " words:" + maxCoherenceValue);
                logger.info("Max Topic coherence value of top " + cmdArgs.numTopWords + " words:" + maxCoherenceValue);
                System.out.println("Average topic coherence value of top " + cmdArgs.numTopWords + " words:" + dbAccumulator.value() / topicWrdListSize);
                logger.info("Average topic coherence value of top " + cmdArgs.numTopWords + " words:" + dbAccumulator.value() / topicWrdListSize);

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
