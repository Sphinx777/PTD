package util;

import breeze.linalg.DenseVector;
import edu.nju.pasalab.marlin.matrix.BlockMatrix;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import vo.TweetInfo;

import java.util.*;

/**
 * Created by user on 2016/10/16.
 */
public class MeasureUtil {
    //public static HashMap<String,Integer> wordCntMap=new HashMap<String,Integer>();

    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MeasureUtil.class.getName());
    public static double getKLDivergence(DenseVecMatrix vDVM, DenseVecMatrix wDVM, DenseVecMatrix hDVM , DoubleAccumulator KLDSumAccumulator){
//        final DoubleAccumulator dbAccumulator = sparkSession.sparkContext().doubleAccumulator();
        double dbResult;
        DenseVecMatrix whDVM = ((BlockMatrix) wDVM.multiply(hDVM,CmdArgs.cores)).toDenseVecMatrix();
        //whDVM.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
        DenseVecMatrix loginSide = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,vDVM,whDVM);
        //loginSide.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
        DenseVecMatrix logResult = getMatrixLogValue(loginSide);
        //logResult.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
        DenseVecMatrix diffPara = whDVM.subtract(vDVM);
        //diffPara.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
        DenseVecMatrix result = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply,vDVM,logResult).add(diffPara);
        //result.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());

        //doubleRDD version
//        JavaDoubleRDD javaDoubleRDD = result.rows().toJavaRDD().flatMapToDouble(new DoubleFlatMapFunction<Tuple2<Object, DenseVector<Object>>>() {
//            @Override
//            public Iterator<Double> call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
//                ArrayList<Double> doubles = new ArrayList<Double>();
//                ArrayList<Object> objects = new ArrayList<Object>();
//                Collections.addAll(objects,objectDenseVectorTuple2._2().data());
//                double dbValue;
//                for (Object obj:(double[])objects.toArray()[0]){
//                    dbValue = (double)obj;
//                    if(!Double.isNaN(dbValue) && !Double.isInfinite(dbValue) && Double.isFinite(dbValue)){
//                        doubles.add(dbValue);
//                    }
//                }
//                return doubles.iterator();
//            }
//        });
//
//        dbResult = javaDoubleRDD.reduce(new Function2<Double, Double, Double>() {
//            @Override
//            public Double call(Double v1, Double v2) throws Exception {
//                return v1+v2;
//            }
//        });

        //accumulator version
        result.rows().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, DenseVector<Object>>>() {
            @Override
            public void call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
                ObjectArrayList<Object> objects = new ObjectArrayList<Object>();
                Collections.addAll(objects,objectDenseVectorTuple2._2().data());
                double dbValue;
                for (Object obj:(double[])objects.toArray()[0]){
                    dbValue = (double)obj;
                    if(!Double.isNaN(dbValue) && !Double.isInfinite(dbValue) && Double.isFinite(dbValue)){
                        KLDSumAccumulator.add(dbValue);
                    }
                }
            }
        });

        dbResult = KLDSumAccumulator.sum();
        return dbResult;
    }

    public static DenseVecMatrix getMatrixLogValue(DenseVecMatrix srcMat){
//        JavaRDD<MatrixEntry> targetRDD = srcMat.entries().toJavaRDD().flatMap(new FlatMapFunction<MatrixEntry, MatrixEntry>() {
//            @Override
//            public Iterator<MatrixEntry> call(MatrixEntry matrixEntry) throws Exception {
//                List<MatrixEntry> entryList = new ArrayList<MatrixEntry>();
//                entryList.add(new MatrixEntry(matrixEntry.i(),matrixEntry.j(),Math.log(matrixEntry.value())));
//                return entryList.iterator();
//            }
//        });

//        JavaPairRDD<Object,DenseVector<Object>> targetRDD = srcMat.rows().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object,DenseVector<Object>>, Object, DenseVector<Object>>() {
//            @Override
//            public Tuple2<Object, DenseVector<Object>> call(Tuple2<Object,DenseVector<Object>> srcTuple2) throws Exception{
//                Double[] dbArray = (Double[]) srcTuple2._2().data();
//                for (Double dbValue:dbArray) {
//                    dbValue = Math.log(dbValue);
//                }
//                return new Tuple2<Object, DenseVector<Object>>(srcTuple2._1(),new DenseVector(dbArray));
//            }
//        });

        //coordinateMatrix version
//        JavaRDD<Tuple2<Tuple2<Object,Object>,Object>> targetRDD2 = srcMat.rows().toJavaRDD().flatMap(new FlatMapFunction<Tuple2<Object, DenseVector<Object>>, Tuple2<Tuple2<Object, Object>, Object>>() {
//            @Override
//            public Iterator<Tuple2<Tuple2<Object, Object>, Object>> call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
//                ArrayList<Tuple2<Tuple2<Object,Object>,Object>> tuple2s = new ArrayList<Tuple2<Tuple2<Object, Object>, Object>>();
//                Tuple2<Object,Object> locTuple;
//                for (int i=0;i<objectDenseVectorTuple2._2().size();i++){
//                    ArrayList<Object> objects = new ArrayList<Object>();
//                    Collections.addAll(objects,objectDenseVectorTuple2._2().data());
//                    locTuple = new Tuple2<Object, Object>(objectDenseVectorTuple2._1(),Integer.valueOf(i).longValue());
//                    tuple2s.add(new Tuple2<Tuple2<Object, Object>,Object>(locTuple,Double.valueOf(Math.log(((double[])objects.get(0))[i])).floatValue()));
//                }
//                return tuple2s.iterator();
//            }
//        });

        //denseVecMatrix
        JavaRDD<Tuple2<Object,DenseVector<Object>>> targetRDD2 = srcMat.rows().toJavaRDD().map(new Function<Tuple2<Object,DenseVector<Object>>, Tuple2<Object,DenseVector<Object>>>() {
            @Override
            public Tuple2<Object,DenseVector<Object>> call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
                double [] doubles = new double[objectDenseVectorTuple2._2().size()];
                ObjectArrayList<Double> arrayList = new ObjectArrayList<Double>();

                for (int i=0;i<objectDenseVectorTuple2._2().size();i++){
                    ObjectArrayList<Object> objects = new ObjectArrayList<Object>();
                    Collections.addAll(objects,objectDenseVectorTuple2._2().data());
                    arrayList.add(Double.valueOf(Math.log(((double[])objects.get(0))[i])).doubleValue());
                }
                for(int i=0;i<doubles.length;i++){
                    doubles[i] = Double.valueOf((Double) arrayList.toArray()[i]).doubleValue();
                }
                List<double[]> list = Arrays.asList(doubles);
                DenseVector<Object> denseVector = new DenseVector<Object>(list.toArray()[0]);
                return new Tuple2<Object, DenseVector<Object>>(objectDenseVectorTuple2._1(),denseVector);
            }
        });

        //coordinateMatrix version
        //CoordinateMatrix coordinateMatrix = new CoordinateMatrix(targetRDD2.rdd());
        DenseVecMatrix denseVecMatrix = new DenseVecMatrix(targetRDD2.rdd());
        return denseVecMatrix;
    }

    public static double getTopicCoherenceValue(String[] topicWordArray , JavaRDD<String> tweetRDD , Broadcast<Object2IntOpenHashMap<String>> brWordCntMap , SparkSession sparkSession, ObjectArrayList<TweetInfo> tweetInfoList){
        //parameter : topic word array, RDD tweet
        //return  : dbSum

        double dbSum = 0.0;
        boolean isWjCounted, isWiWjCounted;
        //HashMap<String,Integer> wordCntMap=brWordCntMap.getValue();
        //test
        Object2IntOpenHashMap<String> wordCntMap2= new Object2IntOpenHashMap<String>();
        LongAccumulator wjAccumulator = sparkSession.sparkContext().longAccumulator();
        LongAccumulator hashKeyAccumulator = sparkSession.sparkContext().longAccumulator();

        //topic word array:total topic word , reference:the tfidf corpus
        //hashMap key rule:alphabetical
        for (int i = 1; i < topicWordArray.length; i++) {
            for (int j = 0; j <= i - 1; j++) {
                //prevent repeat compute the same word , Wi == Wj
                if (wordCntMap2.containsKey(topicWordArray[j])) {
                    isWjCounted = true;
                } else {
                    isWjCounted = false;
                }

                //prevent repeat compute the same word

                logger.info("topicWordArray[i]:"+i+"--"+topicWordArray[i]);
                System.out.println("topicWordArray[i]:"+i+"--"+topicWordArray[i]);
                logger.info("topicWordArray[j]:"+j+"--"+topicWordArray[j]);
                System.out.println("topicWordArray[j]:"+j+"--"+topicWordArray[j]);
                String hashKey = topicWordArray[i].compareTo(topicWordArray[j]) >= 0 ? topicWordArray[j]+","+topicWordArray[i] : topicWordArray[i]+","+topicWordArray[j];
                if (wordCntMap2.containsKey(hashKey)) {
                    isWiWjCounted = true;
                } else {
                    isWiWjCounted = false;
                }

                if (isWiWjCounted == false || isWjCounted == false) {
                    //iterate the all tweet(document)
                    //tweetRDD.foreach(new tweetRDD_ForeachFunc(topicWordArray,brWordCntMap,hashKey,i,j,isWjCounted,isWiWjCounted));

                    wjAccumulator.reset();
                    if(wordCntMap2.containsKey(topicWordArray[j])) {
                        wjAccumulator.add(wordCntMap2.get(topicWordArray[j]));
                    }

                    hashKeyAccumulator.reset();
                    if(wordCntMap2.containsKey(hashKey)) {
                        hashKeyAccumulator.add(wordCntMap2.get(hashKey));
                    }

                    //tweetRDD foreach version
//                    tweetRDD.foreach(new tweetRDD_ForeachFunc2(topicWordArray[i],topicWordArray[j],wjAccumulator,hashKeyAccumulator,isWjCounted,isWiWjCounted));
//                    if(wjAccumulator.value() > 0) {
//                        wordCntMap2.put(topicWordArray[j], wjAccumulator.value().intValue());
//                    }

                    //tweetInfo list version
                    Iterator<TweetInfo> tweetInfoIterator = tweetInfoList.iterator();
                    while (tweetInfoIterator.hasNext()){
                        TweetInfo tmpTweetInfo = tweetInfoIterator.next();
                        foundSameWordFunc(tmpTweetInfo.getTweet(),topicWordArray[i],topicWordArray[j],wjAccumulator,hashKeyAccumulator,isWjCounted,isWiWjCounted);
                    }
                    if(wjAccumulator.value() > 0) {
                        wordCntMap2.put(topicWordArray[j], wjAccumulator.value().intValue());
                    }

                    if(hashKeyAccumulator.value() > 0) {
                        wordCntMap2.put(hashKey, hashKeyAccumulator.value().intValue());
                    }
                }
                System.out.println("upper:"+((wordCntMap2.get(hashKey)==null?0:wordCntMap2.get(hashKey).intValue()) + 1));
                System.out.println("lower:"+(wordCntMap2.get(topicWordArray[j])==null?0:wordCntMap2.get(topicWordArray[j]).intValue()) );

                System.out.println("dbSum:"+dbSum);
                System.out.println("wordCntValue:"+Math.log((double) ((wordCntMap2.get(hashKey)==null?0:wordCntMap2.get(hashKey).intValue()) + 1.0) / (double) (wordCntMap2.get(topicWordArray[j])==null?0:wordCntMap2.get(topicWordArray[j]).intValue())));
                System.out.println("log value:"+Math.log((double) ((wordCntMap2.get(hashKey)==null?0:wordCntMap2.get(hashKey).intValue()) + 1.0) / (double) (wordCntMap2.get(topicWordArray[j])==null?0:wordCntMap2.get(topicWordArray[j]).intValue())));
                dbSum += Math.log((double) ((wordCntMap2.get(hashKey)==null?0:wordCntMap2.get(hashKey).intValue()) + 1.0) / (double) (wordCntMap2.get(topicWordArray[j])==null?0:wordCntMap2.get(topicWordArray[j]).intValue()));
                logger.info("dbSum:"+dbSum);
                System.out.println("dbSum:"+dbSum);
            }
        }
        return dbSum;
    }

    public static void foundSameWordFunc(String tweet, String wordI , String wordJ , LongAccumulator wjAccumulator , LongAccumulator hashKeyAccumulator , boolean isWjCounted , boolean isWiWjCounted){
        int WjCount,WiWjCount;
        boolean wjFounded = false;
        if (!isWjCounted || !isWiWjCounted) {
            //wjFounded = Arrays.asList(tweet.split(" ")).stream().map(key -> key.toLowerCase()).collect(Collectors.toList()).contains(topicWordArray[idx_j]);
            wjFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(wordJ.toLowerCase())!=-1);
        }

        //count Wj
        if (!isWjCounted && wjFounded) {
            wjAccumulator.add(1);
        }

        //count Wi,Wj
        if (!isWiWjCounted) {
            Boolean wiFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(wordI.toLowerCase())!=-1);
            if (wiFounded && wjFounded) {
                hashKeyAccumulator.add(1);
            }
        }
        //logger.info("wjFound:"+wjFounded);
        //System.out.println("wjFound:"+wjFounded);
    }
}

class tweetRDD_ForeachFunc implements VoidFunction<String>{
    private int idx_i , idx_j;
    private boolean isWjCounted , isWiWjCounted;
    private String[] topicWordArray;
    private Broadcast<Object2IntOpenHashMap<String>> wrdCntMap;
    private String hashKey;

    public tweetRDD_ForeachFunc(String[] input_topicWordArray , Broadcast<Object2IntOpenHashMap<String>> input_Map , String input_hashKey , int inputIdx_i , int inputIdx_j , boolean input_isWjCounted , boolean input_isWiWjCounted){
        idx_i = inputIdx_i;
        idx_j = inputIdx_j;
        isWjCounted = input_isWjCounted;
        isWiWjCounted = input_isWiWjCounted;
        topicWordArray = input_topicWordArray;
        wrdCntMap = input_Map;
        hashKey = input_hashKey;
    }

    public void call(String tweet){
        int WjCount,WiWjCount;
        boolean wjFounded = false;
        if (!isWjCounted || !isWiWjCounted) {
            //wjFounded = Arrays.asList(tweet.split(" ")).stream().map(key -> key.toLowerCase()).collect(Collectors.toList()).contains(topicWordArray[idx_j]);
            wjFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(topicWordArray[idx_j].toLowerCase())!=-1);
        }

        //count Wj
        if (!isWjCounted && wjFounded) {
            WjCount = wrdCntMap.getValue().containsKey(topicWordArray[idx_j]) ? wrdCntMap.getValue().get(topicWordArray[idx_j]) : 0;
            wrdCntMap.getValue().put(topicWordArray[idx_j], WjCount + 1);
        }

        //count Wi,Wj
        if (!isWiWjCounted) {
            //Boolean wiFounded = Arrays.asList(tweet.split(" ")).stream().map(key -> key.toLowerCase()).collect(Collectors.toList()).contains(topicWordArray[idx_i]);
            Boolean wiFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(topicWordArray[idx_i].toLowerCase())!=-1);
            if (wiFounded && wjFounded) {
                if (wrdCntMap.getValue().containsKey(hashKey)) {
                    WiWjCount = wrdCntMap.getValue().get(hashKey);
                } else {
                    WiWjCount = 0;
                }
                wrdCntMap.getValue().put(hashKey, WiWjCount + 1);
            }
        }
    }
}

class tweetRDD_ForeachFunc2 implements VoidFunction<String>{
    private String wordI , wordJ;
    private boolean isWjCounted , isWiWjCounted;
    private LongAccumulator wjAccumulator , hashKeyAccumulator;
    static Logger logger = Logger.getLogger(tweetRDD_ForeachFunc2.class.getName());

    public tweetRDD_ForeachFunc2(String inputWordI , String inputWordJ , LongAccumulator input_wjAccumulator , LongAccumulator input_hashKeyAccumulator , boolean input_isWjCounted , boolean input_isWiWjCounted){
        wordI = inputWordI;
        wordJ = inputWordJ;
        wjAccumulator = input_wjAccumulator;
        hashKeyAccumulator = input_hashKeyAccumulator;
        isWjCounted = input_isWjCounted;
        isWiWjCounted = input_isWiWjCounted;
    }

    public void call(String tweet){
        int WjCount,WiWjCount;
        boolean wjFounded = false;
        if (!isWjCounted || !isWiWjCounted) {
            //wjFounded = Arrays.asList(tweet.split(" ")).stream().map(key -> key.toLowerCase()).collect(Collectors.toList()).contains(topicWordArray[idx_j]);
            wjFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(wordJ.toLowerCase())!=-1);
        }

        //count Wj
        if (!isWjCounted && wjFounded) {
            wjAccumulator.add(1);
        }

        //count Wi,Wj
        if (!isWiWjCounted) {
            Boolean wiFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(wordI.toLowerCase())!=-1);
            if (wiFounded && wjFounded) {
                 hashKeyAccumulator.add(1);
            }
        }
        //logger.info("wjFound:"+wjFounded);
        //System.out.println("wjFound:"+wjFounded);
    }
}