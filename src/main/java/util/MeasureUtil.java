package util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import util.nmf.NMF;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by user on 2016/10/16.
 */
public class MeasureUtil {
    //public static HashMap<String,Integer> wordCntMap=new HashMap<String,Integer>();

    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MeasureUtil.class.getName());
    public static double getKLDivergence(BlockMatrix vBkMat, BlockMatrix wBkMat, BlockMatrix hBkMat, SparkSession sparkSession){
        final DoubleAccumulator dbAccumulator = sparkSession.sparkContext().doubleAccumulator();
        BlockMatrix whBkMat = wBkMat.multiply(hBkMat).persist(StorageLevel.MEMORY_ONLY_SER());

        CoordinateMatrix loginSide = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,vBkMat,whBkMat);
        CoordinateMatrix logResult = getMatrixLogValue(loginSide);
        BlockMatrix diffPara = whBkMat.subtract(vBkMat).persist(StorageLevel.MEMORY_ONLY_SER());
        CoordinateMatrix result = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply,vBkMat,logResult.toBlockMatrix()).toBlockMatrix().add(diffPara).toCoordinateMatrix();

//        result.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
//            @Override
//            public void call(Vector vector) throws Exception {
//                System.out.println(vector);
//            }
//        });

        result.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
            @Override
            public void call(MatrixEntry t) throws Exception{
                System.out.println(t.value());
                dbAccumulator.add(t.value());
            }
        });
        //System.out.println("rows:"+result.numRows()+",cols:"+result.numCols());
        return dbAccumulator.sum();
    }

    public static CoordinateMatrix getMatrixLogValue(CoordinateMatrix srcMat){
        JavaRDD<MatrixEntry> targetRDD = srcMat.entries().toJavaRDD().flatMap(new FlatMapFunction<MatrixEntry, MatrixEntry>() {
            @Override
            public Iterator<MatrixEntry> call(MatrixEntry matrixEntry) throws Exception {
                List<MatrixEntry> entryList = new ArrayList<MatrixEntry>();
                entryList.add(new MatrixEntry(matrixEntry.i(),matrixEntry.j(),Math.log(matrixEntry.value())));
                return entryList.iterator();
            }
        });

        CoordinateMatrix targetMat = new CoordinateMatrix(targetRDD.rdd());
        return targetMat;
    }

    public static double getTopicCoherenceValue(String[] topicWordArray , JavaRDD<String> tweetRDD , Broadcast<HashMap<String,Integer>> brWordCntMap , SparkSession sparkSession){
        //parameter : topic word array, RDD tweet
        //return  : dbSum

        double dbSum = 0.0;
        int WjCount, WiWjCount;
        boolean isWjCounted, isWiWjCounted;
        //HashMap<String,Integer> wordCntMap=brWordCntMap.getValue();
        //test
        HashMap<String,Integer> wordCntMap2= new HashMap<String,Integer>();
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
                logger.info("topicWordArray[j]:"+j+"--"+topicWordArray[j]);
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

                    //test
                    tweetRDD.foreach(new tweetRDD_ForeachFunc2(topicWordArray[i],topicWordArray[j],wjAccumulator,hashKeyAccumulator,isWjCounted,isWiWjCounted));
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
            }
        }
        return dbSum;
    }
}

class tweetRDD_ForeachFunc implements VoidFunction<String>{
    private int idx_i , idx_j;
    private boolean isWjCounted , isWiWjCounted;
    private String[] topicWordArray;
    private Broadcast<HashMap<String,Integer>> wrdCntMap;
    private String hashKey;

    public tweetRDD_ForeachFunc(String[] input_topicWordArray , Broadcast<HashMap<String,Integer>> input_Map , String input_hashKey , int inputIdx_i , int inputIdx_j , boolean input_isWjCounted , boolean input_isWiWjCounted){
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
    }
}