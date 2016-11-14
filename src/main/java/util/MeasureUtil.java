package util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.util.DoubleAccumulator;
import topicDerivation.TopicMain;

import java.util.*;

/**
 * Created by user on 2016/10/16.
 */
public class MeasureUtil {
    public static HashMap<String,Integer> wordCntMap=new HashMap<String,Integer>();

    public static double getKLDivergence(CoordinateMatrix V, CoordinateMatrix W,CoordinateMatrix H){
        final DoubleAccumulator dbAccumulator = TopicMain.sparkSession.sparkContext().doubleAccumulator();
        CoordinateMatrix WH = W.toBlockMatrix().multiply(H.toBlockMatrix()).toCoordinateMatrix();
        CoordinateMatrix loginSide = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,V,WH);
        CoordinateMatrix logResult = getMatrixLogValue(loginSide);
        CoordinateMatrix diffPara = WH.toBlockMatrix().subtract(V.toBlockMatrix()).toCoordinateMatrix();
        CoordinateMatrix result = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply,V,logResult).toBlockMatrix().add(diffPara.toBlockMatrix()).toCoordinateMatrix();

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

    public static double getTopicCoherenceValue(String[] topicWordArray , JavaRDD<String> tweetRDD){
        //parameter : topic word array, RDD tweet
        //return  : dbSum

        double dbSum = 0.0;
        int WjCount, WiWjCount;
        boolean isWjCounted, isWiWjCounted;

        //topic word array:total topic word , reference:the tfidf corpus
        //hashMap key rule:alphabetical
        for (int i = 1; i < topicWordArray.length; i++) {
            for (int j = 0; j <= i - 1; j++) {
                //prevent repeat compute the same word , Wi == Wj
                if (wordCntMap.containsKey(topicWordArray[j])) {
                    isWjCounted = true;
                } else {
                    isWjCounted = false;
                }

                //prevent repeat compute the same word
                String hashKey = topicWordArray[i].compareTo(topicWordArray[j]) >= 0 ? topicWordArray[j]+","+topicWordArray[i] : topicWordArray[i]+","+topicWordArray[j];
                if (wordCntMap.containsKey(hashKey)) {
                    isWiWjCounted = true;
                } else {
                    isWiWjCounted = false;
                }

                if (isWiWjCounted == false || isWjCounted == false) {
                    //iterate the all tweet(document)
                    tweetRDD.foreach(new tweetRDD_ForeachFunc(topicWordArray,wordCntMap,hashKey,i,j,isWjCounted,isWiWjCounted));
                }
                System.out.println("upper:"+((wordCntMap.get(hashKey)==null?0:wordCntMap.get(hashKey).intValue()) + 1));
                System.out.println("lower:"+(wordCntMap.get(topicWordArray[j])==null?0:wordCntMap.get(topicWordArray[j]).intValue()) );

                System.out.println("dbSum:"+dbSum);
                System.out.println("wordCntValue:"+Math.log((double) ((wordCntMap.get(hashKey)==null?0:wordCntMap.get(hashKey).intValue()) + 1.0) / (double) (wordCntMap.get(topicWordArray[j])==null?0:wordCntMap.get(topicWordArray[j]).intValue())));
                dbSum += Math.log((double) ((wordCntMap.get(hashKey)==null?0:wordCntMap.get(hashKey).intValue()) + 1.0) / (double) (wordCntMap.get(topicWordArray[j])==null?0:wordCntMap.get(topicWordArray[j]).intValue()));
            }
        }
        return dbSum;
    }
}

class tweetRDD_ForeachFunc implements VoidFunction<String>{
    private int idx_i , idx_j;
    private boolean isWjCounted , isWiWjCounted;
    private String[] topicWordArray;
    //private HashMap<String,Integer> wrdCntMap;
    private String hashKey;

    public tweetRDD_ForeachFunc(String[] input_topicWordArray , HashMap<String,Integer> input_Map , String input_hashKey , int inputIdx_i , int inputIdx_j , boolean input_isWjCounted , boolean input_isWiWjCounted){
        idx_i = inputIdx_i;
        idx_j = inputIdx_j;
        isWjCounted = input_isWjCounted;
        isWiWjCounted = input_isWiWjCounted;
        topicWordArray = input_topicWordArray;
//        wrdCntMap = input_Map;
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
            WjCount = MeasureUtil.wordCntMap.containsKey(topicWordArray[idx_j]) ? MeasureUtil.wordCntMap.get(topicWordArray[idx_j]) : 0;
            MeasureUtil.wordCntMap.put(topicWordArray[idx_j], WjCount + 1);
        }

        //count Wi,Wj
        if (!isWiWjCounted) {
            //Boolean wiFounded = Arrays.asList(tweet.split(" ")).stream().map(key -> key.toLowerCase()).collect(Collectors.toList()).contains(topicWordArray[idx_i]);
            Boolean wiFounded = Arrays.asList(tweet.split(" ")).stream().anyMatch(key -> key.toLowerCase().indexOf(topicWordArray[idx_i].toLowerCase())!=-1);
            if (wiFounded && wjFounded) {
                if (MeasureUtil.wordCntMap.containsKey(hashKey)) {
                    WiWjCount = MeasureUtil.wordCntMap.get(hashKey);
                } else {
                    WiWjCount = 0;
                }
                MeasureUtil.wordCntMap.put(hashKey, WiWjCount + 1);
            }
        }
    }
}
