package Util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.util.DoubleAccumulator;
import parquet.org.apache.thrift.TProcessor;
import topicDerivation.TopicMain;

import java.util.*;

/**
 * Created by user on 2016/10/16.
 */
public class MeasureUtil {
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

    public static double getTopicCoherenceValue(ArrayList<String> topicWordArray , JavaRDD<String> tweetRDD){
        //parameter : topic word array, RDD tweet
        //return  : dbSum

        double dbSum = 0.0;
        int WjCount, WiWjCount;
        HashMap<String,Integer> hashMap=new HashMap<String,Integer>();
        boolean isWjCounted, isWiWjCounted;

        //topic word array:total topic word , reference:the tfidf corpus
        //hashMap key rule:alphabetical
        for (int i = 2; i <= topicWordArray.size(); i++) {
            for (int j = 1; j <= i - 1; j++) {
                //prevent repeat compute the same word , Wi == Wj
                if (hashMap.containsKey(topicWordArray.get(j))) {
                    isWjCounted = true;
                } else {
                    isWjCounted = false;
                }

                //prevent repeat compute the same word
                String hashKey = topicWordArray.get(i).compareTo(topicWordArray.get(j)) >= 0 ? topicWordArray.get(j)+","+topicWordArray.get(i) : topicWordArray.get(i)+","+topicWordArray.get(j);
                if (hashMap.containsKey(hashKey)) {
                    isWiWjCounted = true;
                } else {
                    isWiWjCounted = false;
                }

                if (isWiWjCounted == false || isWjCounted == false) {
                    //iterate the all tweet(document)
                    tweetRDD.foreach(new tweetRDD_ForeachFunc(topicWordArray,hashMap,hashKey,i,j,isWjCounted,isWiWjCounted));
                }
                dbSum += Math.log(hashMap.get(hashKey) + 1 / hashMap.get(topicWordArray.get(j)));
            }
        }
        return dbSum;
    }
}

class tweetRDD_ForeachFunc implements VoidFunction<String>{
    private int idx_i , idx_j;
    private boolean isWjCounted , isWiWjCounted;
    private ArrayList<String> topicWordArray;
    private HashMap<String,Integer> wrdCntMap;
    private String hashKey;

    public tweetRDD_ForeachFunc(ArrayList<String> input_topicWordArray , HashMap<String,Integer> input_Map , String input_hashKey , int inputIdx_i , int inputIdx_j , boolean input_isWjCounted , boolean input_isWiWjCounted){
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
            wjFounded = Arrays.asList(tweet.split(" ")).contains(topicWordArray.get(idx_j));
        }

        //count Wj
        if (!isWjCounted && wjFounded) {
            WjCount = wrdCntMap.containsKey(topicWordArray.get(idx_j)) ? wrdCntMap.get(topicWordArray.get(idx_j)) : 0;
            wrdCntMap.put(topicWordArray.get(idx_j), WjCount + 1);
        }

        //count Wi,Wj
        if (!isWiWjCounted) {
            Boolean wiFounded = Arrays.asList(tweet.split(" ")).contains(topicWordArray.get(idx_i));
            if (wiFounded && wjFounded) {
                if (wrdCntMap.containsKey(hashKey)) {
                    WiWjCount = wrdCntMap.get(hashKey);
                } else {
                    WiWjCount = 0;
                }
                wrdCntMap.put(hashKey, WiWjCount + 1);
            }
        }
    }
}
