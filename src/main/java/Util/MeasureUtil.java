package Util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.util.DoubleAccumulator;
import topicDerivation.TopicMain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
}
