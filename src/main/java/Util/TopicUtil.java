package Util;

import jdk.nashorn.internal.runtime.regexp.joni.constants.EncloseType;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.execution.columnar.STRING;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import topicDerivation.TopicMain;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Collectors;


public class TopicUtil {
	private static Random r = new Random(System.currentTimeMillis());
	static final double dbMinForRandom = 0.0;
	public static int computeArrayIntersection(String[] arr1,String[] arr2){
		Set<String> aList = new LinkedHashSet<String>(Arrays.asList(arr1));
		Set<String> bList = new LinkedHashSet<String>(Arrays.asList(arr2));
		ArrayList<String> list =new ArrayList<String>();
		
		for(String str:bList){
			if(!aList.add(str)){
				list.add(str);
			}
		}
		
		if (list.size()>0) {
			//System.out.println();
		}
		return list.size();
	}
	
	public static int computeArrayUnion(String[] arr1,String[] arr2){
		HashSet<String> tmpSet = new HashSet<String>();
		tmpSet.addAll(Arrays.asList(arr1));
		tmpSet.addAll(Arrays.asList(arr2));
		
		return tmpSet.size();
	}
	
	public static double calculateSigmoid(double x){
	    return (1/( 1 + Math.pow(Math.E,(-1*x))));
	}

	public static BigDecimal decimalInRange(double dbValue) {
		if(Double.isInfinite(dbValue)){
			return TopicConstant.DOUBLE_MAX;
		}else if(Double.isNaN(dbValue)){
			return TopicConstant.DOUBLE_MIN;
		}else{
			return new BigDecimal(String.valueOf(dbValue));
		}
	}

	public static CoordinateMatrix getInverseCoordinateMatrix(CoordinateMatrix srcMatrix){
		SingularValueDecomposition<RowMatrix,Matrix> svdComs = srcMatrix.toRowMatrix().computeSVD((int)srcMatrix.numCols(),true,1e-9);
		final long numRows = srcMatrix.numRows();
		final long numCols = srcMatrix.numCols();
		final LongAccumulator rowAccumulator = TopicMain.sparkSession.sparkContext().longAccumulator();
		final LongAccumulator colAccumulator = TopicMain.sparkSession.sparkContext().longAccumulator();

		//U
		rowAccumulator.reset();
		colAccumulator.reset();
		JavaRDD<MatrixEntry> UMatRDD = svdComs.U().rows().toJavaRDD().flatMap(new FlatMapFunction<Vector, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Vector vector) throws Exception {
				List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
				for (double db:vector.toArray()) {
					System.out.println(db);
					arrayList.add(new MatrixEntry(rowAccumulator.value(),colAccumulator.value(),db));
					colAccumulator.add(1);
					if(colAccumulator.value() >= numCols){
						colAccumulator.reset();
						rowAccumulator.add(1);
					}

					if(rowAccumulator.value() >= numRows){
						rowAccumulator.reset();
					}
				}
				return arrayList.iterator();
			}
		});
		//UMatRDD.count();
		CoordinateMatrix UtranMat = new CoordinateMatrix(UMatRDD.rdd()).transpose();

		//S
		double[] dbSvds = svdComs.s().toArray();

		for (int i=0;i<dbSvds.length;i++){
			//System.out.println("before db:"+dbSvds[i]);
			dbSvds[i] = Math.pow(dbSvds[i],-1);
			//System.out.println("after db:"+dbSvds[i]);
		}
		colAccumulator.reset();
		JavaRDD<MatrixEntry> SMatRDD = TopicMain.sparkSession.createDataset(Arrays.asList(ArrayUtils.toObject(dbSvds)), Encoders.DOUBLE()).toJavaRDD().flatMap(new FlatMapFunction<Double, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
				List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
				arrayList.add(new MatrixEntry(colAccumulator.value(),colAccumulator.value(),aDouble.doubleValue()));
				colAccumulator.add(1);
				return arrayList.iterator();
			}
		});
		//System.out.println(sMatRDD.count());
		CoordinateMatrix sInvMat = new CoordinateMatrix(SMatRDD.rdd());

		//V
//		for (int i=0;i<svdComs.V().numRows();i++){
//			for (int j=0;j<svdComs.V().numCols();j++){
//				System.out.println("("+i+","+j+"):"+svdComs.V().apply(i,j));
//			}
//		}
		rowAccumulator.reset();
		colAccumulator.reset();
		JavaRDD<MatrixEntry> VMatRDD = TopicMain.sparkSession.createDataset(Arrays.asList(ArrayUtils.toObject(svdComs.V().toArray())), Encoders.DOUBLE()).toJavaRDD().flatMap(new FlatMapFunction<Double, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
				List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
				arrayList.add(new MatrixEntry(rowAccumulator.value(),colAccumulator.value(),aDouble.doubleValue()));
				rowAccumulator.add(1);
				if (rowAccumulator.value() >= numRows){
					rowAccumulator.reset();
					colAccumulator.add(1);
				}

				if(colAccumulator.value() >= numCols){
					colAccumulator.reset();
				}
				return arrayList.iterator();
			}
		});
		//VMatRDD.count();
		CoordinateMatrix vMat = new CoordinateMatrix(VMatRDD.rdd());

		System.out.println(vMat.numRows()+","+vMat.numCols());
		System.out.println(sInvMat.numRows()+","+sInvMat.numCols());
		System.out.println(UtranMat.numRows()+","+UtranMat.numCols());
		return vMat.toBlockMatrix().multiply(sInvMat.toBlockMatrix()).multiply(UtranMat.toBlockMatrix()).toCoordinateMatrix();
	}

	public static CoordinateMatrix getCoorMatOption(TopicConstant.MatrixOperation srcOpt, CoordinateMatrix matDividend , CoordinateMatrix matDivisor){
		final TopicConstant.MatrixOperation operation = srcOpt;
		JavaPairRDD<String,Double> tmpDividendRDD = matDividend.entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, String, Double>(){
			public Tuple2<String, Double> call(MatrixEntry entry)throws Exception {
				return new Tuple2<String, Double>(entry.i()+TopicConstant.COMMA_DELIMITER+entry.j(),entry.value());
			}
		});

		JavaPairRDD<String,Double> tmpDivisorRDD = matDivisor.entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, String, Double>(){
			public Tuple2<String, Double> call(MatrixEntry entry)throws Exception {
				return new Tuple2<String, Double>(entry.i()+TopicConstant.COMMA_DELIMITER+entry.j(),entry.value());
			}
		});

		JavaPairRDD<String,Tuple2<Double,Optional<Double>>> tmpRDD = tmpDividendRDD.leftOuterJoin(tmpDivisorRDD);

		JavaRDD<MatrixEntry> resultRDD = tmpRDD.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<Double, Optional<Double>>>, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Tuple2<String, Tuple2<Double, Optional<Double>>> stringTuple2Tuple2) throws Exception {
				List<MatrixEntry> entryList = new ArrayList<MatrixEntry>();
				String[] loc = stringTuple2Tuple2._1().split(TopicConstant.COMMA_DELIMITER);
				if(stringTuple2Tuple2._2()._2().isPresent()){
					double dividend = stringTuple2Tuple2._2()._1();
					double divisor = stringTuple2Tuple2._2()._2().get();
					double value=0.0;
					if(operation.equals(TopicConstant.MatrixOperation.Divide)) {
						value = dividend / divisor;
					}else if(operation.equals(TopicConstant.MatrixOperation.Mutiply)){
						value = dividend * divisor;
					}
					entryList.add(new MatrixEntry(Long.parseLong(loc[0]),Long.parseLong(loc[1]),value));
				}
				return entryList.iterator();
			}
		});

		CoordinateMatrix resultMat = new CoordinateMatrix(resultRDD.rdd());
		//System.out.println(resultMat.entries().count());
		//System.out.println(resultMat.numRows());
		//System.out.println(resultMat.numCols());
		resultMat.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
			@Override
			public void call(MatrixEntry matrixEntry) throws Exception {
				System.out.println(matrixEntry.i()+","+matrixEntry.j()+":"+matrixEntry.value());
			}
		});
		return resultMat;
	}

	public static double getRandomValue(double max) {
		return dbMinForRandom + (max - dbMinForRandom) * r.nextDouble();
	}
}
