package util.nmf;

import org.apache.spark.sql.SparkSession;
import util.MeasureUtil;
import util.TopicConstant;
import util.TopicUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import topicDerivation.TopicMain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NMF implements Serializable {
	private int numIters;
	private double initialNum = 0.01;
	private long numRows , numCols;
	//private DenseMatrix W,H,minW,minH;
	private CoordinateMatrix V , W , H , minW , minH;
	//static double tmpKLDivergence = 0.0;
    static double minKLDivergence = Double.MAX_VALUE;
	private boolean boolUpdateW;
	private SparkSession sparkSession;

	public NMF(CoordinateMatrix matV,boolean isUpdateW,SparkSession paraSparkSession,int paraIters,int paraFactors) {
		V = matV;
		boolUpdateW = isUpdateW;
		minKLDivergence = Double.MAX_VALUE;
		numIters = paraIters;
		int numFactors = paraFactors;
		//initialize to zero or other method
		//double[] dbArray =new double[(int)(V.numRows() * numFactors)];
        //Arrays.fill(dbArray,0.01);
		this.sparkSession = paraSparkSession;
		final LongAccumulator rowAccumulator = sparkSession.sparkContext().longAccumulator();
		final LongAccumulator colAccumulator = sparkSession.sparkContext().longAccumulator();
		final LongAccumulator factorAccumulator = sparkSession.sparkContext().longAccumulator();
		numRows = V.numRows();
		numCols = V.numCols();
		List<Double> tmpDBList= new ArrayList<Double>();
		//dummy factor row
		for (int i = 0; i< numFactors; i++){
			tmpDBList.add(initialNum);
		}

		JavaRDD<Double> tmpWRDD = sparkSession.createDataset(tmpDBList, Encoders.DOUBLE()).toJavaRDD();
		JavaRDD<MatrixEntry> tmpWEntryRDD = tmpWRDD.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
				List<MatrixEntry> tmpList = new ArrayList<MatrixEntry>();
				rowAccumulator.reset();
				for (int i=0;i<numRows;i++){
					//normal
					tmpList.add(new MatrixEntry(rowAccumulator.value(),factorAccumulator.value(),TopicUtil.getRandomValue(initialNum)));
					rowAccumulator.add(1);
				}
				factorAccumulator.add(1);
				return tmpList.iterator();
			}
		});

//		JavaRDD<MatrixEntry> tmpWEntryRDD1 = tmpWEntryRDD.map(new Function<MatrixEntry, MatrixEntry>() {
//			@Override
//			public MatrixEntry call(MatrixEntry matrixEntry) throws Exception {
//				//System.out.println("i:"+matrixEntry.i()+",j:"+matrixEntry.j());
//				return new MatrixEntry(matrixEntry.i(),matrixEntry.j(),0.01);
//			}
//		});
		System.out.println(tmpWEntryRDD.count());
		if(boolUpdateW) {
			W = new CoordinateMatrix(tmpWEntryRDD.rdd(), numRows, numFactors);
			minW = new CoordinateMatrix(tmpWEntryRDD.rdd(), numRows, numFactors);
			System.out.println(W.entries().count());
		}
		tmpDBList = new ArrayList<Double>();
		for (int i = 0; i< numFactors; i++){
			tmpDBList.add(initialNum);
		}

		JavaRDD<Double> tmpHRDD = sparkSession.createDataset(tmpDBList, Encoders.DOUBLE()).toJavaRDD();
		JavaRDD<MatrixEntry> tmpHEntryRDD = tmpWRDD.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
				List<MatrixEntry> tmpList = new ArrayList<MatrixEntry>();
				colAccumulator.reset();
				for (int i=0;i<numCols;i++){
					//normal
					tmpList.add(new MatrixEntry(factorAccumulator.value(),colAccumulator.value(),TopicUtil.getRandomValue(initialNum)));
					colAccumulator.add(1);
				}
				factorAccumulator.add(1);
				return tmpList.iterator();
			}
		});

//		JavaRDD<MatrixEntry> tmpHEntryRDD1 = tmpHEntryRDD.map(new Function<MatrixEntry, MatrixEntry>() {
//			@Override
//			public MatrixEntry call(MatrixEntry matrixEntry) throws Exception {
//				//System.out.println("i:"+matrixEntry.i()+",j:"+matrixEntry.j());
//				return new MatrixEntry(matrixEntry.i(),matrixEntry.j(),0.01);
//			}
//		});

		H = new CoordinateMatrix(tmpHEntryRDD.rdd(), numFactors,numCols);
		minH = new CoordinateMatrix(tmpHEntryRDD.rdd(), numFactors,numCols);
		System.out.println(H.entries().count());
//		H.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
//			@Override
//			public void call(MatrixEntry matrixEntry) throws Exception {
//				System.out.println("i:"+matrixEntry.i()+",j"+matrixEntry.j()+":"+matrixEntry.value());
//			}
//		});
		H.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
			@Override
			public void call(Vector vector) throws Exception {
				System.out.println(vector);
			}
		});
	}

	//update W,H
	public void buildNMFModel(){
		final ArrayList<Double> tmpKLDivergenceList = new ArrayList<Double>();
		for(int iter=0;iter<numIters;iter++) {
			CoordinateMatrix HUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, W.transpose().toBlockMatrix().multiply(V.toBlockMatrix()).toCoordinateMatrix(), W.transpose().toBlockMatrix().multiply(W.toBlockMatrix()).multiply(H.toBlockMatrix()).toCoordinateMatrix());
			HUpdateMatrix.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
				@Override
				public void call(Vector vector) throws Exception {
					System.out.println("HU vector:"+vector);
				}
			});

			H = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, H, HUpdateMatrix);

            if(boolUpdateW) {
                CoordinateMatrix WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, V.toBlockMatrix().multiply(H.transpose().toBlockMatrix()).toCoordinateMatrix(), W.toBlockMatrix().multiply(H.toBlockMatrix()).multiply(H.transpose().toBlockMatrix()).toCoordinateMatrix());
                W = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, W, WUpdateMatrix);
            }
			//System.out.println(H.entries().count());
			//System.out.println(W.entries().count());

			//compute KL Divergence
			double tmpKLDivergence = MeasureUtil.getKLDivergence(V,W,H,sparkSession);
			//System.out.println(tmpKLDivergence);
			tmpKLDivergenceList.add(tmpKLDivergence);
			if(tmpKLDivergence<minKLDivergence){
				minKLDivergence = tmpKLDivergence;
				//set minW
                JavaRDD<MatrixEntry> tmpW = W.entries().toJavaRDD().flatMap(new FlatMapFunction<MatrixEntry, MatrixEntry>() {
					@Override
					public Iterator<MatrixEntry> call(MatrixEntry matrixEntry) throws Exception {
						List<MatrixEntry> list = new ArrayList<MatrixEntry>();
						list.add(matrixEntry);
						return list.iterator();
					}
				});
				minW = new CoordinateMatrix(tmpW.rdd());
				//set minH
				JavaRDD<MatrixEntry> tmpH = H.entries().toJavaRDD().flatMap(new FlatMapFunction<MatrixEntry, MatrixEntry>() {
					@Override
					public Iterator<MatrixEntry> call(MatrixEntry matrixEntry) throws Exception {
						List<MatrixEntry> list = new ArrayList<MatrixEntry>();
						list.add(matrixEntry);
						return list.iterator();
					}
				});
				minH = new CoordinateMatrix(tmpH.rdd());
			}

			W.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
				@Override
				public void call(Vector vector) throws Exception {
					System.out.println("W vector:"+vector);
				}
			});

			//print minH
			minH.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
				@Override
				public void call(Vector vector) throws Exception {
					System.out.println("vector:"+vector);
				}
			});

//			minH.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
//				@Override
//				public void call(MatrixEntry matrixEntry) throws Exception {
//					System.out.println(matrixEntry.i()+","+matrixEntry.j()+":"+matrixEntry.value());
//				}
//			});
		}
	}

	public CoordinateMatrix getW() {
		return minW;
	}

	public void setW(CoordinateMatrix w) {
		W = w;
	}

	public CoordinateMatrix getH() {
		return minH;
	}
}
