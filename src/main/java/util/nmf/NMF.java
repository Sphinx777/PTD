package util.nmf;

import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NMF implements Serializable {
	private double initialNum = 0.01;
	private long numRows , numCols;
	//private DenseMatrix W,H,minW,minH;
	private CoordinateMatrix V , W , H , minW , minH;
	//static double tmpKLDivergence = 0.0;
    static double minKLDivergence = Double.MAX_VALUE;
	private boolean boolUpdateW;
	private SparkSession sparkSession;
	private int numFactors;
	private int numIters;
    static Logger logger = Logger.getLogger(NMF.class.getName());

	public NMF(CoordinateMatrix matV,boolean isUpdateW,SparkSession paraSparkSession , int paraNumFactors , int paraNumIters) {
		V = matV;
		boolUpdateW = isUpdateW;
		minKLDivergence = Double.MAX_VALUE;
		//initialize to zero or other method
		//double[] dbArray =new double[(int)(V.numRows() * numFactors)];
        //Arrays.fill(dbArray,0.01);
		this.sparkSession = paraSparkSession;
		final LongAccumulator rowAccumulator = sparkSession.sparkContext().longAccumulator("rowAccumulator");
		final LongAccumulator colAccumulator = sparkSession.sparkContext().longAccumulator("colAccumulator");

        numRows = V.numRows();
		numCols = V.numCols();
		List<Double> tmpDBList= new ArrayList<Double>();
		numFactors = paraNumFactors;
		numIters = paraNumIters;

        logger.info("set dummy factor row!");
		//dummy factor row
		for (int i = 0; i< numFactors; i++){
			tmpDBList.add((double) i);
		}

		JavaRDD<Double> tmpWRDD = sparkSession.createDataset(tmpDBList, Encoders.DOUBLE()).toJavaRDD();

        logger.info("set matrix Entry!");
		//JavaRDD<MatrixEntry> tmpWEntryRDD = tmpWRDD.flatMap(new DoubleMatrixEntryFlatMapFunction(rowAccumulator, factorAccumulator));
        JavaRDD<MatrixEntry> tmpWEntryRDD = tmpWRDD.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
            @Override
            public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
                List<MatrixEntry> tmpList = new ArrayList<MatrixEntry>();
                rowAccumulator.reset();
                for (int i=0;i<numRows;i++){
                    //normal
                    tmpList.add(new MatrixEntry(rowAccumulator.value(), aDouble.longValue(), TopicUtil.getRandomValue(initialNum)));
                    rowAccumulator.add(1);
                }

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

//        tmpWEntryRDD.foreach(new VoidFunction<MatrixEntry>() {
//            @Override
//            public void call(MatrixEntry matrixEntry) throws Exception {
//                System.out.println("W entry:("+matrixEntry.i()+","+matrixEntry.j()+")--"+matrixEntry.value());
//                logger.info("W entry:("+matrixEntry.i()+","+matrixEntry.j()+")--"+matrixEntry.value());
//            }
//    });
//        System.out.println(tmpWEntryRDD.count());
//        logger.info("W.entry count:"+tmpWEntryRDD.count());

		if(boolUpdateW) {
            logger.info("update The W!");
			tmpWEntryRDD.persist(StorageLevel.MEMORY_ONLY_SER());
			W = new CoordinateMatrix(tmpWEntryRDD.rdd(), numRows, numFactors);
			minW = new CoordinateMatrix(tmpWEntryRDD.rdd(), numRows, numFactors);
			//System.out.println(W.entries().count());
		}
		tmpDBList = new ArrayList<Double>();
		for (int i = 0; i< numFactors; i++){
			tmpDBList.add((double) i);
		}

		JavaRDD<Double> tmpHRDD = sparkSession.createDataset(tmpDBList, Encoders.DOUBLE()).toJavaRDD();

        logger.info("set tmp H entry!");
        JavaRDD<MatrixEntry> tmpHEntryRDD = tmpHRDD.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
			@Override
			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
				List<MatrixEntry> tmpList = new ArrayList<MatrixEntry>();
				colAccumulator.reset();
				for (int i=0;i<numCols;i++){
					//normal
					tmpList.add(new MatrixEntry(aDouble.longValue(),colAccumulator.value(),TopicUtil.getRandomValue(initialNum)));
					colAccumulator.add(1);
				}
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

		//System.out.println("tmpHEntryRDD:"+tmpHEntryRDD.count());
		tmpHEntryRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		H = new CoordinateMatrix(tmpHEntryRDD.rdd(), numFactors,numCols);
		minH = new CoordinateMatrix(tmpHEntryRDD.rdd(), numFactors,numCols);
		//System.out.println(H.entries().count());
//		tmpHEntryRDD.foreach(new VoidFunction<MatrixEntry>() {
//			@Override
//			public void call(MatrixEntry matrixEntry) throws Exception {
//				System.out.println("H entry:("+matrixEntry.i()+","+matrixEntry.j()+")--"+matrixEntry.value());
//				logger.info("H entry:("+matrixEntry.i()+","+matrixEntry.j()+")--"+matrixEntry.value());
//			}
//		});
//		System.out.println(tmpHEntryRDD.count());
		logger.info("print H entry!");
//		H.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
//			@Override
//			public void call(Vector vector) throws Exception {
//				System.out.println(vector);
//			}
//		});
        logger.info("NMF initial function finish");
	}

	//update W,H
	public void buildNMFModel(){
		final ArrayList<Double> tmpKLDivergenceList = new ArrayList<Double>();
		for(int iter=0;iter< numIters;iter++) {
            //logger.info("V.toRowMatrix after iter "+ iter +":"+V.toBlockMatrix().numRows()+","+V.toBlockMatrix().numCols());
            //logger.info("H.toBlock after iter "+ iter +":"+H.toBlockMatrix().numRows()+","+H.toBlockMatrix().numCols());
            //logger.info("W.toBlock after iter "+iter+":"+W.toBlockMatrix().numRows()+","+W.toBlockMatrix().numCols());
            //logger.info("W.transpose().toBlockMatrix().multiply(V.toBlockMatrix()):"+W.transpose().toBlockMatrix().multiply(V.toBlockMatrix()).toCoordinateMatrix().numRows()+","+W.transpose().toBlockMatrix().multiply(V.toBlockMatrix()).toCoordinateMatrix().numCols());
            //logger.info("W.transpose().toBlockMatrix().multiply(W.toBlockMatrix()).multiply(H.toBlockMatrix()):"+W.transpose().toBlockMatrix().multiply(W.toBlockMatrix()).multiply(H.toBlockMatrix()).toCoordinateMatrix().numRows()+","+W.transpose().toBlockMatrix().multiply(W.toBlockMatrix()).multiply(H.toBlockMatrix()).toCoordinateMatrix().numCols());

//            W.transpose().toBlockMatrix().multiply(V.toBlockMatrix()).toCoordinateMatrix().entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
//                @Override
//                public void call(MatrixEntry matrixEntry) throws Exception {
//                    System.out.println("W.tran1:("+matrixEntry.i()+","+matrixEntry.j()+")");
//                }
//            });

//            W.transpose().toBlockMatrix().multiply(W.toBlockMatrix()).multiply(H.toBlockMatrix()).toCoordinateMatrix().entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
//                @Override
//                public void call(MatrixEntry matrixEntry) throws Exception {
//                    System.out.println("W.tran2:("+matrixEntry.i()+","+matrixEntry.j()+")");
//                }
//            });
			BlockMatrix wBkMat = W.toBlockMatrix().persist(StorageLevel.MEMORY_ONLY_SER());
			BlockMatrix wTransBkMat = W.transpose().toBlockMatrix().persist(StorageLevel.MEMORY_ONLY_SER());
			BlockMatrix vBkMat = V.toBlockMatrix().persist(StorageLevel.MEMORY_ONLY_SER());
			BlockMatrix hBkMat = H.toBlockMatrix().persist(StorageLevel.MEMORY_ONLY_SER());
			BlockMatrix hTranBkMat = H.transpose().toBlockMatrix().persist(StorageLevel.MEMORY_ONLY_SER());

            CoordinateMatrix HUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, wTransBkMat.multiply(vBkMat), wTransBkMat.multiply(wBkMat).multiply(hBkMat));
//            HUpdateMatrix.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
//				@Override
//				public void call(Vector vector) throws Exception {
//					System.out.println("HU vector:"+vector);
//				}
//			});

            //logger.info("HU.toRowMatrix after multiply iter "+ iter +":"+HUpdateMatrix.toBlockMatrix().numRows()+","+HUpdateMatrix.toBlockMatrix().numCols());

			H = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, H.toBlockMatrix(), HUpdateMatrix.toBlockMatrix());

            if(boolUpdateW) {
				//logger.info("V.toBlockMatrix after multiply iter "+iter+":"+V.toBlockMatrix().numRows()+","+V.toBlockMatrix().numCols());
				//logger.info("H.trans.toBlock after multiply iter "+iter+":"+H.transpose().toBlockMatrix().numRows()+","+H.transpose().toBlockMatrix().numCols());
				//logger.info("W.toBlock after multiply iter "+iter+":"+W.toBlockMatrix().numRows()+","+W.toBlockMatrix().numCols());

                CoordinateMatrix WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, vBkMat.multiply(hTranBkMat), wBkMat.multiply(hBkMat).multiply(hTranBkMat));
                W = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, W.toBlockMatrix(), WUpdateMatrix.toBlockMatrix());
            }
			//System.out.println(H.entries().count());
			//System.out.println(W.entries().count());

			//compute KL Divergence
			double tmpKLDivergence = MeasureUtil.getKLDivergence(vBkMat,wBkMat,hBkMat,sparkSession);
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

//			W.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
//				@Override
//				public void call(Vector vector) throws Exception {
//					System.out.println("W vector:"+vector);
//				}
//			});

			//print minH
//			minH.toRowMatrix().rows().toJavaRDD().foreach(new VoidFunction<Vector>() {
//				@Override
//				public void call(Vector vector) throws Exception {
//					System.out.println("vector:"+vector);
//				}
//			});

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
