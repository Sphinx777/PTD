package util.nmf;

import breeze.linalg.DenseVector;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import edu.nju.pasalab.marlin.utils.MTUtils;
import edu.nju.pasalab.marlin.utils.UniformGenerator;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.SizeEstimator;
import scala.Tuple2;
import util.CmdArgs;
import util.MeasureUtil;
import util.TopicConstant;
import util.TopicUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

public class NMF implements Serializable {
	private double initialNum = 0.01;
	private DenseVecMatrix V , originalW, originalH, minW , minH , newW, newH;
    static double minKLDivergence = java.lang.Double.MAX_VALUE;
	private boolean boolUpdateW;
    private JavaSparkContext javaSparkContext;
    private int numFactors;
	private int numIters;
    private DoubleAccumulator doubleAccumulator;
    static Logger logger = Logger.getLogger(NMF.class.getName());

	public NMF(DenseVecMatrix matV, boolean isUpdateW, JavaSparkContext paraSparkContext , int paraNumFactors , int paraNumIters , long numRows , long numCols) {
		V = matV;
        V.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
		boolUpdateW = isUpdateW;
		minKLDivergence = java.lang.Double.MAX_VALUE;
		//initialize to zero or other method
		//double[] dbArray =new double[(int)(V.numRows() * numFactors)];
        //Arrays.fill(dbArray,0.01);
		this.javaSparkContext = paraSparkContext;
        doubleAccumulator = javaSparkContext.sc().doubleAccumulator();
		//final LongAccumulator rowAccumulator = sparkSession.sparkContext().longAccumulator("rowAccumulator");
		//final LongAccumulator colAccumulator = sparkSession.sparkContext().longAccumulator("colAccumulator");

        //numRows = V.numRows();
		//numCols = V.numCols();
		//List<Double> tmpDBList= new ArrayList<Double>();
		numFactors = paraNumFactors;
		numIters = paraNumIters;

        logger.info("set dummy factor row!");
        System.out.println("set dummy factor row!");
        //dummy factor row
		//old
//		for (int i = 0; i< numFactors; i++){
//			tmpDBList.add((double) i);
//		}
//
//		JavaRDD<Double> tmpWRDD = sparkSession.createDataset(tmpDBList, Encoders.DOUBLE()).toJavaRDD();
//
//        logger.info("set matrix Entry!");
//
//        JavaRDD<MatrixEntry> tmpWEntryRDD = tmpWRDD.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
//            @Override
//            public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
//                List<MatrixEntry> tmpList = new ArrayList<MatrixEntry>();
//                rowAccumulator.reset();
//                for (int i=0;i<numRows;i++){
//                    //normal
//                    tmpList.add(new MatrixEntry(rowAccumulator.value(), aDouble.longValue(), TopicUtil.getRandomValue(initialNum)));
//                    rowAccumulator.add(1);
//                }
//
//                return tmpList.iterator();
//            }
//        });

		if(boolUpdateW) {
            logger.info("update The originalW!");
            System.out.println("update The originalW!");
            //old
			//tmpWEntryRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
			//originalW = new CoordinateMatrix(tmpWEntryRDD.rdd(), numRows, numFactors);
			//minW = new CoordinateMatrix(tmpWEntryRDD.rdd(), numRows, numFactors);

			//new
			originalW = MTUtils.randomDenVecMatrix(javaSparkContext.sc(),numRows,numFactors,0,new UniformGenerator(0.0, 1.0));
			System.out.println("originalW mem size:"+SizeEstimator.estimate(originalW));
			minW = originalW;
		}

		//old
//		tmpDBList = new ArrayList<Double>();
//		for (int i = 0; i< numFactors; i++){
//			tmpDBList.add((double) i);
//		}
//
//		JavaRDD<Double> tmpHRDD = sparkSession.createDataset(tmpDBList, Encoders.DOUBLE()).toJavaRDD();
//
//        logger.info("set tmp originalH entry!");
//        JavaRDD<MatrixEntry> tmpHEntryRDD = tmpHRDD.flatMap(new FlatMapFunction<Double, MatrixEntry>() {
//			@Override
//			public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
//				List<MatrixEntry> tmpList = new ArrayList<MatrixEntry>();
//				colAccumulator.reset();
//				for (int i=0;i<numCols;i++){
//					//normal
//					tmpList.add(new MatrixEntry(aDouble.longValue(),colAccumulator.value(),TopicUtil.getRandomValue(initialNum)));
//					colAccumulator.add(1);
//				}
//				return tmpList.iterator();
//			}
//		});
//
//		tmpHEntryRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
//		originalH = new CoordinateMatrix(tmpHEntryRDD.rdd(), numFactors,numCols);
//		minH = new CoordinateMatrix(tmpHEntryRDD.rdd(), numFactors,numCols);

		//new
		originalH = MTUtils.randomDenVecMatrix(javaSparkContext.sc(),numFactors,Long.valueOf(numCols).intValue(),0,new UniformGenerator(0.0, 1.0));
		System.out.println("originalH mem size:"+SizeEstimator.estimate(originalH));
		minH = originalH;

        logger.info("NMF initial function finish");
        System.out.println("NMF initial function finish");
    }

	//update originalW,originalH
	public void buildNMFModel(){
		final ArrayList<Double> tmpKLDivergenceList = new ArrayList<Double>();
		for(int iter=0;iter< numIters;iter++) {

			DenseVecMatrix wTranMatrix = originalW.transpose().toDenseVecMatrix();
			wTranMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
			DenseVecMatrix hTranMatrix = originalH.transpose().toDenseVecMatrix();
			hTranMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());

            //logger.info("start to compute H--W:"+originalW.numRows()+","+originalW.numCols()+";H:"+originalH.numRows()+","+originalH.numCols());
            //System.out.println("start to compute H");

            DenseVecMatrix HUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,(DenseVecMatrix) wTranMatrix.multiply(V, CmdArgs.cores),(DenseVecMatrix) ((DenseVecMatrix) wTranMatrix.multiply(originalW,CmdArgs.cores)).multiply(originalH,CmdArgs.cores));
		    HUpdateMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
            newH = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, originalH, HUpdateMatrix);
            newH.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
            logger.info("compute H finish");
            System.out.println("compute H finish");

//            if(boolUpdateW) {
//                CoordinateMatrix WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, vBkMat.multiply(hTranBkMat), wBkMat.multiply(hBkMat).multiply(hTranBkMat));
//                originalW = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, originalW.toBlockMatrix(), WUpdateMatrix.toBlockMatrix());
//            }
            if(boolUpdateW){
                //logger.info("start to compute W--W:"+originalW.numRows()+","+originalW.numCols()+";H:"+originalH.numRows()+","+originalH.numCols());
                System.out.println("start to compute W");

//				originalW.rows().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, DenseVector<Object>>>() {
//					@Override
//					public void call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
//						ArrayList<Object> arrayList = new ArrayList<Object>();
//						Collections.addAll(arrayList,objectDenseVectorTuple2._2().data());
//						logger.info("oriW_row_length:"+objectDenseVectorTuple2._1()+","+((double[])arrayList.get(0)).length);
//						for (double db:(double[]) arrayList.get(0)){
//							logger.info("originalW value:"+db);
//						}
//					}
//				});
//
//				originalH.rows().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, DenseVector<Object>>>() {
//					@Override
//					public void call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
//						ArrayList<Object> arrayList = new ArrayList<Object>();
//						Collections.addAll(arrayList,objectDenseVectorTuple2._2().data());
//						logger.info("oriH_row_length:"+objectDenseVectorTuple2._1()+","+((double[])arrayList.get(0)).length);
//						for (double db:(double[]) arrayList.get(0)){
//							logger.info("originalH value:"+db);
//						}
//					}
//				});
//				DenseVecMatrix front = (DenseVecMatrix) originalW.multiply(originalH, TopicConstant.cores);
//				front.rows().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, DenseVector<Object>>>() {
//					@Override
//					public void call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
//						ArrayList<Object> arrayList = new ArrayList<Object>();
//						Collections.addAll(arrayList,objectDenseVectorTuple2._2().data());
//						logger.info("front_row_length:"+objectDenseVectorTuple2._1()+","+((double[])arrayList.get(0)).length);
//						for (double db:(double[]) arrayList.get(0)){
//							logger.info("front value:"+db);
//						}
//					}
//				});
//				logger.info("front:"+front.numRows()+","+front.numCols());
//				DenseVecMatrix tmpWUpdate2 = (DenseVecMatrix) ((DenseVecMatrix) originalW.multiply(originalH, TopicConstant.cores)).multiply(originalH.transpose(),TopicConstant.cores);
//				logger.info("tmpWUpdate2 row length:"+tmpWUpdate2.numRows());
//
//				tmpWUpdate2.rows().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, DenseVector<Object>>>() {
//					@Override
//					public void call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
//						ArrayList<Object> arrayList = new ArrayList<Object>();
//						Collections.addAll(arrayList,objectDenseVectorTuple2._2().data());
//						logger.info("w update2 tuple length:"+((double[])arrayList.get(0)).length);
//					}
//				});
//				tmpWUpdate2.rows().toJavaRDD().collect();

                DenseVecMatrix WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, V.toBlockMatrix(1,1).multiply(hTranMatrix.toBlockMatrix(1,1)).toDenseVecMatrix(),(DenseVecMatrix) ((DenseVecMatrix) originalW.multiply(originalH,CmdArgs.cores)).multiply(hTranMatrix,CmdArgs.cores));
                WUpdateMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
                newW = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Mutiply, originalW,WUpdateMatrix);
                newW.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
                logger.info("compute W finish");
                System.out.println("compute W finish");
            }else{
				newW = originalW;
			}
//
//			//compute KL Divergence
//			double tmpKLDivergence = MeasureUtil.getKLDivergence(vBkMat,wBkMat,hBkMat,sparkSession);
            doubleAccumulator.reset();
            double tmpKLDivergence = MeasureUtil.getKLDivergence(V, newW, newH ,doubleAccumulator);
            logger.info("getKLDivergence: "+tmpKLDivergence);
            System.out.println("getKLDivergence: "+tmpKLDivergence);
//			//System.out.println(tmpKLDivergence);
			tmpKLDivergenceList.add(tmpKLDivergence);
			if(tmpKLDivergence<minKLDivergence){
                logger.info("minKLDivergence:"+minKLDivergence);
                System.out.println("minKLDivergence:"+minKLDivergence);
                minKLDivergence = tmpKLDivergence;
				//set minW
//                JavaRDD<MatrixEntry> tmpW = originalW.entries().toJavaRDD().flatMap(new FlatMapFunction<MatrixEntry, MatrixEntry>() {
//					@Override
//					public Iterator<MatrixEntry> call(MatrixEntry matrixEntry) throws Exception {
//						List<MatrixEntry> list = new ArrayList<MatrixEntry>();
//						list.add(matrixEntry);
//						return list.iterator();
//					}
//				});
//				minW = new CoordinateMatrix(tmpW.rdd());
                minW = newW;
//				//set minH
//				JavaRDD<MatrixEntry> tmpH = originalH.entries().toJavaRDD().flatMap(new FlatMapFunction<MatrixEntry, MatrixEntry>() {
//					@Override
//					public Iterator<MatrixEntry> call(MatrixEntry matrixEntry) throws Exception {
//						List<MatrixEntry> list = new ArrayList<MatrixEntry>();
//						list.add(matrixEntry);
//						return list.iterator();
//					}
//				});
//				minH = new CoordinateMatrix(tmpH.rdd());
                minH = newH;
			}
			originalH = newH;
            originalW = newW;
		}
	}

	public DenseVecMatrix getW() {
		return minW;
	}

	public void setW(DenseVecMatrix originalW) {
		this.originalW = originalW;
	}

	public DenseVecMatrix getH() {
		return minH;
	}
}
