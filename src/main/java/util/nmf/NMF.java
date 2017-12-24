package util.nmf;

import edu.nju.pasalab.marlin.matrix.BlockMatrix;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import edu.nju.pasalab.marlin.matrix.DistributedMatrix;
import edu.nju.pasalab.marlin.utils.MTUtils;
import edu.nju.pasalab.marlin.utils.UniformGenerator;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.SizeEstimator;
import util.CmdArgs;
import util.MeasureUtil;
import util.TopicConstant;
import util.TopicUtil;

import java.io.Serializable;
import java.util.ArrayList;

public class NMF implements Serializable {
	private double initialNum = 0.01;
	private DenseVecMatrix V , originalW, originalH, minW , minH , newW, newH , minY, newY , originalY;
    static double minKLDivergence = java.lang.Double.MAX_VALUE;
	private boolean boolUpdateW;
    private JavaSparkContext javaSparkContext;
    private int numFactors , numIters;
	private long numRowsOfV;
    private DoubleAccumulator doubleAccumulator;
    static Logger logger = Logger.getLogger(NMF.class.getName());

	//for tNMijF use
	public DenseVecMatrix A;

	public NMF(DenseVecMatrix matV, boolean isUpdateW, JavaSparkContext paraSparkContext , int paraNumFactors , int paraNumIters , long numRows , long numCols) {
		V = matV;
        //V.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
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
		numRowsOfV = numRows;
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

    //tNMijF
    public void buildNMFModel_tNMijF(){
		final ArrayList<Double> tmpKLDivergenceList = new ArrayList<Double>();
		BlockMatrix wTranMatrix,yTranMatrix;
		DistributedMatrix wtranwMatrix,wtranVMatrix,wtranAmatrix;
		DenseVecMatrix HUpdateMatrix,WUpdateMatrix,YUpdateMatrix ,yytran;
		double tmpKLDivergence_A , tmpKLDivergence_V , sumOfDivergence;

		logger.info("build the tNMijF NMF model and initialize Y");
		System.out.println("build the tNMijF NMF model and initialize Y");

		originalY = MTUtils.randomDenVecMatrix(javaSparkContext.sc(),numFactors,Long.valueOf(numRowsOfV).intValue(),0,new UniformGenerator(0.0, 1.0));
		minY = originalY;

		logger.info("tNNijF initial finish");
		System.out.println("tNMijF initial finish");

		for(int iter=0;iter< numIters ; iter++){
			logger.info("Start to tNMijF iteration");
			System.out.println("Start to tNMijF iteration");
			//wtran
			logger.info("original W transpose compute!");
			System.out.println("original W transpose compute!");
			wTranMatrix = originalW.transpose();

			//ytran
			logger.info("original Y transpose compute!");
			System.out.println("original Y transpose compute!");
			yTranMatrix = originalY.transpose();


			logger.info("Start to compute Wupdate matrix");
			System.out.println("Start to compute Wupdate matrix");

			yytran = (DenseVecMatrix) originalY.multiply(yTranMatrix,CmdArgs.numThreshold);
			//W update matrix = (D)A * (B)ytran / (D?)wyMatrix * (B)ytran
			WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, (DenseVecMatrix)A.multiply(yTranMatrix,CmdArgs.cores),(DenseVecMatrix)originalW.multiply(yytran,CmdArgs.cores));
			//WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, (DenseVecMatrix) V.multiply(hTranMatrix,CmdArgs.cores),(DenseVecMatrix) originalW.multiply(hhtran,CmdArgs.cores));


			logger.info("start to compute new W:");
			newW = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Multiply, originalW, WUpdateMatrix);
			logger.info("compute W finish");
			System.out.println("compute W finish");

			//wtranAMatrix
			logger.info("compute wtranA matrix!");
			System.out.println("compute wtranA matrix!");
			wtranAmatrix = originalW.transpose().multiply(A,CmdArgs.cores,CmdArgs.numThreshold);
			if(wtranAmatrix instanceof BlockMatrix){
				logger.info("wtranAmatrix is block Matrix");
				System.out.println("wtranAmatrix is block Matrix");
				wtranAmatrix = ((BlockMatrix)wtranAmatrix).toDenseVecMatrix();
			}else if(wtranAmatrix instanceof DenseVecMatrix){
				logger.info("wtranAmatrix is DenseVecMatrix");
				System.out.println("wtranAmatrix is DenseVecMatrix");
			}

			//Y update matrix = (B)wtran * (D)A / (B)wtran * (D?)wyMatrix
			YUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, (DenseVecMatrix) wtranAmatrix, ((BlockMatrix)(((BlockMatrix)(wTranMatrix.multiply(originalW,CmdArgs.cores,CmdArgs.numThreshold))).multiply(originalY,CmdArgs.cores,CmdArgs.numThreshold))).toDenseVecMatrix());
//			TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,(DenseVecMatrix) wtranv,((BlockMatrix)(((BlockMatrix)(wTranMatrix.multiply(originalW,CmdArgs.cores,CmdArgs.numThreshold))).multiply(originalH,CmdArgs.cores,CmdArgs.numThreshold))).toDenseVecMatrix());

			logger.info("start to compute new Y:");
			newY = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Multiply, originalY, YUpdateMatrix);
			logger.info("compute Y finish");
			System.out.println("compute Y finish");

			//wtranVMatrix
			logger.info("compute wtranV matrix!");
			System.out.println("compute wtranV matrix!");
			wtranVMatrix = originalW.transpose().multiply(V,CmdArgs.cores,CmdArgs.numThreshold);
			if(wtranVMatrix instanceof BlockMatrix){
				logger.info("wtranVMatrix is block Matrix");
				System.out.println("wtranVMatrix is block Matrix");
				wtranVMatrix = ((BlockMatrix)wtranVMatrix).toDenseVecMatrix();
			}else if(wtranVMatrix instanceof DenseVecMatrix){
				logger.info("wtranVMatrix is DenseVecMatrix");
				System.out.println("wtranVMatrix is DenseVecMatrix");
			}

			//H update matrix = (B)wtran * (D)V / (B)wtran * (D?)whMatrix
			HUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, (DenseVecMatrix) wtranVMatrix,((BlockMatrix)(((BlockMatrix)(wTranMatrix.multiply(originalW,CmdArgs.cores,CmdArgs.numThreshold))).multiply(originalH,CmdArgs.cores,CmdArgs.numThreshold))).toDenseVecMatrix());
			//TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,(DenseVecMatrix) wtranv,((BlockMatrix)(((BlockMatrix)(wTranMatrix.multiply(originalW,CmdArgs.cores,CmdArgs.numThreshold))).multiply(originalH,CmdArgs.cores,CmdArgs.numThreshold))).toDenseVecMatrix());

			logger.info("start to compute new H:");
			newH = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Multiply, originalH, HUpdateMatrix);
			logger.info("compute H finish");
			System.out.println("compute H finish");


			//compute D(A||WY)
			doubleAccumulator.reset();
			logger.info("Start to compute D(A||WY) KLDivergence");
			tmpKLDivergence_A = MeasureUtil.getKLDivergence(A, newW, newY ,doubleAccumulator);
			logger.info("D(A||WY) getKLDivergence: "+tmpKLDivergence_A);
			System.out.println("D(A||WY) getKLDivergence: "+tmpKLDivergence_A);
			//tmpKLDivergenceList.add(tmpKLDivergence);

			//compute D(V||WH)
			doubleAccumulator.reset();
			logger.info("Start to compute D(V||WH) KLDivergence");
			tmpKLDivergence_V = MeasureUtil.getKLDivergence(V, newW, newH ,doubleAccumulator);
			logger.info("D(V||WH) getKLDivergence: "+tmpKLDivergence_V);
			System.out.println("D(V||WH) getKLDivergence: "+tmpKLDivergence_V);

			//sum D value and record
			sumOfDivergence = tmpKLDivergence_A + tmpKLDivergence_V;
			logger.info("sum of KLDivergence:"+sumOfDivergence);
			System.out.println("sum of KLDivergence:"+sumOfDivergence);

			//compare minimal Divergence
			if(sumOfDivergence<minKLDivergence) {
				logger.info("minKLDivergence:" + minKLDivergence);
				System.out.println("minKLDivergence:" + minKLDivergence);
				minKLDivergence = sumOfDivergence;

				minW = newW;
				minH = newH;
				minY = newY;
			}
			originalH = newH;
			originalW = newW;
			originalY = newY;
			logger.info("assign the new H , new W, new Y finish");
		}
		logger.info("finish the NMF");
	}

	//update originalW,originalH
	public void buildNMFModel_intJNMF_Related(){
		final ArrayList<Double> tmpKLDivergenceList = new ArrayList<Double>();
		logger.info("Start to build NMF model:");
        BlockMatrix wTranMatrix,hTranMatrix;
        DistributedMatrix wtranv;
        DenseVecMatrix HUpdateMatrix,hhtran,WUpdateMatrix;

        double tmpKLDivergence;
		for(int iter=0;iter< numIters;iter++) {
			logger.info("original W transpose compute:");
			wTranMatrix = originalW.transpose();
			//wTranMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
			logger.info("original H transpose compute:");
            hTranMatrix = originalH.transpose();
			//hTranMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());

            //logger.info("start to compute H--W:"+originalW.numRows()+","+originalW.numCols()+";H:"+originalH.numRows()+","+originalH.numCols());
            //System.out.println("start to compute H");

			logger.info("start to compute H update:");
            wtranv =  wTranMatrix.multiply(V, CmdArgs.cores , CmdArgs.numThreshold);

            if(wtranv instanceof BlockMatrix){
                logger.info("I am block Matrix");
                wtranv = ((BlockMatrix)wtranv).toDenseVecMatrix();
            }else if(wtranv instanceof DenseVecMatrix){
                logger.info("I am DenseVecMatrix");
            }


            HUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide,(DenseVecMatrix) wtranv,((BlockMatrix)(((BlockMatrix)(wTranMatrix.multiply(originalW,CmdArgs.cores,CmdArgs.numThreshold))).multiply(originalH,CmdArgs.cores,CmdArgs.numThreshold))).toDenseVecMatrix());
		    //HUpdateMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
            logger.info("start to compute new H:");
			newH = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Multiply, originalH, HUpdateMatrix);
            //newH.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
            logger.info("compute H finish");
            System.out.println("compute H finish");

//            if(boolUpdateW) {
//                CoordinateMatrix WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, vBkMat.multiply(hTranBkMat), wBkMat.multiply(hBkMat).multiply(hTranBkMat));
//                originalW = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Multiply, originalW.toBlockMatrix(), WUpdateMatrix.toBlockMatrix());
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
                logger.info("start to compute W update:");
                hhtran = (DenseVecMatrix) originalH.multiply(hTranMatrix,CmdArgs.numThreshold);

                WUpdateMatrix = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Divide, (DenseVecMatrix) V.multiply(hTranMatrix,CmdArgs.cores),(DenseVecMatrix) originalW.multiply(hhtran,CmdArgs.cores));
                //WUpdateMatrix.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
                logger.info("start to compute new W:");
                newW = TopicUtil.getCoorMatOption(TopicConstant.MatrixOperation.Multiply, originalW,WUpdateMatrix);
                //newW.rows().persist(StorageLevel.MEMORY_AND_DISK_SER());
                logger.info("compute W finish");
                System.out.println("compute W finish");
            }else{
				logger.info("new W is original W");
				newW = originalW;
			}
//
//			//compute KL Divergence
//			double tmpKLDivergence = MeasureUtil.getKLDivergence(vBkMat,wBkMat,hBkMat,sparkSession);
            doubleAccumulator.reset();
			logger.info("Start to compute KLDivergence");
            tmpKLDivergence = MeasureUtil.getKLDivergence(V, newW, newH ,doubleAccumulator);
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
			logger.info("assign the new H , new W finish");
		}
		//V.rows().unpersist(true);
		logger.info("finish the NMF");
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
