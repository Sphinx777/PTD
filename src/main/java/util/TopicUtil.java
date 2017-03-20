package util;

import breeze.linalg.DenseVector;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;
import scala.Tuple2;
import vo.TweetInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TopicUtil {
    private static Random r = new Random(System.currentTimeMillis());
    static final double dbMinForRandom = 0.0;
    static Logger logger = Logger.getLogger(TopicUtil.class.getName());

    public static int computeArrayIntersection(String[] arr1, String[] arr2) {
        ObjectLinkedOpenHashSet<String> aList = new ObjectLinkedOpenHashSet<String>(Arrays.asList(arr1));
        ObjectLinkedOpenHashSet<String> bList = new ObjectLinkedOpenHashSet<String>(Arrays.asList(arr2));
        ObjectArrayList<String> list = new ObjectArrayList<String>();

        for (String str : bList) {
            if (!aList.add(str)) {
                list.add(str);
            }
        }

        //if (list.size() > 0) {
            //System.out.println();
        //}
        return list.size();
    }

    public static int computeArrayUnion(String[] arr1, String[] arr2) {
        ObjectOpenHashSet<String> tmpSet = new ObjectOpenHashSet<String>();
        tmpSet.addAll(Arrays.asList(arr1));
        tmpSet.addAll(Arrays.asList(arr2));

        return tmpSet.size();
    }

    public static double calculateSigmoid(double x) {
        return (1 / (1 + Math.pow(Math.E, (-1 * x))));
    }

//    public static CoordinateMatrix getInverseCoordinateMatrix(CoordinateMatrix srcMatrix) {
//        SingularValueDecomposition<RowMatrix, Matrix> svdComs = srcMatrix.toRowMatrix().computeSVD((int) srcMatrix.numCols(), true, 1e-9);
//        final long numRows = srcMatrix.numRows();
//        final long numCols = srcMatrix.numCols();
//        final LongAccumulator rowAccumulator = TopicMain.sparkSession.sparkContext().longAccumulator();
//        final LongAccumulator colAccumulator = TopicMain.sparkSession.sparkContext().longAccumulator();
//
//        //U
//        rowAccumulator.reset();
//        colAccumulator.reset();
//        JavaRDD<MatrixEntry> UMatRDD = svdComs.U().rows().toJavaRDD().flatMap(new FlatMapFunction<Vector, MatrixEntry>() {
//            @Override
//            public Iterator<MatrixEntry> call(Vector vector) throws Exception {
//                List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//                for (double db : vector.toArray()) {
//                    System.out.println(db);
//                    arrayList.add(new MatrixEntry(rowAccumulator.value(), colAccumulator.value(), db));
//                    colAccumulator.add(1);
//                    if (colAccumulator.value() >= numCols) {
//                        colAccumulator.reset();
//                        rowAccumulator.add(1);
//                    }
//
//                    if (rowAccumulator.value() >= numRows) {
//                        rowAccumulator.reset();
//                    }
//                }
//                return arrayList.iterator();
//            }
//        });
//        UMatRDD.count();
//        CoordinateMatrix UtranMat = new CoordinateMatrix(UMatRDD.rdd()).transpose();
//
//        //S
//        double[] dbSvds = svdComs.s().toArray();
//
//        for (int i = 0; i < dbSvds.length; i++) {
//            //System.out.println("before db:"+dbSvds[i]);
//            dbSvds[i] = Math.pow(dbSvds[i], -1);
//            //System.out.println("after db:"+dbSvds[i]);
//        }
//        colAccumulator.reset();
//        JavaRDD<MatrixEntry> SMatRDD = TopicMain.sparkSession.createDataset(Arrays.asList(ArrayUtils.toObject(dbSvds)), Encoders.DOUBLE()).toJavaRDD().flatMap(new FlatMapFunction<Double, MatrixEntry>() {
//            @Override
//            public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
//                List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//                arrayList.add(new MatrixEntry(colAccumulator.value(), colAccumulator.value(), aDouble.doubleValue()));
//                colAccumulator.add(1);
//                return arrayList.iterator();
//            }
//        });
//        //System.out.println(sMatRDD.count());
//        CoordinateMatrix sInvMat = new CoordinateMatrix(SMatRDD.rdd());
//
//        //V
////		for (int i=0;i<svdComs.V().numRows();i++){
////			for (int j=0;j<svdComs.V().numCols();j++){
////				System.out.println("("+i+","+j+"):"+svdComs.V().apply(i,j));
////			}
////		}
//        rowAccumulator.reset();
//        colAccumulator.reset();
//        JavaRDD<MatrixEntry> VMatRDD = TopicMain.sparkSession.createDataset(Arrays.asList(ArrayUtils.toObject(svdComs.V().toArray())), Encoders.DOUBLE()).toJavaRDD().flatMap(new FlatMapFunction<Double, MatrixEntry>() {
//            @Override
//            public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
//                List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//                arrayList.add(new MatrixEntry(rowAccumulator.value(), colAccumulator.value(), aDouble.doubleValue()));
//                rowAccumulator.add(1);
//                if (rowAccumulator.value() >= numRows) {
//                    rowAccumulator.reset();
//                    colAccumulator.add(1);
//                }
//
//                if (colAccumulator.value() >= numCols) {
//                    colAccumulator.reset();
//                }
//                return arrayList.iterator();
//            }
//        });
//        //VMatRDD.count();
//        CoordinateMatrix vMat = new CoordinateMatrix(VMatRDD.rdd());
//
//        System.out.println(vMat.numRows() + "," + vMat.numCols());
//        System.out.println(sInvMat.numRows() + "," + sInvMat.numCols());
//        System.out.println(UtranMat.numRows() + "," + UtranMat.numCols());
//        return vMat.toBlockMatrix().multiply(sInvMat.toBlockMatrix()).multiply(UtranMat.toBlockMatrix()).toCoordinateMatrix();
//    }

    public static DenseVecMatrix getCoorMatOption(TopicConstant.MatrixOperation srcOpt, DenseVecMatrix matDividend, DenseVecMatrix matDivisor) {
        TopicConstant.MatrixOperation operation = srcOpt;
        logger.info("getCoorMatOption start:"+srcOpt.toString());
        System.out.println("getCoorMatOption start:"+srcOpt.toString());

        //long , DenseVector<Double> --> Object , DenseVector<Object>

//        JavaPairRDD<String, Double> tmpDividendRDD = matDividend.toCoordinateMatrix().entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, String, Double>() {
//            public Tuple2<String, Double> call(MatrixEntry entry) throws Exception {
//                return new Tuple2<String, Double>(entry.i() + TopicConstant.COMMA_DELIMITER + entry.j(), entry.value());
//            }
//        });
     JavaPairRDD<Object,ObjectArrayList<Object>> tmpDividendRDD = matDividend.rows().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, DenseVector<Object>>, Object, ObjectArrayList<Object>>() {
         @Override
         public Tuple2<Object, ObjectArrayList<Object>> call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
             logger.info("matDividend map to pair");
             System.out.println("matDividend map to pair");

             ObjectArrayList<Object> arrayList = new ObjectArrayList<>();
             Collections.addAll(arrayList, objectDenseVectorTuple2._2().data());
             return new Tuple2<Object, ObjectArrayList<Object>>((Object) objectDenseVectorTuple2._1(),arrayList);
         }
     });

//        JavaPairRDD<String, Double> tmpDivisorRDD = matDivisor.toCoordinateMatrix().entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, String, Double>() {
//            public Tuple2<String, Double> call(MatrixEntry entry) throws Exception {
//                return new Tuple2<String, Double>(entry.i() + TopicConstant.COMMA_DELIMITER + entry.j(), entry.value());
//            }
//        });
tmpDividendRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

    JavaPairRDD<Object,ObjectArrayList<Object>> tmpDivisorRDD = matDivisor.rows().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, DenseVector<Object>>, Object, ObjectArrayList<Object>>() {
        @Override
        public Tuple2<Object, ObjectArrayList<Object>> call(Tuple2<Object, DenseVector<Object>> objectDenseVectorTuple2) throws Exception {
            logger.info("matDivisor map to pair");
            System.out.println("matDivisor map to pair");
            ObjectArrayList<Object> arrayList = new ObjectArrayList<>();
            Collections.addAll(arrayList, objectDenseVectorTuple2._2().data());
            return new Tuple2<Object, ObjectArrayList<Object>>((Object) objectDenseVectorTuple2._1(),arrayList);
        }
    });

        tmpDivisorRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        JavaPairRDD<String, Tuple2<Double, Optional<Double>>> tmpRDD = tmpDividendRDD.leftOuterJoin(tmpDivisorRDD);

     JavaPairRDD<Object,Tuple2<ObjectArrayList<Object>, Optional<ObjectArrayList<Object>>>> tmpRDD = tmpDividendRDD.leftOuterJoin(tmpDivisorRDD);
        System.out.println("tmpRDD mem size:"+ SizeEstimator.estimate(tmpRDD));
        tmpRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        class Tuple2MatrixEntryFlatMapFunction implements FlatMapFunction<Tuple2<String, Tuple2<Double, Optional<Double>>>, MatrixEntry> {
//            private final TopicConstant.MatrixOperation operation;

//            public Tuple2MatrixEntryFlatMapFunction(TopicConstant.MatrixOperation operation) {
//                this.operation = operation;
//            }

//            @Override
//            public Iterator<MatrixEntry> call(Tuple2<String, Tuple2<Double, Optional<Double>>> stringTuple2Tuple2) throws Exception {
//                List<MatrixEntry> entryList = new ArrayList<MatrixEntry>();
//                String[] loc = stringTuple2Tuple2._1().split(TopicConstant.COMMA_DELIMITER);
//                if (stringTuple2Tuple2._2()._2().isPresent()) {
//                    double dividend = stringTuple2Tuple2._2()._1();
//                    double divisor = stringTuple2Tuple2._2()._2().get();
//                    double value = 0.0;
//                    if (operation.equals(TopicConstant.MatrixOperation.Divide)) {
//                        value = dividend / divisor;
//                    } else if (operation.equals(TopicConstant.MatrixOperation.Mutiply)) {
//                        value = dividend * divisor;
//                    }
//                    entryList.add(new MatrixEntry(Long.parseLong(loc[0]), Long.parseLong(loc[1]), value));
//                }
//                for (MatrixEntry mat:entryList) {
//                    System.out.println("tuple iterator:("+mat.i()+","+mat.j()+")");
//                }
//                return entryList.iterator();
//            }
//        }
//        JavaRDD<MatrixEntry> resultRDD = tmpRDD.flatMap(new Tuple2MatrixEntryFlatMapFunction(operation));

     //JavaPairRDD<Object,DenseVector<Object>> resultRDD = tmpRDD.mapToPair(new Tuple2DenseVectorPairFunction(operation));

        //coordinate matrix version
        //JavaRDD<Tuple2<Tuple2<Object,Object>,Object>> resultRDD = tmpRDD.flatMap(new Tuple2DenseVectorPairFunction(operation));
        //denseVecMatrix
        JavaPairRDD<Object,ObjectArrayList<Object>> resultRDD = tmpRDD.mapToPair(new Tuple2DenseVectorPairFunction(operation));

        System.out.println("resultRDD mem size:"+SizeEstimator.estimate(resultRDD));

        JavaRDD<Tuple2<Object,DenseVector<Object>>> resultRDD2 = resultRDD.map(new Function<Tuple2<Object, ObjectArrayList<Object>>, Tuple2<Object, DenseVector<Object>>>() {
            @Override
            public Tuple2<Object, DenseVector<Object>> call(Tuple2<Object, ObjectArrayList<Object>> v1) throws Exception {
                double [] doubles = new double[v1._2().size()];
                for(int i=0;i<doubles.length;i++){
                    doubles[i] = Double.valueOf((Double) v1._2().toArray()[i]).doubleValue();
                }
                List<double[]> list = Arrays.asList(doubles);
                DenseVector<Object> denseVector = new DenseVector<Object>(list.toArray()[0]);
                return new Tuple2<Object, DenseVector<Object>>(v1._1(),denseVector);
            }
        });

        System.out.println("resultRDD2 mem size"+SizeEstimator.estimate(resultRDD2));
        //CoordinateMatrix coordinateMatrix = new CoordinateMatrix(resultRDD.rdd());
        //DenseVecMatrix denseVecMatrix = coordinateMatrix.toDenseVecMatrix();
        DenseVecMatrix denseVecMatrix = new DenseVecMatrix(resultRDD2.rdd());

        System.out.println("denseVecMatrix mem size:"+SizeEstimator.estimate(denseVecMatrix));
        logger.info("getCoorMatOption finish");
        System.out.println("getCoorMatOption finish");
        return denseVecMatrix;
    }

    public static double getRandomValue(double max) {
        return dbMinForRandom + (max - dbMinForRandom) * r.nextDouble();
    }

    public static double getWeightedValue(String strDateTime1, String strDateTime2, Date currDate) {
        Date dateTime1 = null, dateTime2 = null;
        try {
            dateTime1 = new SimpleDateFormat(TopicConstant.DATE_FORMAT, Locale.ENGLISH).parse(strDateTime1);
            dateTime2 = new SimpleDateFormat(TopicConstant.DATE_FORMAT, Locale.ENGLISH).parse(strDateTime2);
        } catch (ParseException e) {
            e.printStackTrace();
            return 1;
        }

        Date dateTimeLeast = dateTime1.after(dateTime2) ? dateTime1 : dateTime2;
        double dbWeighted = 1;
        long diffT1T2 = Math.abs(dateTime1.getTime() - dateTime2.getTime());
        long diffT1T2InDays = (long) (diffT1T2 / (24 * 60 * 60 * 1000));
        long diffTnowLeast = currDate.getTime() - dateTimeLeast.getTime();
        long diffTnowLeastInDays = (long) (diffTnowLeast / (24 * 60 * 60 * 1000));
        if (diffT1T2InDays != 0 || diffTnowLeastInDays != 0) {
            dbWeighted = 1 + (1.0 / (diffT1T2InDays + diffTnowLeastInDays));
        }
        return dbWeighted;
    }

    public static JavaRDD<TweetInfo> preProcessTweetRDD(Dataset<TweetInfo> tweetInfoDataset){
        RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("tweet").setOutputCol("token").setPattern("\\W").setMinTokenLength(3);
        Dataset<Row> wordsData = regexTokenizer.transform(tweetInfoDataset);

        StopWordsRemover remover = new StopWordsRemover().setInputCol("token").setOutputCol("words");
        Dataset<Row> filterData = remover.transform(wordsData);
        filterData.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //create corpus
        JavaRDD<TweetInfo> outputCorpusStringRDD = filterData.toJavaRDD().map(new Function<Row, TweetInfo>() {
            @Override
            public TweetInfo call(Row v1) throws Exception {
                TweetInfo tweetInfo = new TweetInfo();
                tweetInfo.setUserName(v1.getAs("userName"));
                List<Object> rowList = v1.getList(v1.size()-1);
                tweetInfo.setTweet(String.join(TopicConstant.SPACE_DELIMITER,Arrays.copyOf(rowList.toArray(),rowList.size(),String[].class)));
                tweetInfo.setDateString(v1.getAs("dateString"));
                tweetInfo.setMentionMen(v1.getAs("mentionMen"));
                tweetInfo.setTweetId(v1.getAs("tweetId"));
                tweetInfo.setUserInteraction(v1.getAs("userInteraction"));
                return tweetInfo;
            }
        });

        //outputCorpusStringRDD.collect();
        return outputCorpusStringRDD;
    }

    public static void transformToVector(Dataset<Row> tweetInfoJavaRDD,String corpusFilePath,String wordVectorFilePath){
        RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("tweet").setOutputCol("token").setPattern("\\W").setMinTokenLength(3);
        Dataset<Row> wordsData = regexTokenizer.transform(tweetInfoJavaRDD);

        StopWordsRemover remover = new StopWordsRemover().setInputCol("token").setOutputCol("words");
        Dataset<Row> filterData = remover.transform(wordsData);
        filterData.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //create corpus
        JavaRDD<String> outputCorpusStringRDD = filterData.select("words").toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                List<Object> rowList = v1.getList(0);
                return String.join(TopicConstant.SPACE_DELIMITER,Arrays.copyOf(rowList.toArray(),rowList.size(),String[].class));
            }
        });
        outputCorpusStringRDD.collect();
        outputCorpusStringRDD.coalesce(1,true).saveAsTextFile(corpusFilePath);

        //create word vector
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("vector")
                .setMinCount(0);

        Word2VecModel model = word2Vec.fit(filterData);
        Dataset<Row> vectorDS = model.transform(filterData);
        filterData.unpersist();
        model.getVectors().toJavaRDD().saveAsTextFile(wordVectorFilePath);
    }

    public static ObjectArrayList<String[]> readTopicWordList(JavaSparkContext sc, String inputTopicWordPath){
        ObjectArrayList<String[]> topicWordList = new ObjectArrayList<String[]>();
        //BufferedReader br = null;
        try {
            JavaRDD<String> stringJavaRDD = sc.textFile(inputTopicWordPath,1);
            List<String> tmpStrList = stringJavaRDD.collect();
            for(String string:tmpStrList){
                topicWordList.add(string.split("\\s+"));
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }

        return topicWordList;
    }

    public static void writeParameter(String outFilePath){
        String paraFilePath = outFilePath + File.separator + "parameter";
        FileSystem fileSystem;
        BufferedWriter br;
        try {
            fileSystem = FileSystem.get(new File(paraFilePath).toURI(),new Configuration());
            Path file = new Path(paraFilePath);
            if(fileSystem.exists(file)){
                fileSystem.delete(file,true);
            }

            OutputStream os = fileSystem.create(file,
                    new Progressable() {
                        @Override
                        public void progress() {
                            System.out.println("...bytes written...");
                        }
                    }
            );

            br = new BufferedWriter(new OutputStreamWriter(os , "UTF-8"));
            br.write("inputFilePath(-input):"+CmdArgs.inputFilePath+TopicConstant.LINE_BREAKER);
            br.write("outputFilePath(-output):"+CmdArgs.outputFilePath+TopicConstant.LINE_BREAKER);
            br.write("coherenceFilePath(-cohInput):"+CmdArgs.coherenceFilePath+TopicConstant.LINE_BREAKER);

            br.write("model(-model):"+CmdArgs.model+TopicConstant.LINE_BREAKER);
            br.write("numIters(-iters):"+CmdArgs.numIters+TopicConstant.LINE_BREAKER);
            br.write("numFactors(-factor):"+CmdArgs.numFactors+TopicConstant.LINE_BREAKER);
            br.write("numTopWords(-top):"+CmdArgs.numTopWords+TopicConstant.LINE_BREAKER);
            br.write("cores(-cores):"+CmdArgs.cores+TopicConstant.LINE_BREAKER);

            br.close();
            fileSystem.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    //coordinate matrix version
//    private static class Tuple2DenseVectorPairFunction implements FlatMapFunction<Tuple2<Object,Tuple2<ArrayList<Object>,Optional<ArrayList<Object>>>>,Tuple2<Tuple2<Object, Object>, Object>> {
//denseVecMatrix
    private static class Tuple2DenseVectorPairFunction implements PairFunction<Tuple2<Object,Tuple2<ObjectArrayList<Object>,Optional<ObjectArrayList<Object>>>>,Object, ObjectArrayList<Object>> {
        private final TopicConstant.MatrixOperation operation;
        public Tuple2DenseVectorPairFunction(TopicConstant.MatrixOperation operation) {
            this.operation = operation;
        }

//            public Iterator<MatrixEntry> call(Tuple2<String, Tuple2<Double, Optional<Double>>> stringTuple2Tuple2) throws Exception {
//                List<MatrixEntry> entryList = new ArrayList<MatrixEntry>();
//                String[] loc = stringTuple2Tuple2._1().split(TopicConstant.COMMA_DELIMITER);
//                if (stringTuple2Tuple2._2()._2().isPresent()) {
//                    double dividend = stringTuple2Tuple2._2()._1();
//                    double divisor = stringTuple2Tuple2._2()._2().get();
//                    double value = 0.0;
//                    if (operation.equals(TopicConstant.MatrixOperation.Divide)) {
//                        value = dividend / divisor;
//                    } else if (operation.equals(TopicConstant.MatrixOperation.Mutiply)) {
//                        value = dividend * divisor;
//                    }
//                    entryList.add(new MatrixEntry(Long.parseLong(loc[0]), Long.parseLong(loc[1]), value));
//                }
//                for (MatrixEntry mat:entryList) {
//                    System.out.println("tuple iterator:("+mat.i()+","+mat.j()+")");
//                }
//                return entryList.iterator();
//            }


//coordinatematrix version
//        @Override
//        public Iterator<Tuple2<Tuple2<Object, Object>, Object>> call(Tuple2<Object,Tuple2<ArrayList<Object>,Optional<ArrayList<Object>>>> tuple2){
//            Long rowNo = (Long) tuple2._1();
//            ArrayList<Tuple2<Tuple2<Object,Object>,Object>> tuple2s = new ArrayList<Tuple2<Tuple2<Object, Object>, Object>>();
//            Tuple2<Object,Object> locTuple;
//            Float flt;
//            if(tuple2._2()._2().isPresent()){
//                double[] dividendArray =  (double[]) ((ArrayList<Object>)tuple2._2()._1()).get(0);
//                double[] divisorArray = (double[]) (((ArrayList<Object>)tuple2._2()._2().get()).get(0));
//                logger.info("dividend array length:"+dividendArray.length);
//                logger.info("divisor array length:"+divisorArray.length);
//                for (int idx=0;idx<dividendArray.length;idx++){
//                    locTuple = new Tuple2<Object, Object>(rowNo,Long.valueOf(idx));
//                    if(operation.equals(TopicConstant.MatrixOperation.Divide)){
//                        flt = Double.valueOf((double)dividendArray[idx] /(double) divisorArray[idx]).floatValue();
//                        tuple2s.add(new Tuple2<Tuple2<Object, Object>, Object>(locTuple,flt));
//                    }else if(operation.equals(TopicConstant.MatrixOperation.Mutiply)){
//                        flt = Double.valueOf((double) dividendArray[idx] * (double) divisorArray[idx]).floatValue();
//                        tuple2s.add(new Tuple2<Tuple2<Object, Object>, Object>(locTuple, flt));
//                    }
//                }
//            }
//            return tuple2s.iterator();
//        }

        //denseVecMatrix
        public Tuple2<Object,ObjectArrayList<Object>> call(Tuple2<Object,Tuple2<ObjectArrayList<Object>,Optional<ObjectArrayList<Object>>>> tuple2){
            Long rowNo = (Long) tuple2._1();
            ObjectArrayList<Object> tuple2s = new ObjectArrayList<Object>();
            double flt;
            if(tuple2._2()._2().isPresent()){
                double[] dividendArray =  (double[]) ((ObjectArrayList<Object>)tuple2._2()._1()).get(0);
                double[] divisorArray = (double[]) (((ObjectArrayList<Object>)tuple2._2()._2().get()).get(0));
                logger.info("dividend array length:"+dividendArray.length);
                logger.info("divisor array length:"+divisorArray.length);
                for (int idx=0;idx<dividendArray.length;idx++){
                    if(operation.equals(TopicConstant.MatrixOperation.Divide)){
                        flt = Double.valueOf((double)dividendArray[idx] /(double) divisorArray[idx]).doubleValue();
                        //tuple2s.add(new Tuple2<Tuple2<Object, Object>, Object>(locTuple,flt));
                        tuple2s.add(flt);
                    }else if(operation.equals(TopicConstant.MatrixOperation.Mutiply)){
                        flt = Double.valueOf((double) dividendArray[idx] * (double) divisorArray[idx]).doubleValue();
                        //tuple2s.add(new Tuple2<Tuple2<Object, Object>, Object>(locTuple, flt));
                        tuple2s.add(flt);
                    }
                }
            }
            return new Tuple2<Object, ObjectArrayList<Object>>(rowNo,tuple2s);
        }
    }
}
