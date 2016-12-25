package util;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.*;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import topicDerivation.TopicMain;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TopicUtil {
    private static Random r = new Random(System.currentTimeMillis());
    static final double dbMinForRandom = 0.0;

    public static int computeArrayIntersection(String[] arr1, String[] arr2) {
        Set<String> aList = new LinkedHashSet<String>(Arrays.asList(arr1));
        Set<String> bList = new LinkedHashSet<String>(Arrays.asList(arr2));
        ArrayList<String> list = new ArrayList<String>();

        for (String str : bList) {
            if (!aList.add(str)) {
                list.add(str);
            }
        }

        if (list.size() > 0) {
            //System.out.println();
        }
        return list.size();
    }

    public static int computeArrayUnion(String[] arr1, String[] arr2) {
        HashSet<String> tmpSet = new HashSet<String>();
        tmpSet.addAll(Arrays.asList(arr1));
        tmpSet.addAll(Arrays.asList(arr2));

        return tmpSet.size();
    }

    public static double calculateSigmoid(double x) {
        return (1 / (1 + Math.pow(Math.E, (-1 * x))));
    }

    public static CoordinateMatrix getInverseCoordinateMatrix(CoordinateMatrix srcMatrix) {
        SingularValueDecomposition<RowMatrix, Matrix> svdComs = srcMatrix.toRowMatrix().computeSVD((int) srcMatrix.numCols(), true, 1e-9);
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
                for (double db : vector.toArray()) {
                    System.out.println(db);
                    arrayList.add(new MatrixEntry(rowAccumulator.value(), colAccumulator.value(), db));
                    colAccumulator.add(1);
                    if (colAccumulator.value() >= numCols) {
                        colAccumulator.reset();
                        rowAccumulator.add(1);
                    }

                    if (rowAccumulator.value() >= numRows) {
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

        for (int i = 0; i < dbSvds.length; i++) {
            //System.out.println("before db:"+dbSvds[i]);
            dbSvds[i] = Math.pow(dbSvds[i], -1);
            //System.out.println("after db:"+dbSvds[i]);
        }
        colAccumulator.reset();
        JavaRDD<MatrixEntry> SMatRDD = TopicMain.sparkSession.createDataset(Arrays.asList(ArrayUtils.toObject(dbSvds)), Encoders.DOUBLE()).toJavaRDD().flatMap(new FlatMapFunction<Double, MatrixEntry>() {
            @Override
            public Iterator<MatrixEntry> call(Double aDouble) throws Exception {
                List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
                arrayList.add(new MatrixEntry(colAccumulator.value(), colAccumulator.value(), aDouble.doubleValue()));
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
                arrayList.add(new MatrixEntry(rowAccumulator.value(), colAccumulator.value(), aDouble.doubleValue()));
                rowAccumulator.add(1);
                if (rowAccumulator.value() >= numRows) {
                    rowAccumulator.reset();
                    colAccumulator.add(1);
                }

                if (colAccumulator.value() >= numCols) {
                    colAccumulator.reset();
                }
                return arrayList.iterator();
            }
        });
        //VMatRDD.count();
        CoordinateMatrix vMat = new CoordinateMatrix(VMatRDD.rdd());

        System.out.println(vMat.numRows() + "," + vMat.numCols());
        System.out.println(sInvMat.numRows() + "," + sInvMat.numCols());
        System.out.println(UtranMat.numRows() + "," + UtranMat.numCols());
        return vMat.toBlockMatrix().multiply(sInvMat.toBlockMatrix()).multiply(UtranMat.toBlockMatrix()).toCoordinateMatrix();
    }

    public static CoordinateMatrix getCoorMatOption(TopicConstant.MatrixOperation srcOpt, CoordinateMatrix matDividend, CoordinateMatrix matDivisor) {
        TopicConstant.MatrixOperation operation = srcOpt;
        JavaPairRDD<String, Double> tmpDividendRDD = matDividend.entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, String, Double>() {
            public Tuple2<String, Double> call(MatrixEntry entry) throws Exception {
                return new Tuple2<String, Double>(entry.i() + TopicConstant.COMMA_DELIMITER + entry.j(), entry.value());
            }
        });

        JavaPairRDD<String, Double> tmpDivisorRDD = matDivisor.entries().toJavaRDD().mapToPair(new PairFunction<MatrixEntry, String, Double>() {
            public Tuple2<String, Double> call(MatrixEntry entry) throws Exception {
                return new Tuple2<String, Double>(entry.i() + TopicConstant.COMMA_DELIMITER + entry.j(), entry.value());
            }
        });

        JavaPairRDD<String, Tuple2<Double, Optional<Double>>> tmpRDD = tmpDividendRDD.leftOuterJoin(tmpDivisorRDD);

        class Tuple2MatrixEntryFlatMapFunction implements FlatMapFunction<Tuple2<String, Tuple2<Double, Optional<Double>>>, MatrixEntry> {
            private final TopicConstant.MatrixOperation operation;

            public Tuple2MatrixEntryFlatMapFunction(TopicConstant.MatrixOperation operation) {
                this.operation = operation;
            }

            @Override
            public Iterator<MatrixEntry> call(Tuple2<String, Tuple2<Double, Optional<Double>>> stringTuple2Tuple2) throws Exception {
                List<MatrixEntry> entryList = new ArrayList<MatrixEntry>();
                String[] loc = stringTuple2Tuple2._1().split(TopicConstant.COMMA_DELIMITER);
                if (stringTuple2Tuple2._2()._2().isPresent()) {
                    double dividend = stringTuple2Tuple2._2()._1();
                    double divisor = stringTuple2Tuple2._2()._2().get();
                    double value = 0.0;
                    if (operation.equals(TopicConstant.MatrixOperation.Divide)) {
                        value = dividend / divisor;
                    } else if (operation.equals(TopicConstant.MatrixOperation.Mutiply)) {
                        value = dividend * divisor;
                    }
                    entryList.add(new MatrixEntry(Long.parseLong(loc[0]), Long.parseLong(loc[1]), value));
                }
                return entryList.iterator();
            }
        }
        JavaRDD<MatrixEntry> resultRDD = tmpRDD.flatMap(new Tuple2MatrixEntryFlatMapFunction(operation));

        CoordinateMatrix resultMat = new CoordinateMatrix(resultRDD.rdd());
        //System.out.println(resultMat.entries().count());
        //System.out.println(resultMat.numRows());
        //System.out.println(resultMat.numCols());
        resultMat.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
            @Override
            public void call(MatrixEntry matrixEntry) throws Exception {
                System.out.println(matrixEntry.i() + "," + matrixEntry.j() + ":" + matrixEntry.value());
            }
        });
        return resultMat;
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

    public static void transformToVector(Dataset<Row> tweetInfoJavaRDD,String corpusFilePath,String wordVectorFilePath){
        Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("token");
        RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("tweet").setOutputCol("token").setPattern("\\W").setMinTokenLength(3);
        Dataset<Row> wordsData = regexTokenizer.transform(tweetInfoJavaRDD);

        StopWordsRemover remover = new StopWordsRemover().setInputCol("token").setOutputCol("words");
        Dataset<Row> filterData = remover.transform(wordsData);

        //create corpus
        JavaRDD<String> outputCorpusStringRDD = filterData.select("words").toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                List<Object> rowList = v1.getList(0);
                return String.join(TopicConstant.SPACE_DELIMITER,Arrays.copyOf(rowList.toArray(),rowList.size(),String[].class));
            }
        });
        outputCorpusStringRDD.saveAsTextFile(corpusFilePath);

        //create word vector
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("vector")
                .setMinCount(0);

        Word2VecModel model = word2Vec.fit(filterData);
        Dataset<Row> vectorDS = model.transform(filterData);
        model.getVectors().toJavaRDD().saveAsTextFile(wordVectorFilePath);
    }
}
