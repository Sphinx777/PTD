package util.tfidf;

import breeze.linalg.DenseVector;
import edu.nju.pasalab.marlin.matrix.CoordinateMatrix;
import edu.nju.pasalab.marlin.matrix.DenseVecMatrix;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import util.TopicConstant;

import java.io.Serializable;
import java.util.*;

public class TFIDF implements Serializable{
	static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(TFIDF.class.getName());

	private Dataset<Row> tweetDataset;
	private Dataset<Row> tfidfDataSet;
	private DenseVecMatrix tfidfDVM;
	private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
	public Dataset<Row> getTfidfDataSet() {
		return tfidfDataSet;
	}

	private static Broadcast<HashMap<String,String>> brTweetIDMap;

	private static Broadcast<HashMap<String,String>> brTweetWordMap;

	private static JavaSparkContext sparkContext;

	private static CollectionAccumulator<String> tweetAccumulator;

	public TFIDF(Dataset<Row> rdd , JavaSparkContext paraSparkContext ,
				CollectionAccumulator<String> paraTweetAccumulator){
		tweetDataset = rdd;
        sparkContext = paraSparkContext;
		tweetAccumulator = paraTweetAccumulator;
	}
	
	//tweet:original message
	//words:the tokened tweet
	//rawFeatures:the token id and the token times
	//features: the tfidf value
	public void buildModel(){
		Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("token");
		RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol("tweet").setOutputCol("token").setPattern("\\W").setMinTokenLength(3);
		//tokenizer version
		//Dataset<Row> wordsData = tokenizer.transform(tweetDataset);
		//regexTokenizer version

		Dataset<Row> wordsData = regexTokenizer.transform(tweetDataset);

		StopWordsRemover remover = new StopWordsRemover().setInputCol("token").setOutputCol("words");
		Dataset<Row> filterData = remover.transform(wordsData);

		//get wordAccumulator
		filterData.select("words").toJavaRDD().foreach(new filterRowFunction(tweetAccumulator));

		org.apache.spark.mllib.feature.HashingTF tf = new org.apache.spark.mllib.feature.HashingTF();
		//tfidf for ml start

		JavaRDD<List<String>> listJavaRDD = filterData.select("words").toJavaRDD().map(new Function<Row, List<String>>() {
			@Override
			public List<String> call(Row v1) throws Exception {
				List<String> stringList = v1.getList(0);
				return stringList;
			}
		});

		HashSet<String> tweetHashSet = new HashSet<String>(tweetAccumulator.value());

		JavaRDD<org.apache.spark.mllib.linalg.Vector> termFreqs = tf.transform(listJavaRDD);
		long idxCnt = 0;
		logger.info("tweetHashSet.count:"+tweetHashSet.size());
        HashMap<String,String> tmpTweetIDMap = new HashMap<>();
        HashMap<String,String> tweetWordMap = new HashMap<>();
		for(Object obj:tweetHashSet.toArray()){
			System.out.println("index:"+tf.indexOf(obj.toString())+",word:"+obj.toString());
			logger.info("index:"+tf.indexOf(obj.toString())+",word:"+obj.toString());
            tmpTweetIDMap.put(String.valueOf(tf.indexOf(obj.toString())), obj.toString()); //ok to broadcast
			if(!tweetWordMap.containsValue(obj.toString())){
				idxCnt = Long.valueOf(tweetWordMap.size());
                tweetWordMap.put(String.valueOf(idxCnt) , obj.toString());
			}
		}

		brTweetIDMap = sparkContext.broadcast(tmpTweetIDMap);
        brTweetWordMap = sparkContext.broadcast(tweetWordMap);

		org.apache.spark.mllib.feature.IDF idfMllib = new org.apache.spark.mllib.feature.IDF();
		JavaRDD<org.apache.spark.mllib.linalg.Vector> tfIdfs = idfMllib.fit(termFreqs).transform(termFreqs);

		JavaPairRDD<org.apache.spark.mllib.linalg.Vector,Long> tfidfPairRDD = tfIdfs.zipWithIndex();
		/*tfidfPairRDD.foreach(new VoidFunction<Tuple2<org.apache.spark.mllib.linalg.Vector, Long>>() {
			@Override
			public void call(Tuple2<Vector, Long> vectorLongTuple2) throws Exception {
                System.out.println("vectorLongTuple2 for collect:"+vectorLongTuple2._1()+","+vectorLongTuple2._2());
			}
		});*/
		//tfidfPairRDD.collect();
		//JavaRDD<MatrixEntry> entryJavaRDD = tfidfPairRDD.flatMap(new Tuple2MatrixEntryFlatMapFunction(brTweetIDMap,brTweetWordMap));

        //coordinateMatrix version
		//JavaRDD<Tuple2<Tuple2<Object,Object>,Object>> entryJavaRDD = tfidfPairRDD.flatMap(new Tuple2MatrixEntryFlatMapFunction(brTweetIDMap,brTweetWordMap));
        //denseVecMatrix version
        JavaRDD<Tuple2<Object,DenseVector<Object>>> entryJavaRDD = tfidfPairRDD.map(new Tuple2MatrixEntryFlatMapFunction(brTweetIDMap,brTweetWordMap));

//		JavaRDD<MatrixEntry> entryJavaRDD = tfIdfs.flatMap(new FlatMapFunction<org.apache.spark.mllib.linalg.Vector, MatrixEntry>() {
//															   @Override
//															   public Iterator<MatrixEntry> call(org.apache.spark.mllib.linalg.Vector vector) throws Exception {
//																   List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//																   long matrixIdx=0;
//																   tweetAccumulator.add(1);
//																   for (int i : vector.toSparse().indices()) {
//																	   if(tweetWordMapBroadCast.getValue().containsValue(tweetIDMapBroadCast.getValue().get(String.valueOf(i)))==false){
//																		   matrixIdx = Long.valueOf(tweetWordMapBroadCast.getValue().size()+1);
//																		   tweetWordMapBroadCast.getValue().put(String.valueOf(matrixIdx) , tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//																	   }else{
//																		   Iterator<Map.Entry<String,String>> iter =tweetWordMapBroadCast.getValue().entrySet().iterator();
//																		   while (iter.hasNext()){
//																			   Map.Entry<String,String> entry = iter.next();
//																			   if(entry.getValue().equals(tweetIDMapBroadCast.getValue().get(String.valueOf(i)))){
//																				   matrixIdx = Long.valueOf(entry.getKey());
//																				   break;
//																			   }
//																		   }
//																	   }
//																	   arrayList.add(new MatrixEntry(Double.valueOf(tweetAccumulator.value().toString()).longValue(), matrixIdx, vector.toArray()[i]));
//																   }
//																   return arrayList.iterator();
//															   }
//														   });
		tweetIDMap = brTweetWordMap.getValue();

		/*for(Map.Entry<String,String> entry:tweetIDMap.entrySet()){
			System.out.println("tweetIDMap["+entry.getKey()+"]:"+entry.getValue());
			logger.info("tweetIDMap["+entry.getKey()+"]:"+entry.getValue());
		}*/
		//entryJavaRDD.collect();

		//tfidf for ml end

		//tfidf for mllib start

//		HashingTF hashingTF = new HashingTF()
//								  .setInputCol("words")
//								  .setOutputCol("rawFeatures")
//								  .setNumFeatures(TopicConstant.numFeatures);
//
//		Dataset<Row> featurizedData = hashingTF.transform(filterData);
//
//		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
//		IDFModel idfModel = idf.fit(featurizedData);
//		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
//
//		rescaledData.select("tweetId","words","features","rawFeatures").toJavaRDD().foreach(new RowVoidFunction(tweetIDMapBroadCast));
//
//
//		tfidfDataSet = rescaledData;
//
//		JavaRDD<MatrixEntry> entryJavaRDD = tfidfDataSet.toJavaRDD().flatMap((FlatMapFunction<Row,MatrixEntry>) row ->{
//			List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//			String id = row.getAs("tweetId");
//			Vector feature = row.getAs("features");
//			for(int i : feature.toSparse().indices()){
//				arrayList.add(new MatrixEntry(Double.valueOf(id).longValue(),(long)i,feature.toArray()[i]));
//			}
//			return arrayList.iterator();
//		});
//		entryJavaRDD.count();

		//tfidf for mllib end

		//print the element of ml tfidf
//		for(Row row:rescaledData.select("tweetId","words","features","rawFeatures").collectAsList()){
//			Vector feature = row.getAs("features");
//			String id = row.getAs("tweetId");
//			//sparseVector
//			System.out.println("feature class:"+row.getAs("features").getClass().getName());
//			//wrappedArray
//			System.out.println("words class:"+row.getAs("words").getClass().getName());
//			System.out.println("id:"+id+",words:"+row.getAs("words")+",feature:"+feature);
//			LinkedList<Object> list = new LinkedList<Object>(row.getList(1));
//			for(int idx:feature.toSparse().indices()){
//				System.out.println("id:"+idx+" , value:"+feature.toArray()[idx]);
//				String wrd = list.poll().toString();
//				tweetIDMap.put(String.valueOf(idx),wrd);
//			}
//		}

		//print the tfidf value array
//		JavaRDD<MatrixEntry> entryJavaRDD = tfidfDataSet.toJavaRDD().flatMap(new FlatMapFunction<Row, MatrixEntry>() {
//			@Override
//			public Iterator<MatrixEntry> call(Row row) throws Exception {
//				List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//				Vector feature = row.getAs("features");
//				for(int i=0;i<feature.toArray().length;i++){
//					System.out.println("i:"+i+":"+feature.toArray()[i]);
//				}
//				return arrayList.iterator();
//			}
//		});

		//print the matrixEntry array
//		tfidfDVM.entries().toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {
//			@Override
//			public void call(MatrixEntry matrixEntry) throws Exception {
//				System.out.println(matrixEntry.i()+","+matrixEntry.j()+":"+matrixEntry.value());
//			}
//		});
		//coordinateMatrix version
        //CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entryJavaRDD.rdd());
		//tfidfDVM = coordinateMatrix.toDenseVecMatrix();
		tfidfDVM = new DenseVecMatrix(entryJavaRDD.rdd());
	}
	//x:tweet Id , y:term id(features--vector) , value:tfidf(features--vector)
	public DenseVecMatrix getTfidfDVM(){
		return tfidfDVM;
	}
	public HashMap<String, String> getTweetIDMap() {
		return tweetIDMap;
	}

//	private class RowVoidFunction implements VoidFunction<Row> {
//		private Broadcast<HashMap<String,String>> tweetIDMap;
//
//		public RowVoidFunction(Broadcast<HashMap<String,String>> srcMap) {
//			tweetIDMap = srcMap;
//		}
//
//		@Override
//        public void call(Row row) throws Exception {
//            String id = row.getAs("tweetId");
//            Vector feature = row.getAs("features");
//            LinkedList<Object> list = new LinkedList<Object>(row.getList(1));
//            for(int idx:feature.toSparse().indices()){
//                System.out.println("id:"+idx+" , value:"+feature.toArray()[idx]);
//                String wrd = list.poll().toString();
//                tweetIDMap.getValue().put(String.valueOf(idx),wrd);
//            }
//        }
//	}

	private class filterRowFunction implements VoidFunction<Row>{
		private CollectionAccumulator<String> wordAccumulator;

		public filterRowFunction(CollectionAccumulator<String> paraAccu){
			wordAccumulator = paraAccu;
		}

		@Override
        public void call(Row row) throws Exception {
			List<Object> rowList = row.getList(0);
			for(Object obj:rowList){
				String wrd = obj.toString();
				wordAccumulator.add(wrd);
			}
		}
	}

	//coordinateMatrix version
    private class Tuple2MatrixEntryFlatMapFunction implements Function<Tuple2<Vector, Long>, Tuple2<Object, DenseVector<Object>>> {
        private Broadcast<HashMap<String,String>> tweetIDMapBroadCast;
        private Broadcast<HashMap<String,String>> tweetWordMapBroadCast;

        public Tuple2MatrixEntryFlatMapFunction(Broadcast<HashMap<String,String>> paraTweetIDMap , Broadcast<HashMap<String,String>> paraTweetWordMap){
            tweetIDMapBroadCast = paraTweetIDMap;
            tweetWordMapBroadCast = paraTweetWordMap;
        }

        //denseVecMatrix
        @Override
		public Tuple2<Object, DenseVector<Object>> call(Tuple2<Vector,Long> vectorLongTuple2)throws Exception {
			long matrixIdx = 0;
			ArrayList<Object> tuple2s = new ArrayList<Object>();
			double[] doubles = new double[tweetWordMapBroadCast.value().size()];
			for (int i : vectorLongTuple2._1().toSparse().indices()) {
				if (tweetWordMapBroadCast.getValue().containsValue(tweetIDMapBroadCast.getValue().get(String.valueOf(i))) == false) {
					System.out.println("tweetWordMapBroadCast contains no value error!");
					logger.info("tweetWordMapBroadCast contains no value error!");
				} else {
					Iterator<Map.Entry<String, String>> iter = tweetWordMapBroadCast.getValue().entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry<String, String> entry = iter.next();
						if (entry.getValue().equals(tweetIDMapBroadCast.getValue().get(String.valueOf(i)))) {
							matrixIdx = Long.valueOf(entry.getKey());
							//format matrixIdx+ ";" + doubleValue
							tuple2s.add(matrixIdx+ TopicConstant.SEMICOLON_DELIMITER+Double.valueOf(vectorLongTuple2._1().toArray()[i]).doubleValue());
							break;
						}
					}
				}
			}
			String[] splitStr;
			for(Object object:tuple2s.toArray()){
				splitStr = ((String) object).split(TopicConstant.SEMICOLON_DELIMITER);
				doubles[Integer.parseInt(splitStr[0])] = Double.valueOf(splitStr[1]).doubleValue();
			}
			List<double[]> list = Arrays.asList(doubles);
			DenseVector<Object> denseVector = new DenseVector<Object>(list.toArray()[0]);
			return new Tuple2<Object, DenseVector<Object>>(vectorLongTuple2._2().longValue(),denseVector);
		}


//coordinateMatrix version
//        @Override
//		public Iterator<Tuple2<Tuple2<Object, Object>, Object>> call(Tuple2<Vector,Long> vectorLongTuple2)throws Exception {
//			TreeMap<Long, Double> wordMap = new TreeMap<>();
//			long matrixIdx = 0;
//			ArrayList<Tuple2<Tuple2<Object,Object>,Object>> tuple2s = new ArrayList<Tuple2<Tuple2<Object, Object>, Object>>();
//			Tuple2<Object,Object> locTuple;
//			for (int i : vectorLongTuple2._1().toSparse().indices()) {
//				//logger.info("tweetIDMapBroadCast.getValue().entrySet().size():" + tweetIDMapBroadCast.getValue().entrySet().size());
//				//logger.info("tweetWordMapBroadCast.getValue().entrySet().size():" + tweetWordMapBroadCast.getValue().entrySet().size());
//				//logger.info("tweetIDMapBroadCast.getValue().get(String.valueOf(i)):" + tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//
//				//System.out.println("tweetIDMapBroadCast.getValue().entrySet().size():" + tweetIDMapBroadCast.getValue().entrySet().size());
//				//System.out.println("tweetWordMapBroadCast.getValue().entrySet().size():" + tweetWordMapBroadCast.getValue().entrySet().size());
//				//System.out.println("tweetIDMapBroadCast.getValue().get(String.valueOf(i)):" + tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//				if (tweetWordMapBroadCast.getValue().containsValue(tweetIDMapBroadCast.getValue().get(String.valueOf(i))) == false) {
//					System.out.println("tweetWordMapBroadCast contains no value error!");
//					logger.info("tweetWordMapBroadCast contains no value error!");
//					//matrixIdx = Long.valueOf(tweetWordMapBroadCast.getValue().size()+1);
//					//tweetWordMapBroadCast.getValue().put(String.valueOf(matrixIdx) , tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//				} else {
//					Iterator<Map.Entry<String, String>> iter = tweetWordMapBroadCast.getValue().entrySet().iterator();
//					while (iter.hasNext()) {
//						Map.Entry<String, String> entry = iter.next();
//						if (entry.getValue().equals(tweetIDMapBroadCast.getValue().get(String.valueOf(i)))) {
//							matrixIdx = Long.valueOf(entry.getKey());
//							locTuple = new Tuple2<Object, Object>(vectorLongTuple2._2().longValue(),matrixIdx);
//							tuple2s.add(new Tuple2<Tuple2<Object, Object>,Object>(locTuple, Double.valueOf(vectorLongTuple2._1().toArray()[i]).floatValue()));
//							break;
//						}
//					}
//				}
//			}
//			return tuple2s.iterator();
//		}


//        @Override
//        public Iterator<MatrixEntry> call(Tuple2<Vector, Long> vectorLongTuple2) throws Exception {
//            List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
//            long matrixIdx = 0;
//            logger.info("vectorLongTuple2:" + vectorLongTuple2._1() + "," + vectorLongTuple2._2());
//            System.out.println("vectorLongTuple2:" + vectorLongTuple2._1() + "," + vectorLongTuple2._2());
//            for (int i : vectorLongTuple2._1().toSparse().indices()) {
//                logger.info("tweetIDMapBroadCast.getValue().entrySet().size():" + tweetIDMapBroadCast.getValue().entrySet().size());
//                logger.info("tweetWordMapBroadCast.getValue().entrySet().size():" + tweetWordMapBroadCast.getValue().entrySet().size());
//                logger.info("tweetIDMapBroadCast.getValue().get(String.valueOf(i)):" + tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//
//                System.out.println("tweetIDMapBroadCast.getValue().entrySet().size():" + tweetIDMapBroadCast.getValue().entrySet().size());
//                System.out.println("tweetWordMapBroadCast.getValue().entrySet().size():" + tweetWordMapBroadCast.getValue().entrySet().size());
//                System.out.println("tweetIDMapBroadCast.getValue().get(String.valueOf(i)):" + tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//                if (tweetWordMapBroadCast.getValue().containsValue(tweetIDMapBroadCast.getValue().get(String.valueOf(i))) == false) {
//                    System.out.println("tweetWordMapBroadCast contains no value error!");
//                    logger.info("tweetWordMapBroadCast contains no value error!");
//                    //matrixIdx = Long.valueOf(tweetWordMapBroadCast.getValue().size()+1);
//                    //tweetWordMapBroadCast.getValue().put(String.valueOf(matrixIdx) , tweetIDMapBroadCast.getValue().get(String.valueOf(i)));
//                } else {
//                    Iterator<Map.Entry<String, String>> iter = tweetWordMapBroadCast.getValue().entrySet().iterator();
//                    while (iter.hasNext()) {
//                        Map.Entry<String, String> entry = iter.next();
//                        if (entry.getValue().equals(tweetIDMapBroadCast.getValue().get(String.valueOf(i)))) {
//                            matrixIdx = Long.valueOf(entry.getKey());
//                            break;
//                        }
//                    }
//                }
//                logger.info("arrayList.add[" + (vectorLongTuple2._2().longValue() + 1) + "," + matrixIdx + "]:" + vectorLongTuple2._1().toArray()[i]);
//                System.out.println("arrayList.add[" + (vectorLongTuple2._2().longValue() + 1) + "," + matrixIdx + "]:" + vectorLongTuple2._1().toArray()[i]);
//                arrayList.add(new MatrixEntry(vectorLongTuple2._2().longValue() + 1, matrixIdx, vectorLongTuple2._1().toArray()[i]));
//            }
//            return arrayList.iterator();
//        }
    }
}
