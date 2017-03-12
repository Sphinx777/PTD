package util;

import breeze.linalg.DenseVector;
import info.debatty.java.stringsimilarity.KShingling;
import info.debatty.java.stringsimilarity.StringProfile;
import it.unimi.dsi.fastutil.longs.Long2DoubleArrayMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import vo.TweetInfo;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class JoinMentionStringForPartition implements FlatMapFunction<Iterator<TweetInfo>,Tuple2<Object, DenseVector<Object>>> {
	//List<Row> tmpMentionList;
    ObjectArrayList<TweetInfo> tweetInfos = new ObjectArrayList<TweetInfo>();
	Date currDate;
	private String model;
    static Logger logger = Logger.getLogger(JoinMentionStringForPartition.class.getName());
	CollectionAccumulator<TweetInfo> collectionAccumulator;

	public JoinMentionStringForPartition(ObjectArrayList<TweetInfo> oriTweetAccumulator, Date paraCurrDate , String paraModel){
		//tmpMentionList = strings;
		tweetInfos = oriTweetAccumulator;
		currDate = paraCurrDate;
		model = paraModel;
	}

	public Iterator<Tuple2<Object, DenseVector<Object>>> call(Iterator<TweetInfo> tweetInfoIterator) throws Exception{
		List<Tuple2<Object, DenseVector<Object>>> denseVecList = new ArrayList<>();
		while(tweetInfoIterator.hasNext()){
			TweetInfo tweetData = tweetInfoIterator.next();
			final String splitStrings = tweetData.getUserName();
			String tmpString;
			Long2DoubleArrayMap treeMap = new Long2DoubleArrayMap();

			//compute mention people
			//old
			//for (Row row : tmpMentionList) {
			//logger.info("collectAccumulator length:"+collectionAccumulator.value().size());
			for (int i = (int) tweetData.getTweetId() + 1; i < tweetInfos.size(); i++) {
				TweetInfo tweetInfo = tweetInfos.get(i);
				//for(TweetInfo tweetInfo:tweetInfos){
				//old
				//tmpString = row.getAs("userName").toString();
				tmpString = tweetInfo.getUserName();
				if (!tmpString.equals(splitStrings)) {
					//compute po
					//old
					//String[] arr1= row.getAs("mentionMen").toString().split(",");
					//logger.info("compute mention men start");
					String[] arr1 = tweetInfo.getMentionMen().split(TopicConstant.COMMA_DELIMITER);
					String[] arr2 = tweetData.getMentionMen().split(TopicConstant.COMMA_DELIMITER);
					double dbValue = (double) TopicUtil.computeArrayIntersection(arr1, arr2) / TopicUtil.computeArrayUnion(arr1, arr2);
					//logger.info("compute mention men finish");
					//if(dbValue > 0){
					//System.out.println();
					//}

					//compute act
					//old
					//String[] arr3= row.getAs("userInteraction").toString().split(",");
					//new
					//logger.info("compute user interaction start");
					String[] arr3 = tweetInfo.getUserInteraction().split(TopicConstant.COMMA_DELIMITER);
					String[] arr4 = tweetData.getUserInteraction().split(TopicConstant.COMMA_DELIMITER);
					int intValue = TopicUtil.computeArrayIntersection(arr3, arr4);
					if (intValue > 0) {
						dbValue += 1;
					} else {
						dbValue += 0;
					}
					//logger.info("compute user interaction finish");

					//compute cosine similarity min sentence size
					//logger.info("compute cosine similarity start");
					KShingling ks = new KShingling(2);
					StringProfile pro1 = ks.getProfile(tweetData.getTweet());
					//old
					//StringProfile pro2 = ks.getProfile(row.getAs("tweet").toString());
					StringProfile pro2 = ks.getProfile(tweetInfo.getTweet());
					double dbCosValue = pro1.cosineSimilarity(pro2);
					dbValue += dbCosValue;
					//logger.info("compute cosine similarity finish");

					//weighted by time-factor
					if (model.trim().toUpperCase().equals("DTTD")) {
						//old
						//dbValue *= TopicUtil.getWeightedValue(tweetData.getDateString(), row.getAs("dateString").toString(), currDate);
						//new
						dbValue *= TopicUtil.getWeightedValue(tweetData.getDateString(), tweetInfo.getDateString(), currDate);
					}

					//passing the sigmoid

					//logger.info("compute sigmoid start");
					dbValue = TopicUtil.calculateSigmoid(dbValue);
					//logger.info("compute sigmoid start");
					treeMap.put(tweetInfo.getTweetId(), dbValue);

					//logger.info("tweet id:"+ Double.valueOf(tweetInfo.getTweetId())+",value:"+Double.valueOf(dbValue).doubleValue());
					//System.out.println("tweet id:"+Double.valueOf(tweetInfo.getTweetId())+",value:"+Double.valueOf(dbValue).doubleValue());
				} else {
					treeMap.put(tweetInfo.getTweetId(), 0.0);
				}
			}

            //logger.info("current compute mention tweet id:" + tweetData.getTweetId());
            System.out.println("current compute mention tweet id:" + tweetData.getTweetId());

			double[] doubles = new double[tweetInfos.size()];

			//double[] doubles = Arrays.copyOf(treeMap.values().toDoubleArray(),tweetInfos.size());

			System.arraycopy(treeMap.values().toDoubleArray(), 0, doubles, (int) tweetData.getTweetId() + 1, treeMap.size());

			DenseVector<Object> denseVector = new DenseVector<Object>(doubles);

			denseVecList.add(new Tuple2<Object, DenseVector<Object>>(Long.valueOf(tweetData.getTweetId()),denseVector));
		}
		return denseVecList.iterator();
	}
}
