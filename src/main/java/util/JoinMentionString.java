package util;

import breeze.linalg.DenseVector;
import info.debatty.java.stringsimilarity.KShingling;
import info.debatty.java.stringsimilarity.StringProfile;
import it.unimi.dsi.fastutil.doubles.Double2DoubleArrayMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import vo.TweetInfo;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class JoinMentionString implements Function<TweetInfo,Tuple2<Object, DenseVector<Object>>> {
	//List<Row> tmpMentionList;
    ObjectArrayList<TweetInfo> tweetInfos = new ObjectArrayList<TweetInfo>();
	Date currDate;
	private String model;
    static Logger logger = Logger.getLogger(JoinMentionString.class.getName());
	CollectionAccumulator<TweetInfo> collectionAccumulator;

	public JoinMentionString(ObjectArrayList<TweetInfo> oriTweetAccumulator, Date paraCurrDate , String paraModel){
		//tmpMentionList = strings;
		tweetInfos = oriTweetAccumulator;
		currDate = paraCurrDate;
		model = paraModel;
	}

	public Tuple2<Object, DenseVector<Object>> call(TweetInfo tweetData) throws Exception{
		final String splitStrings=tweetData.getUserName();
		final ObjectArrayList<String> arr = new ObjectArrayList<String>();
		String tmpString;
		Double2DoubleArrayMap treeMap = new Double2DoubleArrayMap();

		//compute mention people
        //old
		//for (Row row : tmpMentionList) {
		//logger.info("collectAccumulator length:"+collectionAccumulator.value().size());
        for(TweetInfo tweetInfo:tweetInfos){
			//old
            //tmpString = row.getAs("userName").toString();
			tmpString = tweetInfo.getUserName();
            if(!tmpString.equals(splitStrings)){
				//compute po
				//old
                //String[] arr1= row.getAs("mentionMen").toString().split(",");
				String[] arr1 = tweetInfo.getMentionMen().split(TopicConstant.COMMA_DELIMITER);
                String[] arr2= tweetData.getMentionMen().split(TopicConstant.COMMA_DELIMITER);
				double dbValue = (double)TopicUtil.computeArrayIntersection(arr1, arr2) / TopicUtil.computeArrayUnion(arr1, arr2);
				//if(dbValue > 0){
					//System.out.println();
				//}

				//compute act
				//old
                //String[] arr3= row.getAs("userInteraction").toString().split(",");
                //new
                String[] arr3= tweetInfo.getUserInteraction().split(",");
				String[] arr4= tweetData.getUserInteraction().split(",");
				int intValue = TopicUtil.computeArrayIntersection(arr3, arr4);
				if(intValue >0 ){
					dbValue +=1;
				}else {
					dbValue +=0;
				}

				//compute cosine similarity min sentence size
				KShingling ks = new KShingling(2);
				StringProfile pro1 = ks.getProfile(tweetData.getTweet());
				//old
                //StringProfile pro2 = ks.getProfile(row.getAs("tweet").toString());
                StringProfile pro2 = ks.getProfile(tweetInfo.getTweet());
                double dbCosValue = pro1.cosineSimilarity(pro2);
		        dbValue += dbCosValue;

				//weighted by time-factor
				if(model.trim().toUpperCase().equals("DTTD")) {
					//old
                    //dbValue *= TopicUtil.getWeightedValue(tweetData.getDateString(), row.getAs("dateString").toString(), currDate);
                    //new
                    dbValue *= TopicUtil.getWeightedValue(tweetData.getDateString(), tweetInfo.getDateString(), currDate);
				}

		        //passing the sigmoid

		        dbValue = TopicUtil.calculateSigmoid(dbValue);
                treeMap.put(Double.valueOf(tweetInfo.getTweetId()).doubleValue(), Double.valueOf(dbValue).doubleValue());
                logger.info("tweet id:"+ Double.valueOf(tweetInfo.getTweetId())+",value:"+Double.valueOf(dbValue).doubleValue());
                System.out.println("tweet id:"+Double.valueOf(tweetInfo.getTweetId())+",value:"+Double.valueOf(dbValue).doubleValue());
            }else{
				treeMap.put(Double.valueOf(tweetInfo.getTweetId()).doubleValue(),0.0);
			}
		}

		double [] doublesArray = new double[treeMap.values().toArray().length];
		for(int i=0;i<treeMap.values().toArray().length;i++){
			doublesArray[i] = ((Double) treeMap.values().toArray()[i]);
		}

		List<double[]> list = Arrays.asList(doublesArray);
		DenseVector<Object> denseVector = new DenseVector<Object>(list.toArray()[0]);

		return new Tuple2<Object, DenseVector<Object>>(Long.valueOf(tweetData.getTweetId()),denseVector);
	}
}
