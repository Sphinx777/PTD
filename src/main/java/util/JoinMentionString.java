package util;

import info.debatty.java.stringsimilarity.KShingling;
import info.debatty.java.stringsimilarity.StringProfile;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import vo.TweetInfo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

public class JoinMentionString implements PairFunction<TweetInfo, Object, ArrayList<Object>> {
	//List<Row> tmpMentionList;
    List<TweetInfo> tweetInfos = new ArrayList<TweetInfo>();
	Date currDate;
	private String model;
    static Logger logger = Logger.getLogger(JoinMentionString.class.getName());
	CollectionAccumulator<TweetInfo> collectionAccumulator;
	
	public JoinMentionString(List<TweetInfo> oriTweetAccumulator, Date paraCurrDate , String paraModel){
		//tmpMentionList = strings;
		tweetInfos = oriTweetAccumulator;
		currDate = paraCurrDate;
		model = paraModel;
	}
	
	public Tuple2<Object, ArrayList<Object>> call(TweetInfo tweetData) throws Exception{
		final String splitStrings=tweetData.getUserName();
		final List<String> arr = new ArrayList<String>();
		String tmpString;
		TreeMap<java.lang.Double, Float> treeMap = new TreeMap<>();

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
		        //old
                /*arr.add(tweetData.getTweetId()+TopicConstant.COMMA_DELIMITER+row.getAs("tweetId")+TopicConstant.COMMA_DELIMITER+dbValue);
				treeMap.put(java.lang.Double.valueOf(row.getAs("tweetId").toString()), java.lang.Double.valueOf(dbValue).floatValue());
                logger.info("tweet id:"+java.lang.Double.valueOf(row.getAs("tweetId").toString())+",value:"+java.lang.Double.valueOf(dbValue).floatValue());
                System.out.println("tweet id:"+java.lang.Double.valueOf(row.getAs("tweetId").toString())+",value:"+java.lang.Double.valueOf(dbValue).floatValue());*/
                //new
                //arr.add(tweetData.getTweetId()+TopicConstant.COMMA_DELIMITER+tweetInfo.getTweetId()+TopicConstant.COMMA_DELIMITER+dbValue);
                treeMap.put(Double.valueOf(tweetInfo.getTweetId()), Double.valueOf(dbValue).floatValue());
                logger.info("tweet id:"+java.lang.Double.valueOf(tweetInfo.getTweetId())+",value:"+Double.valueOf(dbValue).floatValue());
                System.out.println("tweet id:"+Double.valueOf(tweetInfo.getTweetId())+",value:"+Double.valueOf(dbValue).floatValue());
            }else{
				treeMap.put(java.lang.Double.valueOf(tweetInfo.getTweetId()),0F);
				//arr.add("X");
			}
		}

		ArrayList<Object> doubles = new ArrayList<>();

		for(Float value:treeMap.values()){
			doubles.add(value);
		}
		//Double[] db = Arrays.copyOfRange(doubles.toArray(new Double[doubles.size()]),0,doubles.size());

		//DenseVector<Object> denseVector = new DenseVector<Object>(db);

		//long , DenseVector<Double> --> Object , DenseVector<Object>
		return new Tuple2<Object, ArrayList<Object>>(Long.valueOf(tweetData.getTweetId()),doubles);
	}
}
