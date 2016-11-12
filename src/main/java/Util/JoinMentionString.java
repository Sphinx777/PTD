package Util;

import VO.tweetInfo;
import info.debatty.java.stringsimilarity.KShingling;
import info.debatty.java.stringsimilarity.StringProfile;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class JoinMentionString implements Function<tweetInfo, String>{
	List<Row> tmpMentionList;
	//Word2Vec word2Vec;
	
	public JoinMentionString(List<Row> strings){
		tmpMentionList = strings;
		//word2Vec = new Word2Vec.Builder().build();
	}
	
	public String call(tweetInfo tweetData) throws Exception{
		final String splitStrings=tweetData.getUserName();
		final List<String> arr = new ArrayList<String>();
		String tmpString;
		
		//compute mention people
		for (Row row : tmpMentionList) {
			//.substring(1, row.getAs("mentionMen").toString().length()-1)
			tmpString = row.getAs("userName").toString();
			if(!tmpString.equals(splitStrings)){
				//compute po
				String[] arr1= row.getAs("mentionMen").toString().split(",");
				String[] arr2= tweetData.getMentionMen().split(",");
				double dbValue = (double)TopicUtil.computeArrayIntersection(arr1, arr2) / TopicUtil.computeArrayUnion(arr1, arr2); 
				if(dbValue > 0){
					//System.out.println();
				}
				
				//compute act
				String[] arr3= row.getAs("userInteraction").toString().split(",");
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
				StringProfile pro2 = ks.getProfile(row.getAs("tweet").toString());
		        double dbCosValue = pro1.cosineSimilarity(pro2);
		        dbValue += dbCosValue;

				//weighted by time-factor
				dbValue *= TopicUtil.getWeightedValue(tweetData.getDateString(),row.getAs("dateString").toString());

		        //passing the sigmoid
		        dbValue = TopicUtil.calculateSigmoid(dbValue);
		        arr.add(tweetData.getTweetId()+TopicConstant.COMMA_DELIMITER+row.getAs("tweetId")+TopicConstant.COMMA_DELIMITER+dbValue);
			}else{
				arr.add("X");
			}
		}
		
		//compute user interaction
		/*for(Row row:tmpMentionList){
			tmpString = row.getAs("userName").toString();
			if(!tmpString.equals(splitStrings)){
				String[] arr1= row.getAs("userInteraction").toString().split(",");
				String[] arr2= tweetData.getUserInteraction().split(",");
				int intValue = TopicUtil.computeArrayIntersection(arr1, arr2);
				if(intValue >0 ){
					arr.add("1");
				}else {
					arr.add("0");
				}
			}else{
				arr.add("0");
			}
		}*/
	
		//compute cosine similarity
		/*for(Row row:tmpMentionList){
			tmpString = row.getAs("userName").toString();
			if(!tmpString.equals(splitStrings)){
				//min sentence size
				KShingling ks = new KShingling(2);
				
				StringProfile pro1 = ks.getProfile(tweetData.getTweet());
				StringProfile pro2 = ks.getProfile(row.getAs("tweet").toString());
				double dbValue = pro1.cosineSimilarity(pro2);
				arr.add(String.valueOf(dbValue));
			}else{
				arr.add("0");
			}
		}*/
		
		//double[] db = Doubles.toArray(arr);
		//Vector dv = Vectors.dense(db);
		//return dv;
		return String.join(TopicConstant.SEMICOLON_DELIMITER, arr);
	}
}
