package util;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import topicDerivation.TopicMain;

import java.util.*;

/**
 * Created by user on 2016/11/12.
 */
public class GetTopTopicWord implements Function<LinkedHashMap<Integer,Double>,String[]> {
    //private final int intTopWordCnt = 10;
    private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
    private int numTopWords;
    static Logger logger = Logger.getLogger(GetTopTopicWord.class.getName());
    public  GetTopTopicWord(HashMap<String,String> srcMap , int paraNumTopWords){
        tweetIDMap = srcMap;
        numTopWords = paraNumTopWords;
    }

    public String[] call(LinkedHashMap<Integer,Double> map) throws Exception {
        ArrayList<String> stringArrayList = new ArrayList<String>();
        //logger.info("content entry length:"+map.entrySet().size());
        //System.out.println("content entry length:"+map.entrySet().size());
        for(Map.Entry<Integer,Double> entry:map.entrySet()){
            System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
            logger.info("key:"+entry.getKey()+",value:"+entry.getValue());
            if(entry.getKey().equals(null) || entry.getValue().equals(null) || tweetIDMap.containsKey(entry.getKey().toString())==false){
                System.out.println("Get top topic word null");
                logger.info("Get top topic word null");
                continue;
            }

            logger.info("get topic word:"+tweetIDMap.get(entry.getKey().toString()).toString());
            System.out.println("get topic word:"+tweetIDMap.get(entry.getKey().toString()).toString());
            stringArrayList.add(tweetIDMap.get(entry.getKey().toString()));

            //stop condition 1--top word limit
    //            if(stringArrayList.size()>=TopicConstant.numTopWords){
    //                break;
    //            }
        }
        //System.out.println(stringArrayList.toArray());
        //stop condition 2--copy range of  array
        String[] resultArray = Arrays.copyOfRange(stringArrayList.toArray(new String[stringArrayList.size()]),0,numTopWords);
        for (String str:resultArray){
            System.out.println("resultArray:"+str);
            logger.info("resultArray:"+str);
        }
        return resultArray;
    }
}
