package util;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.CollectionAccumulator;

import java.util.*;

/**
 * Created by user on 2016/11/12.
 */
public class GetTopTopicWord implements VoidFunction<LinkedHashMap<Integer,Double>> {
    //private final int intTopWordCnt = 10;
    private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
    private int numTopWords;
    private CollectionAccumulator<String[]> tweetWordAccumulator;
    static Logger logger = Logger.getLogger(GetTopTopicWord.class.getName());
    public  GetTopTopicWord(HashMap<String,String> srcMap , int paraNumTopWords , CollectionAccumulator<String[]> paraAccumulator){
        tweetIDMap = srcMap;
        numTopWords = paraNumTopWords;
        tweetWordAccumulator = paraAccumulator;
    }

    public void call(LinkedHashMap<Integer,Double> map) throws Exception {
        ArrayList<String> stringArrayList = new ArrayList<String>();
        //logger.info("content entry length:"+map.entrySet().size());
        //System.out.println("content entry length:"+map.entrySet().size());
        logger.info("Get topic word call function!");
        System.out.println("Get topic word call function!");
        for(Map.Entry<Integer,Double> entry:map.entrySet()){
            if(entry.getKey().equals(null) || entry.getValue().equals(null) || tweetIDMap.containsKey(entry.getKey().toString())==false){
                System.out.println("Get top topic word null");
                logger.info("Get top topic word null");
                continue;
            }
            stringArrayList.add(tweetIDMap.get(entry.getKey().toString()));
        }

        String[] resultArray = Arrays.copyOfRange(stringArrayList.toArray(new String[stringArrayList.size()]),0,numTopWords);
        tweetWordAccumulator.add(resultArray);
        logger.info(String.join(TopicConstant.COMMA_DELIMITER,resultArray));
//        for (String str:resultArray){
//            System.out.println("resultArray:"+str);
//            logger.info("resultArray:"+str);
//        }
    }
}
