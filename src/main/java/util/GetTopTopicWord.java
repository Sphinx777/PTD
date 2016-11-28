package util;

import org.apache.spark.api.java.function.Function;

import java.util.*;

/**
 * Created by user on 2016/11/12.
 */
public class GetTopTopicWord implements Function<LinkedHashMap<Integer,Double>,String[]> {
    //private final int intTopWordCnt = 10;
    private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
    public  GetTopTopicWord(HashMap<String,String> srcMap){
        tweetIDMap = srcMap;
    }

    public String[] call(LinkedHashMap<Integer,Double> map) throws Exception {
        ArrayList<String> stringArrayList = new ArrayList<String>();
        for(Map.Entry<Integer,Double> entry:map.entrySet()){
            System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
            if(entry.getKey().equals(null) || entry.getValue().equals(null) || tweetIDMap.containsKey(entry.getKey().toString())==false){
                continue;
            }
            System.out.println("tweet:"+tweetIDMap.get(entry.getKey().toString()));
            stringArrayList.add(tweetIDMap.get(entry.getKey().toString()));

            //stop condition 1--top word limit
    //            if(stringArrayList.size()>=TopicConstant.numTopWords){
    //                break;
    //            }
        }
        System.out.println(stringArrayList.toArray());
        //stop condition 2--copy range of  array
        String[] resultArray = Arrays.copyOfRange(stringArrayList.toArray(new String[stringArrayList.size()]),0,TopicConstant.numTopWords);
        return resultArray;
    }
}
