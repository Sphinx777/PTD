package util;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.CollectionAccumulator;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;

/**
 * Created by user on 2016/10/21.
 */

//map       topic1--word1:value1;word2:value2;
public class WriteToJSON implements Function<LinkedHashMap<Integer,Double>,String>{
    private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
    private int numTopWords;
    private CollectionAccumulator<String[]> topicWordAccumulator;
    static Logger logger = Logger.getLogger(WriteToJSON.class.getName());

    public  WriteToJSON(HashMap<String,String> srcMap , int paraNumTopWords , CollectionAccumulator<String[]> paraCollectionAccumulator){
        tweetIDMap = srcMap;
        numTopWords = paraNumTopWords;
        topicWordAccumulator = paraCollectionAccumulator;
    }

    public String call(LinkedHashMap<Integer,Double> map) throws Exception {
        String resultStr;
        ObjectMapper mapper = new ObjectMapper();
        LinkedHashMap<String,Double> resultMap = new LinkedHashMap<String,Double>();
        ArrayList<String> stringArrayList = new ArrayList<String>();
        for(Map.Entry<Integer,Double> entry:map.entrySet()){
            //logger.info("key:"+entry.getKey()+",value:"+entry.getValue());
            //System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
            if(entry.getKey().equals(null) || entry.getValue().equals(null) || tweetIDMap.containsKey(entry.getKey().toString())==false){
                continue;
            }

            //logger.info("tweet:"+tweetIDMap.get(entry.getKey().toString()));
            //System.out.println("tweet:"+tweetIDMap.get(entry.getKey().toString()));
            resultMap.put(tweetIDMap.get(entry.getKey().toString()),entry.getValue());
            stringArrayList.add(tweetIDMap.get(entry.getKey().toString()));

            //set the top topic word limit
            if(resultMap.size()>= numTopWords){
                break;
            }
        }
        topicWordAccumulator.add(Arrays.copyOf(stringArrayList.toArray(),stringArrayList.size(),String[].class));
        resultStr = mapper.writeValueAsString(resultMap);

        logger.info("write to json:"+resultStr);
        System.out.println("write to json:"+resultStr);
        return resultStr;
    }
}
