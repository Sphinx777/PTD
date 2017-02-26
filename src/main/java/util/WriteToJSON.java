package util;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;

/**
 * Created by user on 2016/10/21.
 */

//map       topic1--word1:value1;word2:value2;
public class WriteToJSON implements Function<LinkedHashMap<Integer,Double>,String>{
    private HashMap<String,String> tweetIDMap = new HashMap<String,String>();
    private int numTopWords;
    static Logger logger = Logger.getLogger(WriteToJSON.class.getName());

    public  WriteToJSON(HashMap<String,String> srcMap , int paraNumTopWords){
        tweetIDMap = srcMap;
        numTopWords = paraNumTopWords;
    }

    public String call(LinkedHashMap<Integer,Double> map) throws Exception {
        String resultStr;
        ObjectMapper mapper = new ObjectMapper();
        LinkedHashMap<String,Double> resultMap = new LinkedHashMap<String,Double>();
        for(Map.Entry<Integer,Double> entry:map.entrySet()){
            //logger.info("key:"+entry.getKey()+",value:"+entry.getValue());
            //System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
            if(entry.getKey().equals(null) || entry.getValue().equals(null) || tweetIDMap.containsKey(entry.getKey().toString())==false){
                continue;
            }

            //logger.info("tweet:"+tweetIDMap.get(entry.getKey().toString()));
            //System.out.println("tweet:"+tweetIDMap.get(entry.getKey().toString()));
            resultMap.put(tweetIDMap.get(entry.getKey().toString()),entry.getValue());

            //set the top topic word limit
            if(resultMap.size()>= numTopWords){
                break;
            }
        }
        resultStr = mapper.writeValueAsString(resultMap);

        logger.info("write to json:"+resultStr);
        System.out.println("write to json:"+resultStr);
        return resultStr;
    }
}
