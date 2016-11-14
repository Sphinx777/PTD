package util;

import org.apache.spark.api.java.function.Function;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;

/**
 * Created by user on 2016/10/21.
 */

//map       topic1--word1:value1;word2:value2;
public class WriteToJSON implements Function<LinkedHashMap<Integer,Double>,String>{
    private HashMap<String,String> tweetIDMap = new HashMap<String,String>();

    public  WriteToJSON(HashMap<String,String> srcMap){
        tweetIDMap = srcMap;
    }

    public String call(LinkedHashMap<Integer,Double> map) throws Exception {
        String resultStr;
        ObjectMapper mapper = new ObjectMapper();
        LinkedHashMap<String,Double> resultMap = new LinkedHashMap<String,Double>();
        for(Map.Entry<Integer,Double> entry:map.entrySet()){
            System.out.println("key:"+entry.getKey()+",value:"+entry.getValue());
            if(entry.getKey().equals(null) || entry.getValue().equals(null) || tweetIDMap.containsKey(entry.getKey().toString())==false){
                continue;
            }
            System.out.println("tweet:"+tweetIDMap.get(entry.getKey().toString()));
            resultMap.put(tweetIDMap.get(entry.getKey().toString()),entry.getValue());
        }
        resultStr = mapper.writeValueAsString(resultMap);

        System.out.println(resultStr);
        return resultStr;
    }
}
