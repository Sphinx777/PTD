package util;

import it.unimi.dsi.fastutil.ints.Int2DoubleArrayMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.CollectionAccumulator;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by user on 2016/11/12.
 */
public class GetTopTopicWord implements VoidFunction<Int2DoubleArrayMap> {
    //private final int intTopWordCnt = 10;
    private Object2ObjectOpenHashMap<String,String> tweetIDMap = new Object2ObjectOpenHashMap<String,String>();
    private int numTopWords;
    private CollectionAccumulator<String[]> tweetWordAccumulator;
    static Logger logger = Logger.getLogger(GetTopTopicWord.class.getName());
    public  GetTopTopicWord(Object2ObjectOpenHashMap<String,String> srcMap , int paraNumTopWords , CollectionAccumulator<String[]> paraAccumulator){
        tweetIDMap = srcMap;
        numTopWords = paraNumTopWords;
        tweetWordAccumulator = paraAccumulator;
    }

    public void call(Int2DoubleArrayMap map) throws Exception {
        ObjectArrayList<String> stringArrayList = new ObjectArrayList<String>();
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
