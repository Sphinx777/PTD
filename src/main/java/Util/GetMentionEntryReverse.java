package Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

public class GetMentionEntryReverse implements FlatMapFunction<String,MatrixEntry>{
	public Iterator<MatrixEntry> call(String paraStr){
		String[] splitStrings = paraStr.split(TopicConstant.SEMICOLON_DELIMITER);
		List<MatrixEntry> arrayList = new ArrayList<MatrixEntry>();
		for(String str:splitStrings){
			if(str.indexOf(TopicConstant.COMMA_DELIMITER)==-1){
				continue;
			}
			String[] splitStrings2 = str.split(TopicConstant.COMMA_DELIMITER);
			arrayList.add(new MatrixEntry(Long.parseLong(splitStrings2[1]) ,Long.parseLong(splitStrings2[0]), Double.parseDouble(splitStrings2[2])));
			//arrayList.add(splitStrings2[0]+TopicConstant.COMMA_DELIMITER+splitStrings2[1]+TopicConstant.COMMA_DELIMITER+splitStrings2[2]);
		}
		return arrayList.iterator();
	}
}
