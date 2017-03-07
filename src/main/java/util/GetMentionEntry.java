package util;

import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.util.Iterator;

public class GetMentionEntry implements FlatMapFunction<String,MatrixEntry>{
	public Iterator<MatrixEntry> call(String paraStr){
		String[] splitStrings = paraStr.split(TopicConstant.SEMICOLON_DELIMITER);
		ObjectArrayList<MatrixEntry> arrayList = new ObjectArrayList<MatrixEntry>();
		for(String str:splitStrings){
			if(str.indexOf(TopicConstant.COMMA_DELIMITER)==-1){
				continue;
			}
			String[] splitStrings2 = str.split(TopicConstant.COMMA_DELIMITER);

			arrayList.add(new MatrixEntry(Double.valueOf(splitStrings2[0]).longValue() ,Double.valueOf(splitStrings2[1]).longValue() , Double.parseDouble(splitStrings2[2])));
			//arrayList.add(new MatrixEntry(Long.parseLong(splitStrings2[0]) ,Long.parseLong(splitStrings2[1]) , Double.parseDouble(splitStrings2[2])));
			//arrayList.add(splitStrings2[0]+TopicConstant.COMMA_DELIMITER+splitStrings2[1]+TopicConstant.COMMA_DELIMITER+splitStrings2[2]);
		}
		return arrayList.iterator();
	}
}
