package app;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.netlib.util.intW;

import scala.tools.nsc.settings.AdvancedScalaSettings.X;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkSession spark = SparkSession.builder().master("local")
				.appName("javaTfidfTest")
				.getOrCreate();
		
		List<Row> data = Arrays.asList(
				RowFactory.create(0.0, "Hi I heard about Spark"),
			      RowFactory.create(0.0, "I wish Java could use case classes"),
			      RowFactory.create(1.0, "Logistic regression models are neat")
			      );
		
		StructType schema = new StructType(new StructField[]{
				new StructField("label",DataTypes.DoubleType,false,Metadata.empty()),
				new StructField("sentence",DataTypes.StringType,false,Metadata.empty())
		});
		
		Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		Dataset<Row> wordsData = tokenizer.transform(sentenceData);
		
		int numFeatures = 20;
		HashingTF hashingTF = new HashingTF().setInputCol("words")
				.setOutputCol("rawFeatures")
				.setNumFeatures(numFeatures);
		
		Dataset<Row> featurizedData = hashingTF.transform(wordsData);
		
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		//rescaledData.select("words","features").show();
	   
	    List<Row> list =rescaledData.collectAsList();
	    for(Row r:list){
	    	System.out.println(r.mkString("||||"));
	    }
		
		spark.stop();
	}

}
