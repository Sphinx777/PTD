package util;

import org.kohsuke.args4j.Option;

public class TopicConstant {
	public static final String COMMA_DELIMITER=",";
	public static final String SEMICOLON_DELIMITER=";";
	public static final String SPACE_DELIMITER=" ";
	public enum MatrixOperation {
		Mutiply,
		Divide
	}
	public static final String DATE_FORMAT = "EEE MMM dd kk:mm:ss zzz yyyy";
	public static final String OUTPUT_FILE_DATE_FORMAT = "yyyy-MMdd-HHmmss";
	public static final int numFeatures = 100;

	@Option(name="-iters",usage="Sets a number of iteration")
	public static int numIters;

	@Option(name="-factor",usage="Sets a number of factor")
	public static int numFactors;

	@Option(name="-top",usage="Sets a number of top word")
	public static int numTopWords;

	@Option(name="-input",usage="Sets a path of input")
	public static String inputFilePath;

	@Option(name="-output",usage="Sets a path of output")
	public static String outputFilePath;

	@Option(name="-model",usage="Sets a model(DTTD , intJNMF , vector , coherence)")
	public static String model;

	@Option(name="-cohInput",usage="Sets a coherence file input")
	public static String coherenceFilePath;
}