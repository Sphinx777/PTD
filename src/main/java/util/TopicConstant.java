package util;

import org.kohsuke.args4j.Option;

public class TopicConstant {
	public static final String COMMA_DELIMITER=",";
	public static final String SEMICOLON_DELIMITER=";";
	public static final String SPACE_DELIMITER=" ";
	public static final String DOUBLE_QUOTE_DELIMITER="\"";
	public enum MatrixOperation {
		Mutiply,
		Divide
	}
	public static final String DATE_FORMAT = "EEE MMM dd kk:mm:ss zzz yyyy";
	public static final String OUTPUT_FILE_DATE_FORMAT = "yyyy-MMdd-HHmmss";
	public static final int numFeatures = 100;
}