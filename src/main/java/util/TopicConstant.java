package util;

import org.kohsuke.args4j.Option;

public class TopicConstant {
	public static final String COMMA_DELIMITER=",";
	public static final String SEMICOLON_DELIMITER=";";
	public static final String SPACE_DELIMITER=" ";
	public static final String DOUBLE_QUOTE_DELIMITER="\"";
	public static final String LINE_BREAKER=System.getProperty("line.separator");
	public enum MatrixOperation {
		Add,
		Subtract,
		Multiply,
		Divide
	}
	public static final String DATE_FORMAT = "EEE MMM dd kk:mm:ss zzz yyyy";
	public static final String OUTPUT_FILE_DATE_FORMAT = "yyyy-MMdd-HHmmss";
	public static final double DAY_IN_MILLISECONDS = 24 * 60 * 60 * 1000;
	public static final double MONTH_IN_MILLISECONDS = 30 * DAY_IN_MILLISECONDS;
	public static final double YEAR_IN_MILLISECONDS = 365 * DAY_IN_MILLISECONDS;
	public static final double DECADE_IN_MILLISECONDS = 10 * YEAR_IN_MILLISECONDS;
}