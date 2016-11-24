package util;

import org.kohsuke.args4j.Option;

import java.math.BigDecimal;

public class TopicConstant {
	public static final String COMMA_DELIMITER=",";
	public static final String SEMICOLON_DELIMITER=";";
	public enum MatrixOperation {
		Mutiply,
		Divide
	}
	public static final String DATE_FORMAT = "EEE MMM dd kk:mm:ss zzz yyyy";
	public static final int numFeatures = 100;
	@Option(name="-iters",usage="Sets a number of iteration")
	public static int numIters=10;
}
