package util;

import java.math.BigDecimal;

public class TopicConstant {
	public static final String COMMA_DELIMITER=",";
	public static final String SEMICOLON_DELIMITER=";";
	public static final BigDecimal DOUBLE_MAX = BigDecimal.valueOf(Double.MAX_VALUE);
	public static final BigDecimal DOUBLE_MIN = BigDecimal.valueOf(-Double.MAX_VALUE);
	public enum MatrixOperation {
		Mutiply,
		Divide
	}
	public static final String DATE_FORMAT = "EEE MMM dd kk:mm:ss zzz yyyy";
}
