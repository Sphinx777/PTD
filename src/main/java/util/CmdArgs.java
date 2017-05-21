package util;

import org.kohsuke.args4j.Option;

/**
 * Created by user on 2017/3/8.
 */
public class CmdArgs {
    @Option(name="-iters",usage="Sets a number of iteration")
    public static int numIters = 10;

    @Option(name="-factor",usage="Sets a number of factor")
    public static int numFactors = 5;

    @Option(name="-top",usage="Sets a number of top word")
    public static int numTopWords = 10;

    @Option(name="-input",usage="Sets a path of input")
    public static String inputFilePath;

    @Option(name="-output",usage="Sets a path of output")
    public static String outputFilePath;

    @Option(name="-model",usage="Sets a model(DTTD , intJNMF , vector , coherence)")
    public static String model;

    @Option(name="-cohInput",usage="Sets a coherence file input")
    public static String coherenceFilePath;

    @Option(name="-cores",usage="Sets compute cores")
    public static int cores = 2;

    @Option(name="-threshold",usage="Sets a number of threshold")
    public static int numThreshold = 3000;

    @Option(name="-sample",usage="Sets a number of threshold")
    public static int numSample = 0;
}
