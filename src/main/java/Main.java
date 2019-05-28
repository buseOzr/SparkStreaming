import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;

public class Main {

	static String TWITTER_CONFIG_PATH = "C:\\Users\\buse\\Desktop\\BDMA\\UPC\\Big Data Management\\Spark-Streaming Lab\\Lab06-Training_Java\\Lab06-Training_Java\\src\\main\\resources\\twitter configuration.txt";
	static String HADOOP_COMMON_PATH = "C:\\Users\\buse\\Desktop\\BDMA\\UPC\\Big Data Management\\Spark-Streaming Lab\\Lab06-Training_Java\\Lab06-Training_Java\\src\\main\\resources\\winutils\\bin";

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
		SparkConf conf = new SparkConf().setAppName("SparkStreamingTraining").setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		JavaStreamingContext jsc = new JavaStreamingContext(ctx, new Duration(1000));
		LogManager.getRootLogger().setLevel(Level.ERROR);
		LogManager.shutdown();
		jsc.checkpoint(Files.createTempDir().getAbsolutePath());
		Utils.setupTwitter(TWITTER_CONFIG_PATH);
		JavaDStream<Status> tweets = TwitterUtils.createStream(jsc);

		if (args[0].equals("exercise_1")) {
			Exercise_1.displayAllTweets(tweets);
		} else if (args[0].equals("exercise_2")) {
			Exercise_2.get10MostPopularHashtagsInLast5min(tweets);
		} else if (args[0].equals("exercise_3")) {
			Exercise_3.historicalAnalysis(tweets);
		} else if (args[0].equals("exercise_4")) {
			Exercise_4.historicalAnalysis(tweets);
		}

		jsc.start();
		jsc.awaitTermination();
	}
}
