import java.util.Arrays;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import twitter4j.Status;

public class Exercise_2 {

	public static void get10MostPopularHashtagsInLast5min(JavaDStream<Status> statuses) {

		/**
		 * Get the stream of hashtags from the stream of tweets
		 */

		JavaDStream<String> words = statuses.flatMap(x -> Arrays.asList(x.getText().split(" ")).iterator());

		JavaDStream<String> hashTags = words.filter(x -> x.startsWith("#"));

		// hashTags.print();

		/**
		 * Count the hashtags over a 5 minute window
		 */

		JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(x -> new Tuple2<>(x, 1)); // map with key and value
																								// pairs

		JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow((a, b) -> a + b, (a, b) -> a - b,
				new Duration(5 * 60 * 1000), new Duration(1 * 1000));

		// counts.print();

		/**
		 * Find the top 10 hashtags based on their counts
		 */

		JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(x -> x.swap());

		JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(x -> x.sortByKey(false));

		sortedCounts.foreachRDD(x -> {
			
			System.out.println("NEW RDD: \n");
			
			for (Tuple2<Integer, String> i : x.take(10)) {
				System.out.println(i.toString() + " \n");
			}

		});

	}

}