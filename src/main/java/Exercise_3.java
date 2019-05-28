import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import twitter4j.Status;

public class Exercise_3 {

	public static void historicalAnalysis(JavaDStream<Status> statuses) {

		JavaDStream<String> words = statuses.flatMap(x -> Arrays.asList(x.getText().split(" ")).iterator());

		JavaDStream<String> hashTags = words.filter(x -> x.startsWith("#"));

		// hashTags.print();

		/**
		 * Count the hashtags over a 5 minute window
		 */

		JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(x -> new Tuple2<>(x, 1));

		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction = (values, state) -> {
			Integer newSum = 0;
			if (state.isPresent()) {
				newSum = state.get();
			}

			Iterator<Integer> i = values.iterator();
			while (i.hasNext()) {
				newSum += i.next();
			}
			return Optional.of(newSum);
		};

		JavaPairDStream<String, Integer> runningCounts = tuples.updateStateByKey(updateFunction);
		// runningCounts.print();

		JavaPairDStream<Integer, String> swappedCounts = runningCounts.mapToPair(x -> x.swap());

		JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(x -> x.sortByKey(false));

		sortedCounts.foreachRDD(x -> {
			System.out.println("NEW BATCH: \n");

			for (Tuple2<Integer, String> i : x.take(10)) {
				System.out.println(i.toString() + " \n");
			}

		});

	}

}
