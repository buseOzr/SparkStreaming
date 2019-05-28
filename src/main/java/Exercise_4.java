import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;
import twitter4j.Status;

public class Exercise_4 {

	public static void historicalAnalysis(JavaDStream<Status> statuses) {

		JavaDStream<String> words = statuses.flatMap(x -> Arrays.asList(x.getText().split(" ")).iterator());

		JavaDStream<String> hashTags = words.filter(x -> x.startsWith("#"));

		// hashTags.print();

		/**
		 * Count the hashtags over a 5 minute window
		 */ // key,value,state

		JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(x -> new Tuple2<>(x, 1));
		Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> updateFunction2 = (word, one,
				state) -> {
			int sum = one.or(0) + (state.exists() ? state.get() : 0);
			Tuple2<String, Integer> output = new Tuple2<String, Integer>(word, sum);
			state.update(sum);
			return output;
		};

		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> runningCounts = tuples
				.mapWithState(StateSpec.function(updateFunction2));
		// runningCounts.print();

		JavaPairDStream<Integer, String> swappedCounts = runningCounts.mapToPair(x -> x.swap());

		JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(x -> x.sortByKey(false));

		/*
		 * sortedCounts.foreachRDD(x -> { System.out.println("NEW BATCH: \n");
		 * 
		 * for (Tuple2<Integer, String> i : x.take(10)) {
		 * System.out.println(i.toString() + " \n"); }
		 * 
		 * });
		 */
		sortedCounts.foreachRDD(rdd -> {
			// System.out.println("oley");
			List<Tuple2<Integer, String>> sortedSt = rdd.collect();
			if (!sortedSt.isEmpty()) {
				System.out.println("Median: " + sortedSt.get(sortedSt.size() / 2));
			}
		});
	}

}
