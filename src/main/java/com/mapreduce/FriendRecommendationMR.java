package com.mapreduce;

import com.util.SparkUtil;

import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;


/**
 * A Map Reduce solution for Friend Recommendation Links using Apache Spark
 */
public class FriendRecommendationMR {

    public static void main(String[] args) throws Exception {
        //If the path to the file is missing show error message with Usage
        if (args.length < 1) {
            System.err.println("Usage: FriendRecommendationMR <input-path>");
            System.exit(1);
        }

        final String friendsInputPath = args[0];    // Path to the data set

        // Create a JavaSparkContext
        JavaSparkContext sparkContext = SparkUtil.createJavaSparkContext("FriendRecommendationMR");

        // Read data set
        JavaRDD<String> records = sparkContext.textFile(friendsInputPath, 1);

        List<String> debug1 = records.collect();

        PrintWriter writer = new PrintWriter("result_recommendation.txt", "UTF-8");


        // Start PHASE 1 - Read DATA
        System.out.println("PHASE 1 - Read Lines\n");
        writer.println("################################## PHASE 1 - Read Lines");
        writer.println("HOW TO READ: <USER_ID><TAB><RECOMMENDATION_1><,><RECOMMENDATION_2><,><RECOMMENDATION_3><,>...\n");
        for (String t : debug1) {
            System.out.println("\tUSER_ID=" + t);
            writer.println("\tUSER_ID=" + t);
        }

        // MAPPER
        JavaPairRDD<Long, Tuple2<Long, Long>> pairs = records.flatMapToPair(new PairFlatMapFunction<String, Long, Tuple2<Long, Long>>() {
            @Override
            public Iterator<Tuple2<Long, Tuple2<Long, Long>>> call(String record) {

                // DATA SET TEMPLATE: record=<user_id><TAB><friend1><,><friend2><,><friend3><,>...

                String[] tokens = record.split("\t");
                long user_id = Long.parseLong(tokens[0]);
                String friendsAsString = tokens[1];
                String[] friendsTokenized = friendsAsString.split(",");

                List<Long> friends = new ArrayList<Long>();
                List<Tuple2<Long, Tuple2<Long, Long>>> mapperOutput = new ArrayList<Tuple2<Long, Tuple2<Long, Long>>>();

                // Set direct friendship as -1
                for (String friendAsString : friendsTokenized) {
                    long toUser = Long.parseLong(friendAsString);
                    friends.add(toUser);
                    Tuple2<Long, Long> directFriend = T2(toUser, -1L); // Set direct friendship as -1
                    mapperOutput.add(T2(user_id, directFriend));
                }

                // Cross data and get all possible friends
                for (int i = 0; i < friends.size(); i++) {
                    for (int j = i + 1; j < friends.size(); j++) {
                        // Possible friend 1
                        Tuple2<Long, Long> possibleFriend1 = T2(friends.get(j), user_id);
                        mapperOutput.add(T2(friends.get(i), possibleFriend1));
                        // Possible friend 2
                        Tuple2<Long, Long> possibleFriend2 = T2(friends.get(i), user_id);
                        mapperOutput.add(T2(friends.get(j), possibleFriend2));
                    }
                }
                return mapperOutput.iterator();
            }
        });


        // Start PHASE 2
        System.out.println("PHASE 2 - Result of the MAPPER\n\n");
        writer.println("\n################################## PHASE 2 - Result of the MAPPER");
        writer.println("Showing All possible friendships. Those with -1 are direct friendship.\n");
        //writer.println("HOW TO READ: USER_ID=<USER_ID> VALUE=(DIRECT_FRIEND,POSSIBLE_FRIEND)\n\n");
        List<Tuple2<Long, Tuple2<Long, Long>>> debug2 = pairs.collect();
        for (Tuple2<Long, Tuple2<Long, Long>> t2 : debug2) {
            System.out.println("\tUSER_ID=" + t2._1 + "\t VALUE=" + t2._2);
            writer.println("\tUSER_ID=" + t2._1 + "\t VALUE=" + t2._2);
        }

        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> grouped = pairs.groupByKey();


        // Start PHASE 2
        System.out.println("PHASE 3 - Regroup\n\n");
        writer.println("\n################################## PHASE 3 - Regroup\n");

        List<Tuple2<Long, Iterable<Tuple2<Long, Long>>>> debug3 = grouped.collect();
        for (Tuple2<Long, Iterable<Tuple2<Long, Long>>> t2 : debug3) {
            System.out.println("\tUSER_ID=" + t2._1 + "\t VALUES=" + t2._2);
            writer.println("\tUSER_ID=" + t2._1 + "\t VALUES=" + t2._2);
        }

        // REDUCER
        // Find intersection of all List<List<Long>>
        // mapValues[U](f: (V) => U): JavaPairRDD[K, U]
        // Pass each value in the key-value pair RDD through a map function without changing the keys;
        // this also retains the original RDD's partitioning.
        JavaPairRDD<Long, String> recommendations =
                grouped.mapValues(new Function<Iterable<Tuple2<Long, Long>>, String>() {
                    @Override
                    public String call(Iterable<Tuple2<Long, Long>> values) {

                        final Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();
                        for (Tuple2<Long, Long> t2 : values) {
                            final Long toUser = t2._1;
                            final Long mutualFriend = t2._2;
                            final boolean alreadyFriend = (mutualFriend == -1);

                            if (mutualFriends.containsKey(toUser)) {
                                if (alreadyFriend) {
                                    mutualFriends.put(toUser, null);
                                } else if (mutualFriends.get(toUser) != null) {
                                    mutualFriends.get(toUser).add(mutualFriend);
                                }
                            } else {
                                if (alreadyFriend) {
                                    mutualFriends.put(toUser, null);
                                } else {
                                    List<Long> list1 = new ArrayList<Long>(Arrays.asList(mutualFriend));
                                    mutualFriends.put(toUser, list1);
                                }
                            }
                        }
                        return buildRecommendations(mutualFriends);
                    }
                });


        // FINAL RESULT
        System.out.println("################# FINAL RESULT ################# \n\n");
        writer.println("\n\n################################## FINAL RESULT ################# \n\n");
        List<Tuple2<Long, String>> debug4 = recommendations.collect();
        for (Tuple2<Long, String> t2 : debug4) {
            System.out.println("\tUSER_ID=" + t2._1 + "\t RECOMMENDATION=" + t2._2);
            writer.println("\tUSER_ID=" + t2._1 + "\t RECOMMENDATION=" + t2._2);
        }

        System.out.println("\n\nAuthor: Robert Marinho - robstermarinho@gmail.com");
        writer.close();
        sparkContext.close();
        System.exit(0);
    }


    static String buildRecommendations(Map<Long, List<Long>> mutualFriends) {
        StringBuilder recommendations = new StringBuilder();
        for (Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            recommendations.append(entry.getKey());
            recommendations.append(" (");
            recommendations.append(entry.getValue().size());
            recommendations.append(": ");
            recommendations.append(entry.getValue());
            recommendations.append(") ");
        }
        return recommendations.toString();
    }

    static Tuple2<Long, Long> buildSortedTuple(long a, long b) {
        if (a < b) {
            return new Tuple2<Long, Long>(a, b);
        } else {
            return new Tuple2<Long, Long>(b, a);
        }
    }

    static Tuple2<Long, Long> T2(long a, long b) {
        return new Tuple2<Long, Long>(a, b);
    }

    static Tuple2<Long, Tuple2<Long, Long>> T2(long a, Tuple2<Long, Long> b) {
        return new Tuple2<Long, Tuple2<Long, Long>>(a, b);
    }

}
