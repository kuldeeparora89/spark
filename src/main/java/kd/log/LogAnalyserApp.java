package kd.log;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.util.Comparator;
import java.util.List;

/**
 * Created by kuldeep on 05-04-2017.
 */
public class LogAnalyserApp {


    public void start(String master, String logFile) {


//
//        The average, min, and max content size of responses returned from the server.
//                A count of response code's returned.
//        All IPAddresses that have accessed this server more than N times.
//                The top endpoints requested by count.


        SparkConf conf = new SparkConf().setMaster(master).setAppName("Log Analyser");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile(logFile);

        JavaRDD<ApacheAccessLog> accessLogJavaRDD = data.map(ApacheAccessLog::parseFromLogLine);

        List<String> ipCount = getAllIpAdressWithMoreThanNHits(accessLogJavaRDD, 10);


        //Statistics
        System.out.println("Ip Address With >= 10 hits : ");
        ipCount.forEach(System.out::println);

        StatCounter contentStats = getContentSizeStats(accessLogJavaRDD);
        System.out.println("Avg. size of content : " + contentStats.sum() / contentStats.count());
        System.out.println("Min. size of content : " + contentStats.min());
        System.out.println("Max. size of content : " + contentStats.max());

        System.out.println("Count of response codes : " + getCountOfResponseCode(accessLogJavaRDD));

        System.out.println("Top Endpoints");
        getTopEndpointsByCount(accessLogJavaRDD,10).forEach(t-> System.out.println(t._1()+" "+t._2()));


        sc.stop();


    }


    private List<String> getAllIpAdressWithMoreThanNHits(JavaRDD<ApacheAccessLog> accessLogJavaRDD, int count) {


        JavaRDD<String> javaRdd =
                accessLogJavaRDD.mapToPair(a -> new Tuple2<>(a.getIpAddress(), 1L)).reduceByKey((v1, v2) -> v1 + v2).filter(t -> t._2() >= count).map(Tuple2::_1);

        System.out.println("------------------<><><><>-----------------");
        System.out.println(javaRdd.toDebugString());


        return  javaRdd.collect();



    }


    private StatCounter getContentSizeStats(JavaRDD<ApacheAccessLog> accessLogJavaRDD) {
        return accessLogJavaRDD.mapToDouble(ApacheAccessLog::getContentSize).stats();

    }


    private long getCountOfResponseCode(JavaRDD<ApacheAccessLog> accessLogJavaRDD) {
        return accessLogJavaRDD.map(ApacheAccessLog::getResponseCode).distinct().count();
    }

    private List<Tuple2<String, Long>> getTopEndpointsByCount(JavaRDD<ApacheAccessLog> accessLogJavaRDD, int count) {
        return accessLogJavaRDD.mapToPair(a -> new Tuple2<>(a.getEndpoint(), 1L)).reduceByKey((v1, v2) -> v1 + v2).top(count, Comparator.comparing(Tuple2::_2, Comparator.reverseOrder()));


    }


}
