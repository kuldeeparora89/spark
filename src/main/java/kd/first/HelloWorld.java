package kd.first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kuldeep pc on 31-Mar-17.
 */
public class HelloWorld {

public void countListValues(String master){


    SparkConf conf = new SparkConf().setMaster(master).setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
    JavaRDD<Integer> distData = sc.parallelize(data);

    System.out.println("Count  ------> " + distData.count());
    sc.stop();

}

}
