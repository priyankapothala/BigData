package edu.ucr.cs.cs226.ppoth001;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class SparkRDD
{
    public static void main( String[] args ) throws IOException
    {
        if (args.length == 0)
        {
            System.out.println("Please enter the file name.");
            System.exit(0);
        }
        JavaSparkContext sc =
                new JavaSparkContext("local", "SparkRDD");
        JavaRDD<String> inputFile = sc.textFile(args[0]);
        JavaPairRDD<Integer,Double> bytesBycode = inputFile.mapToPair(s->{
            String[] cols = s.split("\t");
            return new Tuple2<Integer, Double>(Integer.parseInt(cols[5]),Double.parseDouble(cols[6]));
        });

        JavaPairRDD<Integer,Tuple2<Double,Integer>> valuesRDD = bytesBycode.mapValues(value -> new Tuple2<Double,Integer>(value,1));
        JavaPairRDD<Integer,Tuple2<Double,Integer>> reduceRDD = valuesRDD.reduceByKey((s1,s2)-> new Tuple2<Double,Integer>(s1._1 + s2._1, s1._2 + s2._2));
        JavaPairRDD<Integer, Double> averageRDD = reduceRDD.mapToPair(s->{
            /*
            s._1 is the reponse code, s._2 is a tuple(byteCount,responseCodeCount)
            */
            Tuple2<Double, Integer> byteCount = s._2;
            return new Tuple2<Integer,Double>(s._1,byteCount._1/byteCount._2);
        });

        Map<Integer, Double> result = averageRDD.collectAsMap();
        FileWriter fw1 = new FileWriter("task1.txt");
        for (Integer key : result.keySet()) {
            fw1.write("Code "+key+", average number of bytes = "+ result.get(key)+"\n");
        }
        fw1.close();

        JavaPairRDD<String, String> records = inputFile.mapToPair(s -> {
            String hostURL = s.split("\t")[0] + s.split("\t")[4];
            return new Tuple2<String, String>(hostURL, s);
        });

        JavaPairRDD<String, Tuple2<String, String>> joined = records.join(records);
        joined.filter(s -> {
            if (s._2._1.equalsIgnoreCase(s._2._2))
                return false;
            Long timeStamp1 = Long.parseLong(s._2._1.split("\t")[2]);
            Long timeStamp2 = Long.parseLong(s._2._2.split("\t")[2]);
            if (timeStamp1 > timeStamp2)
                return false;
            if (Math.abs(timeStamp1 - timeStamp2) <= 3600)
                return true;
            return false;
        }).map(s -> String.format("%s\t%s", s._2._1, s._2._2)).saveAsTextFile("task2.txt");
    }
}
