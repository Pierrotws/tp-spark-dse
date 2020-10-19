package com.datastax.spark.example;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple6;
import scala.runtime.AbstractFunction1;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class FileToTable {
  public static void main(String[] args) {

    // A SparkSession
    SparkSession spark = SparkSession
      .builder()
      .appName("Datastax Java example")
      .getOrCreate();

    //Initialization of cassandra goes here
    CassandraConnector.apply(spark.sparkContext()).withSessionDo(
      new AbstractFunction1<Session, Object>() {
        public Object apply(Session session) {
          session.execute("CREATE KEYSPACE IF NOT EXISTS ks WITH "
            + "replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
          return session
            .execute("CREATE TABLE IF NOT EXISTS ks.kv (k int, v int, PRIMARY KEY (k))");
        }
      });

    List<Tuple6<String, String, String, Integer, Integer, Integer>> list = Arrays.asList(new Tuple6<>("James","Sales","NY",90000,34,10000),
            new Tuple6<>("Michael","Sales","NY",86000,56,20000),
            new Tuple6<>("Robert","Sales","CA",81000,30,23000),
            new Tuple6<>("Maria","Finance","CA",90000,24,23000),
            new Tuple6<>("Raman","Finance","CA",99000,40,24000),
            new Tuple6<>("Scott","Finance","NY",83000,36,19000),
            new Tuple6<>("Jen","Finance","NY",79000,53,15000),
            new Tuple6<>("Jeff","Marketing","CA",80000,25,18000),
            new Tuple6<>("Kumar","Marketing","NY",91000,50,21000)
    );

    Encoder<Tuple6<String, String, String, Integer, Integer, Integer>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING(), Encoders.INT(), Encoders.INT(), Encoders.INT());

    Dataset<Tuple6<String, String, String, Integer, Integer, Integer>> simpleData = spark.sparkContext().createDataset(list, encoder);

    simpleData.collect().forEach(System.out::println);
    spark.stop();
    System.exit(0);
  }
}

