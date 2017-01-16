package com.fonuder.syrkxxzb;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Collections.singletonList;
import static org.apache.spark.sql.functions.col;

/**
 * Created by tiger on 2017/1/12.
 */
public class QuerySpark {

    private static Logger logger = LoggerFactory.getLogger(QuerySpark.class);


    public static void main(String[] args) throws ParseException {

        SparkConf sc = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour")
                .set("spark.mongodb.input.uri", "mongodb://172.29.214.23:27017/founder.syrkxxzb")
                .set("spark.mongodb.output.uri", "mongodb://172.29.214.23:27017/founder.syrkxxzb");

        JavaSparkContext jsc = new JavaSparkContext(sc);

//        Dataset<Row> rdd = MongoSpark.load(jsc).toDF();
        JavaMongoRDD rdd = MongoSpark.load(jsc);
        JavaRDD jdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { _id: { $gt: 15, $lt: 20 } } } }")));
        logger.info("{}",jdd.first());
//        long count = rdd.filter(col("_id").equalTo(2)).count();

//        long time = 1378356771820L;
//        Date date = new Date(time);
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String newDate = df.format(date);
//        System.out.println(newDate);
//        long count = rdd.filter(col("birthdate").gt("2017-01-06 00:00:00")).count();

//        SparkSession sparkSession = SparkSession
//                .builder()
//                .master("local")
//                .appName("MongoSparkConnectorTour")
//                .config("spark.mongodb.input.uri", "mongodb://172.29.214.23:27017/founder.syrkxxzb")
//                .config("spark.mongodb.output.uri", "mongodb://172.29.214.23:27017/founder.syrkxxzb")
//                .getOrCreate();
//
//        Dataset<Row> df = sparkSession
//                .read()
//                .format("com.mongodb.spark.sql")
//                .option("pipeline", "{ $match: { _id: { $gt: 15, $lt: 20 } } } }")
//                .load();


//        logger.info("{}",df.first().toString());

    }

}
