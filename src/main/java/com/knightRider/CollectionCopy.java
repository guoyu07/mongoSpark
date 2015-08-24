package com.knightRider;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;

public class CollectionCopy {

    public static void main(String[] args) {

        JavaSparkContext sparkContext = new ContextInitializer(
                                                    ContextInitializer.STANDALONE_MODE,
                                                    "demo application"
                                            ).getSparkContext();

        // Set configuration options for the MongoDB Hadoop Connector.
        Configuration mongodbConfig = new Configuration();

        // MongoInputFormat allows us to read from a live MongoDB instance.
        // We could also use BSONFileInputFormat to read BSON snapshots.
        mongodbConfig.set("mongo.job.input.format",
                "com.mongodb.hadoop.MongoInputFormat");

        // MongoDB connection string naming a collection to use.
        // If using BSON, use "mapred.input.dir" to configure the directory
        // where BSON files are located instead.
        mongodbConfig.set("mongo.input.uri",
                "mongodb://localhost:27017/marketdata.minibars");

        // Create an RDD backed by the MongoDB collection.
        JavaPairRDD<Object, BSONObject> documents = sparkContext.newAPIHadoopRDD(
                mongodbConfig,            // Configuration
                MongoInputFormat.class,   // InputFormat: read from a live cluster.
                Object.class,             // Key class
                BSONObject.class          // Value class
        );

        // Create a separate Configuration for saving data back to MongoDB.
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri",
                "mongodb://localhost:27017/marketdata.result");


        // Save this RDD as a Hadoop "file".
        // The path argument is unused; all documents will go to 'mongo.output.uri'.
        documents.saveAsNewAPIHadoopFile(
                "file:///dummyPath",
                Object.class,
                BSONObject.class,
                MongoOutputFormat.class,
                outputConfig
        );
    }
}
