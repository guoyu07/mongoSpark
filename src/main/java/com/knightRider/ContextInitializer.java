package com.knightRider;

import org.apache.spark.api.java.JavaSparkContext;

import javax.annotation.Nonnull;

/**
 * Created by prashant on 24/8/15.
 */
public class ContextInitializer {

    private JavaSparkContext sparkContext;
    public static final String STANDALONE_MODE = "local[*]";

    /**
     * @param sparkMaster     The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
     *                        run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
     *                        local[*] for architecture dependent number of Cores.
     * @param applicationName A name for your application, to display on the cluster web UI
     * @description accepts two parameters first is SparkMaster and Second is ApplicationName
     */
    public ContextInitializer(@Nonnull String sparkMaster, @Nonnull String applicationName) {
        sparkContext = new JavaSparkContext(sparkMaster, applicationName);
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }
}
