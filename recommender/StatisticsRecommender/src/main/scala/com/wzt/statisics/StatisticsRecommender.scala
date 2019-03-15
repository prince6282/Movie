package com.wzt.statisics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StatisticsRecommender {

    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_MOVIE_COLLECTION = "Movie"

    //统计的表的名称
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"

    // 入口方法
    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
            "mongo.db" -> "recommender"
        )

        //创建SparkConf配置
        val sparkConf = new SparkConf()
                .setAppName("StatisticsRecommender")
                .setMaster(config("spark.cores"))

        //创建SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))




        //加入隐式转换
        import spark.implicits._

        //数据加载进来
        val ratingDF = spark
                .read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Rating]
                .toDF()

        val movieDF = spark
                .read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_MOVIE_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Movie]
                .toDF()

        //创建一张名叫ratings的表
        ratingDF.createOrReplaceTempView("ratings")

        //TODO: 不同的统计推荐结果

        spark.stop()
    }
}