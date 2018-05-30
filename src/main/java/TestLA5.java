import com.twitter.bijection.Bijection;
import com.twitter.bijection.Injection;
import datamodel.LaCanonicalMsgStructure;
import datamodel.LearnedAction;
import datamodel.PlayVodMsgStructure;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.util.Try;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Properties;


public class TestLA5 {
    final static String RESOURCE_HOME = "src/main/resources/";

    public static void main(String[] args) throws StreamingQueryException {

        final String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        String valueSchemaStr = "{\"namespace\":\"com.sky.recommendations.ladata\",\"type\":\"record\",\"name\":\"LearnedAction\",\"fields\":[{\"name\":\"territory\",\"type\":\"string\"},{\"name\":\"proposition\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},{\"name\":\"actionType\",\"type\":{\"type\":\"enum\",\"name\":\"ActivityType\",\"symbols\":[\"WATCH_VOD\",\"WATCH_LINEAR\",\"DOWNLOAD\",\"WATCH_STORE\",\"WATCH_LIVE\",\"WATCH_PVR\",\"PLAY_VOD\"]}},{\"name\":\"sourceId\",\"type\":\"string\"},{\"name\":\"deviceId\",\"type\":[\"null\",\"string\"]},{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"language\",\"type\":[\"null\",\"string\"]},{\"name\":\"videoId\",\"type\":\"string\"},{\"name\":\"genre\",\"type\":[\"null\",\"string\"]},{\"name\":\"channel\",\"type\":[\"null\",\"string\"]}]}";


        /*
        First of all a Spark Session is created. Spark Session as entry-point of Spark Framework
         */
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .appName("TestLA4")
                        //.config("spark.files","conf/hive-site.xml,conf/core-site.xml,conf/hdfs-site.xml")
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config("spark.sql.warehouse.dir", warehouseLocation)
                        .getOrCreate();
        /*
        A simple Test to read throw Structured Stream a source File
         */
        //simpleSourceTest(spark);

        /*
        A Dataset created from DE-VIDEO-PLAY_VOD
         */
        Dataset<Row> de_video_playVod_df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "mitstatlodpbroker01:9091" /*"localhost:9092"*/)
                .option("subscribe", "DE-VIDEO-PLAY_VOD" /*"bootcamp-kafka-messages"*/)
                .option("startingOffsets", "earliest")
                .load();

        /*
        Dataset can't be read correctly as is, need to be parsed to a JSON, in order to achieve this a schema where map
        it is created
         */
        StructType playVodMsgSchema = new PlayVodMsgStructure().getSchema();

        /*
        In this project we are not interest in key of the message, so finally get only the value as a JSON.
        Now we are able to use the double nature of Structured(SQL)-Streaming(Continuosly).
        Streaming can be abstracted as a table
        - each message is a record
        - each JSON property is a column
         */
        Dataset<Row> de_video_playVod_df1 = de_video_playVod_df
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value").cast("string"),playVodMsgSchema).as("vodMsg"))
                .select("vodMsg.*");

        /*
        Check if Schema is correct and try to launch a simple query.
         */
        //checkDataModelCorecteness(de_video_playVod_df1);

        /*
        NOW WE CAN START TO FILTER AND START HAVING FUN WITH OUR DATA
         */

        Dataset<Row> skyTicket_playVod_filtered = onlySkyTicket(de_video_playVod_df1);

        /*
        GET DATA FROM
        */

        Dataset<Row> apix_data = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss ZZ")
                .load(RESOURCE_HOME + "APIX/");
        apix_data.show();

        /*
        Make JOIN ON APIX playVOD.contentID and apix.materialnumber
         */

        Dataset<Row> apix_enriched_plaVod = skyTicket_playVod_filtered.
                join(apix_data,
                        skyTicket_playVod_filtered.col("contentId").substr(3,8)
                                .equalTo(apix_data .col("qsversionid")));
                //.as("enriched")
                //.select("enriched.*");


        apix_enriched_plaVod.printSchema();


        /*
        A canonical Learning Action is provided. Now we have to map it to the corresponding Dataset
         */

        Dataset<Row> raw_learnedAction = mapOnCanonicalLA(apix_enriched_plaVod,spark);


        //printStreamingDataset(raw_learnedAction);
        raw_learnedAction.printSchema();


        StructType laCanonicalMsgStructure = new LaCanonicalMsgStructure().getSchema();
        Dataset<String> testAvro = spark.readStream().format("json").schema(laCanonicalMsgStructure).load(RESOURCE_HOME + "test/").toJSON();

       JavaRDD<GenericRecord> testAvroRDD =  testAvro.toJavaRDD().map(a-> jsonToAvro(a,valueSchemaStr));

        writeOnKafka(testAvro.select(functions
                .to_json(functions
                        .struct(
                                "territory",
                                "proposition",
                                "timestamp",
                                "actionType",
                                "sourceId",
                                "deviceId",
                                "uuid",
                                "language",
                                "videoId",
                                "genre",
                                "channel"
                        )).as("value")));


        /*
        writeOnKafka(raw_learnedAction.select(functions
                .to_json(functions
                        .struct(
                                "territory",
                                "proposition",
                                "timestamp",
                                "actionType",
                                "sourceId",
                                "deviceId",
                                "uuid",
                                "language",
                                "videoId",
                                "genre",
                                "channel"
                        )).as("value")));
        */


        //TODO: Convert Dataset<Row> to Avro Format with the following schema
        /*
        {
            "namespace": "com.sky.recommendations.ladata",
            "type": "record",
            "name": "LearnedAction",
            "fields": [
            {"name": "territory", "type": "string"},
            {"name": "proposition", "type": "string"},
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "actionType", "type": { "type": "enum", "name": "ActivityType", "symbols" : ["WATCH_VOD", "WATCH_LINEAR", "DOWNLOAD", "WATCH_STORE", "WATCH_LIVE", "WATCH_PVR"]}},
            {"name": "sourceId", "type": "string"},
            {"name": "deviceId", "type": ["null", "string"]},
            {"name": "uuid", "type": "string"},
            {"name": "language", "type": ["null", "string"]},
            {"name": "videoId", "type": "string"},
            {"name": "genre", "type": ["null", "string"]},
            {"name": "channel", "type": ["null", "string"]}
             ]
            }
         */

        spark.stop();

    }

    public  static void printOnConsole(Dataset<Row> dataset) throws StreamingQueryException{
        StreamingQuery query = dataset.writeStream()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();

    }

    public static void writeOnKafka(Dataset<Row> dataset) throws StreamingQueryException {
        StreamingQuery query = dataset
                //.selectExpr("CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "mitstatlodpbroker01:9091")
                .option("value.serializer", "com.databricks.spark.avro")
                .option("topic", "test_play_vod_la_2")
                .option("checkpointLocation","/tmp2")
                .start();

        query.awaitTermination();
    }

    public static void printStreamingDataset(Dataset<Row> dataset) throws StreamingQueryException{
        StreamingQuery query = dataset.writeStream()
                .outputMode("append")
                .option("truncate", false)
                .format("console")
                .start();
        query.awaitTermination();

    }

    public static void simpleSourceTest(SparkSession spark){
        String logFile =  RESOURCE_HOME + "README.md"; // Should be some file on your system
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
        long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        Dataset<Row> df = spark.read().text(logFile);

        df.show();

    }

    public static void checkDataModelCorecteness(Dataset<Row> dataset) throws StreamingQueryException {
        dataset.printSchema();

        dataset.select("provider")
                .writeStream()
                .outputMode("append")
                .option("truncate", false)
                .format("console")
                .start()
                .awaitTermination();

    }

    public static Dataset<Row> onlySkyTicket(Dataset<Row> dataset){
        return dataset.select("*").where("provider = 'NOWTV'");
    }

    public static Dataset<Row> mapOnCanonicalLA(Dataset<Row> dataset,SparkSession spark){
        dataset.createOrReplaceTempView("enriched");

        return spark.sql(
                "SELECT " +
                        "activityTimestamp as timestamp," +
                        "providerTerritory as territory," +
                        "proposition," +
                        "householdId as sourceId," +
                        "deviceId," +
                        "contentId as videoId," +
                        "uuid," +
                        "feingenre as genre "
                        + "from enriched"
        )
                .withColumn("language", functions.lit(""))
                .withColumn("channel", functions.lit(""))
                .withColumn("actionType", functions.lit("WATCH_VOD"));
    }

    public static Dataset<Row> mapOnCanonicalLA(SparkSession spark){
        return spark.sql(
                "SELECT " +
                        "activityTimestamp as timestamp," +
                        "providerTerritory as territory," +
                        "proposition," +
                        "householdId as sourceId," +
                        "deviceId," +
                        "contentId as videoId," +
                        "uuid," +
                        "feingenre as genre "
                 + "from apix_enriched_plaVod"
        )
                .withColumn("language", functions.lit(""))
                .withColumn("channel", functions.lit(""))
                .withColumn("actionType", functions.lit("WATCH_VOD"));
    }

    public static GenericRecord jsonToAvro(String jsonValue,String valueSchema){
        JsonAvroConverter converter = new JsonAvroConverter();
        GenericData.Record value = converter.convertToGenericDataRecord(jsonValue.getBytes(StandardCharsets.UTF_8), new Schema.Parser().parse(valueSchema));
        return value;

        /*
                https://stackoverflow.com/questions/39049648/use-schema-to-convert-avro-messages-with-spark-to-dataframe

         */
    }


}
