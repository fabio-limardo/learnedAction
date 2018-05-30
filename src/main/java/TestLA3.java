import datamodel.PlayVodMsgStructure;
import dbconnection.HiveConnectionProperties;
import dbconnection.HiveConnector;
import dbconnection.ResultSetConverter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class TestLA3 {

    public static void main(String[] args) throws StreamingQueryException, SQLException, JSONException, IOException, ClassNotFoundException {

        final Logger log = LoggerFactory.getLogger(TestLA3.class);

        ArgumentParser parser = ArgumentParsers.newArgumentParser("HiveToKafka")
                .defaultHelp(true)
                .description("The Parameters to connect to Hive and write to Kafka");

        parser.addArgument("--hive-connection-string").type(String.class).help("The jdbc connection string of the source database");
        parser.addArgument("--hive-username").type(String.class).help("The username of the source database");
        parser.addArgument("--hive-password").type(String.class).help("The password of the source database");
        parser.addArgument("--hive-query").type(String.class).help("The query to be run on the source database");
        parser.addArgument("--schema-string").type(String.class).help("The Avro Schema to be written on Kafka");
        //parser.addArgument("--key-schema").type(String.class).help("Key Avro Schema");
        parser.addArgument("--broker-address").type(String.class).help("The broker address");
        parser.addArgument("--topic-name").type(String.class).help("The name of the topic to write to");
        parser.addArgument("--schema-registry").type(String.class).help("Schema Registry URL");
        parser.addArgument("--producer-name").type(String.class).help("Producer Name");
        parser.addArgument("--hive-ktab-user").type(String.class).help("key tab");
        parser.addArgument("--hive-ktab-path").type(String.class).help("Key Path");
        parser.addArgument("--flush-parameter").type(String.class).help("Flush Parameter");
        parser.addArgument("--key-field").type(String.class).help("Name of field you want to use as Key of the Output Topic ");
        //parser.addArgument("--output-format").type(String.class).help("Output Format");
        Namespace ns = parser.parseArgsOrFail(args);


        // show all passed arguments
        System.out.println("Passed arguments:");
        for (String k : ns.getAttrs().keySet()) {
            log.info(k + ": " + ns.getAttrs().get(k));
        }
        String sourceJDBCString, sourceUsername, sourcePassword, sourceQuery, sourceKeytab, prodName, brokerAddress, schemaString, schemaUrl, keyPath, topic,keySchemaString,keyField/*,outputFormat */ ;
        sourceJDBCString = ns.getString("hive_connection_string");
        sourceUsername = ns.getString("hive_username");
        sourcePassword = ns.getString("hive_password");
        sourceQuery = ns.getString("hive_query");
        brokerAddress = ns.getString("broker_address");
        schemaString = ns.getString("schema_string");
        topic = ns.getString("topic_name");
        schemaUrl = ns.getString("schema_registry");
        sourceKeytab = ns.getString("hive_ktab_user");
        prodName = ns.getString("producer_name");
        keyPath = ns.getString("hive_ktab_path");
        //keySchemaString = ns.getString("key_schema");
        keyField = ns.getString("key_field");
        //outputFormat = ns.getString("output_format");
        int flushParameter = Integer.valueOf(ns.getString("flush_parameter"));

        /////////////////////MY CODE

        final String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        final String hiveConnectionString = sourceJDBCString;
                //"jdbc:hive2://mitstatlodpdata01.sky.local:10000/default;transportMode=http;httpPath=cliservice;principal=hive/mitstatlodpdata01.sky.local@SKY.LOCAL";
        final String hiveConnectionUser = sourceUsername;
                //"bi_workflows_de";
        final String hiveConnectionPassword = sourcePassword;
                //"atlas%%";
        final String RESOURCE_HOME = "src/main/resources/";

        final String userKeytabPath = sourceKeytab;

        /*
        First of all a Spark Session is created. Spark Session as entry-point of Spark Framework
         */
        SparkSession spark =
                SparkSession.builder()
                        .master("local")
                        .appName("TestLA3")
                        //.config("spark.files","conf/hive-site.xml,conf/core-site.xml,conf/hdfs-site.xml")
                        .config("spark.sql.warehouse.dir", warehouseLocation)
                        .enableHiveSupport()
                        .config("spark.sql.hive.convertMetastoreOrc", "false")
                        .getOrCreate();
        /*
        A simple Test to read throw Structured Stream a source File
         */
        simpleSourceTest(spark);

        /*
        A Dataset created from DE-VIDEO-PLAY_VOD
         */
        Dataset<Row> de_video_playVod_df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "mitstatlodpbroker01:9091" /*"localhost:9092"*/)
                .option("subscribe", "DE-VIDEO-PLAY_VOD" /*"bootcamp-kafka-messages"*/)
                .option("startingOffsets", "earliest")
                .option("value.deserializer", "org.apache.connect.json.JsonDeserializer") //Probably not necessary
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

         */
        String query ="SELECT qsversionid,uuid,feingenre,beitragmrgid FROM default.raw_dapix_vod_nextday";
        String keytabPath = "/tmp/bi_workflows_de.keytab";
        String keytabUser = "bi_workflows_de";


        HiveConnectionProperties hiveConnectionProperties =
                new HiveConnectionProperties(
                        hiveConnectionString,
                        hiveConnectionUser,
                        hiveConnectionPassword,
                        "",
                        "",
                        "",
                        "",
                        query,
                        keytabUser,
                        keytabPath);
        JSONArray queryResult = queryToHive(hiveConnectionProperties);
        List<String> resulList = new ArrayList<>();

        for(int i = 0; i<queryResult.length();i++){
            resulList.add(queryResult.getString(i));
            log.info(queryResult.getString(i));
        }
        Dataset<String> apixJson = spark.createDataset(resulList,Encoders.STRING());
        Dataset<Row> apixRow =  apixJson.toDF();
        apixRow.show();

        //Dataset<Row> apix_data = spark.sql("SELECT qsversionid,uuid,feingenre,beitragmrgid FROM default.raw_dapix_vod_nextday");


        //apix_data.show();

        /*
        Make JOIN ON APIX playVOD.contentID and apix.materialnumber
         */

        /*
        Dataset<Row> apix_enriched_plaVod = skyTicket_playVod_filtered.
                join(apix_data,
                        skyTicket_playVod_filtered.col("contentId").substr(3,8)
                                .equalTo(apix_data .col("qsversionid")));
                //.as("enriched")
                //.select("enriched.*");
        */

        /*
        Launch an exception
        printStreamingDataset(apix_enriched_plaVod);

         */
        //apix_enriched_plaVod.printSchema();


        /*
        apix_enriched_plaVod
                .selectExpr(
                        "CAST(activityTimestamp AS STRING)",
                        "CAST(activityType AS STRING)",
                        "CAST(provider AS STRING)",
                        "CAST(providerTerritory AS STRING)",
                        "CAST(homeTerritory AS STRING)",
                        "CAST(proposition AS STRING)",
                        "CAST(userId AS STRING)",
                        "CAST(userType AS STRING)",
                        "CAST(householdId AS STRING)",
                        "CAST(deviceId AS STRING)",
                        "CAST(deviceType AS STRING)",
                        "CAST(devicePlatform AS STRING)",
                        "CAST(deviceModel AS STRING)",
                        "CAST(countryCode AS STRING)",
                        "CAST(contentId AS STRING)",
                        "CAST(ipAddress AS STRING)",
                        "CAST(deviceId AS STRING)",
                        "CAST(deviceId AS STRING)",

        writeOnKafka(apix_enriched_plaVod);
        printStreamingDataset(apix_enriched_plaVod);

        StreamingQuery apix_enriched_plaVod_sq =
                apix_enriched_plaVod
                        .writeStream()
                        .queryName("apix_enriched_plaVod")
                        .format("memory")
                        .outputMode("append")
                        .start();


        /*
        Dataset<Row> apix_enriched_plaVod = apix_data.
                        join(skyTicket_playVod_filtered,
                        de_video_playVod_df1.col("contentId").substr(3,8)
                                .equalTo(apix_data .col("qsversionid")))
                .as("enriched")
                .select("enriched.*");
                */
        /*
        A canonical Learning Action is provided. Now we have to map it to the corresponding Dataset
         */

        //apix_enriched_plaVod.show(20,false);
        //printStreamingDataset(apix_enriched_plaVod_sq);
        //Dataset<Row> raw_learnedAction = mapOnCanonicalLA(apix_enriched_plaVod);


        Dataset<Row> raw_learnedAction = mapOnCanonicalLA(spark);

        //printStreamingDataset(raw_learnedAction);
        raw_learnedAction.printSchema();

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
        /*
        raw_learnedAction
                .select(functions.to_json(
                        functions.struct(
                                "territory",
                                "deviceId",
                                "channel",
                                "sourceId",
                                "proposition",
                                "timestamp",
                                "language",
                                "actionType",
                                "videoId",
                                "genre",
                                "uuid"))).as("value").show();

        writeOnKafka(raw_learnedAction
                .select(functions.to_json(
                        functions.struct(
                                "territory",
                                "deviceId",
                                "channel",
                                "sourceId",
                                "proposition",
                                "timestamp",
                                "language",
                                "actionType",
                                "videoId",
                                "genre",
                                "uuid"))).as("value"));
          */

        spark.stop();

    }

    public static void writeOnKafka(Dataset<Row> dataset) throws StreamingQueryException {
        StreamingQuery ds = dataset
                //.selectExpr("CAST(value AS STRING)")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "mitstatlodpbroker01:9091")
                .option("topic", "test_play_vod_la")
                .option("checkpointLocation","/tmp")
                .start();

        //ds.awaitTermination();
    }

    public static void printStreamingDataset(Dataset<Row> dataset) throws StreamingQueryException{
        StreamingQuery query = dataset.writeStream()
                .outputMode("append")
                .option("truncate", false)
                .format("console")
                .start();
        query.awaitTermination();

    }

    public static void simpleSourceTest(SparkSession spark) throws IOException {
        final String RESOURCE_HOME = "src/main/resources/";
        ClassLoader classLoader = TestLA3.class.getClassLoader();
        File file = new File(classLoader.getResource("README.md").getFile());


        String logFile = "/tmp/README.md"; // Should be some file on hdfs

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

    public static Dataset<Row> mapOnCanonicalLA(Dataset<Row> dataset){

        return dataset.sqlContext().sql(
                "SELECT " +
                        "activityTimestamp as timestamp," +
                        "providerTerritory as territory," +
                        "proposition," +
                        "householdId as sourceId," +
                        "deviceId," +
                        "contentId as videoId," +
                        "uuid," +
                        "feingenre as genre "
                        // + "from apix_enriched_plaVod"
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

    public static JSONArray queryToHive(HiveConnectionProperties prop) throws SQLException, IOException, ClassNotFoundException, JSONException {
        HiveConnector.getInstance(prop).connect(prop.getHive_connection_string(), prop.getHive_username(), prop.getHive_password());
        ResultSet res = HiveConnector.getInstance(prop).executeQuery(prop.getHive_query());
        return ResultSetConverter.convert(res);

    }
}
