package datamodel;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class LaCanonicalMsgStructure {
    public static final String TERRITORY = "territory";
    public static final String PROPOSITION = "proposition";
    public static final String TIMESTAMP = "timestamp";
    public static final String ACTION_TYPE = "actionType";
    public static final String SOURCE_ID = "sourceId";
    public static final String DEVICE_ID = "deviceId";
    public static final String UUID = "uuid";
    public static final String LANGUAGE = "language";
    public static final String VIDEO_ID = "videoId";
    public static final String GENRE = "genre";
    public static final String CHANNEL = "channel";

    public StructType getSchema() {
        return new StructType()
                .add(LaCanonicalMsgStructure.TERRITORY, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.PROPOSITION, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.TIMESTAMP, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.ACTION_TYPE, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.SOURCE_ID, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.DEVICE_ID, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.UUID, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.LANGUAGE, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.VIDEO_ID, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.GENRE, DataTypes.StringType)
                .add(LaCanonicalMsgStructure.CHANNEL, DataTypes.StringType);
    }
}
