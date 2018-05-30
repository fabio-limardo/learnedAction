package datamodel;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class PlayVodMsgStructure {
    public static final String ACTIVITY_TIMESTAMP = "activityTimestamp";
    public static final String ACTIVITY_TYPE = "activityType";
    public static final String PROVIDER = "provider";
    public static final String PROVIDER_TERRITORY = "providerTerritory";
    public static final String HOME_TERRITORY = "homeTerritory";
    public static final String PROPOSITION = "proposition";
    public static final String USER_ID = "userId";
    public static final String USER_TYPE = "userType";
    public static final String HOUSEHOLD_ID = "householdId";
    public static final String DEVICE_ID = "deviceId";
    public static final String DEVICE_TYPE = "deviceType";
    public static final String DEVICE_PLATFORM ="devicePlatform";
    public static final String DEVICE_MODEL ="deviceModel";
    public static final String COUNTRY_CODE ="countryCode";
    public static final String CONTENT_ID = "contentId";
    public static final String IP_ADRESS = "ipAddress";
    public static final String GEO_IP = "geoIP";
    public static final String PERSONA_ID = "personaId";
    public static final String OFFER_TYPE = "offerType";
    public static final String REQUEST_ID = "requestId";
    public static final String STREAMING_TICKET = "streamingTicket";
    public static final String ORIGINATING_SYSTEM = "originatingSystem";

    public StructType getSchema(){
        return new StructType()
                .add(PlayVodMsgStructure.ACTIVITY_TIMESTAMP, DataTypes.StringType)
                .add(PlayVodMsgStructure.ACTIVITY_TYPE, DataTypes.StringType)
                .add(PlayVodMsgStructure.PROVIDER, DataTypes.StringType)
                .add(PlayVodMsgStructure.PROVIDER_TERRITORY, DataTypes.StringType)
                .add(PlayVodMsgStructure.HOME_TERRITORY, DataTypes.StringType)
                .add(PlayVodMsgStructure.PROPOSITION, DataTypes.StringType)
                .add(PlayVodMsgStructure.USER_ID, DataTypes.StringType)
                .add(PlayVodMsgStructure.USER_TYPE, DataTypes.StringType)
                .add(PlayVodMsgStructure.HOUSEHOLD_ID, DataTypes.StringType)
                .add(PlayVodMsgStructure.DEVICE_ID, DataTypes.StringType)
                .add(PlayVodMsgStructure.DEVICE_TYPE, DataTypes.StringType)
                .add(PlayVodMsgStructure.DEVICE_PLATFORM, DataTypes.StringType)
                .add(PlayVodMsgStructure.DEVICE_MODEL, DataTypes.StringType)
                .add(PlayVodMsgStructure.COUNTRY_CODE, DataTypes.StringType)
                .add(PlayVodMsgStructure.CONTENT_ID, DataTypes.StringType)
                .add(PlayVodMsgStructure.IP_ADRESS, DataTypes.StringType)
                .add(PlayVodMsgStructure.GEO_IP,
                        new StructType()
                                .add(PlayVodMsgStructure.IP_ADRESS, DataTypes.StringType)
                                .add(PlayVodMsgStructure.COUNTRY_CODE, DataTypes.StringType)
                )
                .add(PlayVodMsgStructure.PERSONA_ID, DataTypes.StringType)
                .add(PlayVodMsgStructure.OFFER_TYPE, DataTypes.StringType)
                .add(PlayVodMsgStructure.REQUEST_ID, DataTypes.StringType)
                .add(PlayVodMsgStructure.STREAMING_TICKET, DataTypes.StringType)
                .add(PlayVodMsgStructure.ORIGINATING_SYSTEM, DataTypes.StringType);
    }






}
