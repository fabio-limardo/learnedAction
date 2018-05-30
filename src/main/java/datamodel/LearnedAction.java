package datamodel;

import java.io.Serializable;

public class LearnedAction implements Serializable {
    private String territory;
    private String proposition;
    private String timestamp;
    private String actionType;
    private String sourceId;
    private String deviceId;
    private String uuid;
    private String language;
    private String videoId;
    private String genre;
    private String channel;

    public LearnedAction(
            String territory,
            String proposition,
            String timestamp,
            String actionType,
            String sourceId,
            String deviceId,
            String uuid,
            String language,
            String videoId,
            String genre,
            String channel){
        this.territory = territory;
        this.proposition = proposition;
        this.timestamp = timestamp;
        this.actionType = actionType;
        this.sourceId = sourceId;
        this.deviceId = deviceId;
        this.uuid = uuid;
        this.language = language;
        this.videoId = videoId;
        this.genre = genre;
        this.channel = channel;

    }


    public String getTerritory() {
        return territory;
    }

    public void setTerritory(String territory) {
        this.territory = territory;
    }

    public String getProposition() {
        return proposition;
    }

    public void setProposition(String proposition) {
        this.proposition = proposition;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getVideoId() {
        return videoId;
    }

    public void setVideoId(String videoId) {
        this.videoId = videoId;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }


}
