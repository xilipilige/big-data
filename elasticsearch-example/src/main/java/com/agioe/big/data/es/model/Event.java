package com.agioe.big.data.es.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import java.util.Date;

/**
 * @author yshen
 * @since 19-3-4
 */
@Document(indexName = "monitor",type = "event", shards = 2,replicas = 0)
public class Event {
    @Id
    private String id;

    @Field
    private String eventCode;

    @Field
    private String deviceCode;

    @Field
    private Date eventTime;

    @Field
    private double eventValue;

    @Field
    private String propertyCode;

    @Field
    private String eventNature;

    @Field
    private String eventState;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEventCode() {
        return eventCode;
    }

    public void setEventCode(String eventCode) {
        this.eventCode = eventCode;
    }

    public String getDeviceCode() {
        return deviceCode;
    }

    public void setDeviceCode(String deviceCode) {
        this.deviceCode = deviceCode;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public double getEventValue() {
        return eventValue;
    }

    public void setEventValue(double eventValue) {
        this.eventValue = eventValue;
    }

    public String getPropertyCode() {
        return propertyCode;
    }

    public void setPropertyCode(String propertyCode) {
        this.propertyCode = propertyCode;
    }

    public String getEventNature() {
        return eventNature;
    }

    public void setEventNature(String eventNature) {
        this.eventNature = eventNature;
    }

    public String getEventState() {
        return eventState;
    }

    public void setEventState(String eventState) {
        this.eventState = eventState;
    }
}
