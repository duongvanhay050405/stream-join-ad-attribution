package com.join;

import java.io.Serializable;

public class AdImpressionEvent implements Serializable {
    public String impressionId;
    public String userId;
    public String adId;
    public long timestamp;

    public AdImpressionEvent() {}
    public AdImpressionEvent(String impressionId, String userId, String adId, long timestamp) {
        this.impressionId = impressionId;
        this.userId = userId;
        this.adId = adId;
        this.timestamp = timestamp;
    }
    public String getImpressionId() { return impressionId; }
}