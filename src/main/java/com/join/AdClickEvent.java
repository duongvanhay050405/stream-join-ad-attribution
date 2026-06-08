package com.join;

import java.io.Serializable;

public class AdClickEvent implements Serializable {
    public String clickId;
    public String impressionId;
    public long timestamp;

    public AdClickEvent() {}
    public AdClickEvent(String clickId, String impressionId, long timestamp) {
        this.clickId = clickId;
        this.impressionId = impressionId;
        this.timestamp = timestamp;
    }
    public String getImpressionId() { return impressionId; }
}