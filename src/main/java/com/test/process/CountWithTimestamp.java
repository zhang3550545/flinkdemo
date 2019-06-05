package com.test.process;

import org.apache.commons.lang3.time.FastDateFormat;

public class CountWithTimestamp {
    public Integer key;
    public long count;
    public long lastModified;

    @Override
    public String toString() {
        return "CountWithTimestamp{" +
                "key='" + key + '\'' +
                ", count=" + count +
                ", lastModified=" + FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(lastModified) +
                '}';
    }
}
