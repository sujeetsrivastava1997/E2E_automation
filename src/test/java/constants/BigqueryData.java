package constants;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

public class BigqueryData {

    AtomicLong beforeInsertionCount;
    ZonedDateTime beforeModificationTime;

    public AtomicLong getBeforeInsertionCount() {
        return beforeInsertionCount;
    }

    public void setBeforeInsertionCount(AtomicLong beforeInsertionCount) {
        this.beforeInsertionCount = beforeInsertionCount;
    }

    public ZonedDateTime getBeforeModificationTime() {
        return beforeModificationTime;
    }

    public void setBeforeModificationTime(ZonedDateTime beforeModificationTime) {
        this.beforeModificationTime = beforeModificationTime;
    }

    @Override
    public String toString() {
        return "BigqueryData{" +
                "beforeInsertionCount=" + beforeInsertionCount +
                ", beforeModificationTime=" + beforeModificationTime +
                '}';
    }
}
