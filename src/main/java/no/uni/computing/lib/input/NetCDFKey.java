package no.uni.computing.lib.input;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by pat on 07.11.15.
 */
public class NetCDFKey implements Serializable {
    private Date splitDate;
    private int[] corner;
    private long length;
    private Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

    public NetCDFKey(Date splitDate, int[] corner, long length) {
        this.splitDate = splitDate;
        this.corner = corner;
        this.length = length;
    }

    @Override
    public String toString() {
        cal.setTime(splitDate);
        String dateStr = cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + cal.get(Calendar.DATE);
        String cornerStr = String.valueOf(corner.length);
        for (int i = 0; i < corner.length; i++)
            cornerStr += "-" + String.valueOf(corner[i]);

        String lengthStr = String.valueOf(length);

        return dateStr + "-" + lengthStr + "-" + cornerStr;
    }

    public Date getSplitDate() {
        return splitDate;
    }

    public void setSplitDate(Date splitDate) {
        this.splitDate = splitDate;
    }

    public int[] getCorner() {
        return corner;
    }

    public void setCorner(int[] corner) {
        this.corner = corner;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
