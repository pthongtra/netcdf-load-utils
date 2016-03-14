package no.uni.computing.lib.input;

import org.apache.hadoop.io.WritableComparable;
import ucar.ma2.Array;
import ucar.ma2.ArrayShort;
import ucar.ma2.ArrayFloat;
import ucar.ma2.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by pat on 06.11.15.
 */
public class NetCDFVarStripe implements WritableComparable, Serializable {

    private int[] corner;
    private int[] readShape;
    private long length;
    private short[] dataShort;
    private float[] dataFloat;

    public NetCDFVarStripe() {}

    public NetCDFVarStripe(int[] corner, int[] readShape, long length) {
        this.corner = new int[corner.length];
        for (int i=0; i<this.corner.length; i++)
            this.corner[i] = corner[i];

        this.readShape = new int[readShape.length];
        for (int i=0; i<this.readShape.length; i++)
            this.readShape[i] = readShape[i];

        this.length = new Long(length);

        this.dataShort = null;
        this.dataFloat = null;
    }

    public NetCDFVarStripe(int[] corner) {
        this.corner = new int[corner.length];
        for (int i=0; i<this.corner.length; i++)
            this.corner[i] = corner[i];
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        corner = new int[len];
        for (int i = 0; i < corner.length; i++)
            corner[i] = in.readInt();

        readShape = new int[len];
        for (int i = 0; i < readShape.length; i++)
            readShape[i] = in.readInt();

        length = in.readLong();

        int type = in.readInt();
        if (type == 1) {
            dataShort = new short[(int) in.readLong()];
            for (int i = 0; i < dataShort.length; i++)
                dataShort[i] = in.readShort();
        }
        else if (type == 2) {
            dataFloat = new float[(int) in.readLong()];
            for (int i = 0; i < dataFloat.length; i++)
                dataFloat[i] = in.readFloat();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(corner.length);
        for (int i = 0; i < corner.length; i++)
            out.writeInt(corner[i]);

        for (int i = 0; i < readShape.length; i++)
            out.writeInt(readShape[i]);

        out.writeLong(length);

        if (dataShort != null) {
            out.writeInt(1);
            out.writeLong(dataShort.length);
            for (int i = 0; i < dataShort.length; i++)
                out.writeShort(dataShort[i]);
        }
        else if (dataFloat != null) {
            out.writeInt(2);
            out.writeLong(dataFloat.length);
            for (int i = 0; i < dataFloat.length; i++)
                out.writeFloat(dataFloat[i]);
        }
        else
            out.writeInt(0);
    }

    public int compareTo(Object o) {
        NetCDFVarStripe other = (NetCDFVarStripe) o;

        if (this.length != other.length)
            return (int) (this.length - other.length);

        if (this.corner != null && other.corner != null && this.corner.length == other.corner.length)
            for (int i = 0; i < this.corner.length; i++) {
                if (this.corner[i] - other.corner[i] != 0)
                    return this.corner[i] - other.corner[i];
            }
        else if ((this.corner == null && other.corner != null) || (this.corner != null && other.corner == null) ||
                (this.corner.length != other.corner.length)) {
            return -1;
        }

        if (this.readShape != null && other.readShape != null && this.readShape.length == other.readShape.length)
            for (int i = 0; i < this.readShape.length; i++) {
                if (this.readShape[i] - other.readShape[i] != 0)
                    return this.readShape[i] - other.readShape[i];
            }
        else if ((this.readShape == null && other.readShape != null) || (this.readShape != null && other.readShape == null) ||
                (this.readShape.length != other.readShape.length)) {
            return -1;
        }

        if (this.dataShort != null && other.dataShort != null && this.dataShort.length == other.dataShort.length)
            for (int i = 0; i < this.dataShort.length; i++) {
                 if (this.dataShort[i] - other.dataShort[i] != 0)
                     return this.dataShort[i] - other.dataShort[i];
            }
        else if ((this.dataShort == null && other.dataShort != null) || (this.dataShort != null && other.dataShort == null) ||
                (this.dataShort.length != other.dataShort.length)) {
            return -1;
        }

        if (this.dataFloat != null && other.dataFloat != null && this.dataFloat.length == other.dataFloat.length)
            for (int i = 0; i < this.dataFloat.length; i++) {
                if (this.dataFloat[i] - other.dataFloat[i] != 0)
                    return (int) (this.dataFloat[i] - other.dataFloat[i]);
            }
        else if ((this.dataFloat == null && other.dataFloat != null) || (this.dataFloat != null && other.dataFloat == null) ||
                (this.dataFloat.length != other.dataFloat.length)) {
            return -1;
        }

        return 0;
    }


    public int[] getCorner() {
        return corner;
    }

    public int[] getReadShape() {
        return readShape;
    }

    public void setReadShape(int[] readShape) {
        this.readShape = new int[readShape.length];
        for (int i=0; i<this.readShape.length; i++)
            this.readShape[i] = readShape[i];
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = new Long(length);
    }

    public short[] getDataShortType() {
        return dataShort;

    }

    public float[] getDataFloatType() {
        return dataFloat;

    }

    public void setData(Array data, DataType datatype) throws IOException {
        if (data.getSize() > Integer.MAX_VALUE)
            throw new IOException("data size = " + data.getSize() + "is greater than Integer Max Value");

        if (datatype == DataType.SHORT) {
            this.dataShort = new short[(int) data.getSize()];
        }
        else if (datatype == DataType.FLOAT) {
            this.dataFloat = new float[(int) data.getSize()];
        }

        int[] index = new int[corner.length];
        int[] last = new int[corner.length];
        for (int i = 0; i < corner.length; i++) {
            index[i] = corner[i];
            last[i] = corner[i] + readShape[i];
        }

        if (datatype == DataType.SHORT) {
          for (int i = 0; i < length; i++) {
            if (corner.length == 4) {
                this.dataShort[i] = ((ArrayShort.D4) data).get(index[0] - corner[0], index[1] - corner[1], index[2] - corner[2], index[3] - corner[3]);
                if (index[3] < last[3] - 1) {
                    index[3] += 1;
                }
                else {
                    if (index[2] < last[2] - 1) {
                        index[2] += 1;
                        index[3] = corner[3];
                    }
                    else {
                        if (index[1] < last[1] - 1) {
                            index[1] += 1;
                            index[2] = corner[2];
                            index[3] = corner[3];
                        }
                        else {
                            index[0] += 1;
                            index[1] = corner[1];
                            index[2] = corner[2];
                            index[3] = corner[3];
                        }
                    }
                }
            }     
          } 
        } 
        else { //FLOAT 
          for (int i = 0; i < length; i++) {
            if (corner.length == 4) {
                this.dataFloat[i] = ((ArrayFloat.D4) data).get(index[0] - corner[0], index[1] - corner[1], index[2] - corner[2], index[3] - corner[3]);
                if (index[3] < last[3] - 1) {
                    index[3] += 1;
                }
                else {
                    if (index[2] < last[2] - 1) {
                        index[2] += 1;
                        index[3] = corner[3];
                    }
                    else {
                        if (index[1] < last[1] - 1) {
                            index[1] += 1;
                            index[2] = corner[2];
                            index[3] = corner[3];
                        }
                        else {
                            index[0] += 1;
                            index[1] = corner[1];
                            index[2] = corner[2];
                            index[3] = corner[3];
                        }
                    }
                }
            }
            else if (corner.length == 3) {

            }
            else if (corner.length == 2) {
                this.dataFloat[i] = (float) (Math.round(((ArrayFloat.D2) data).get(index[0] - corner[0], index[1] - corner[1]) * 100000) / 100000.0);
                if (index[1] < last[1] - 1) {
                    index[1] += 1;
                }
                else {
                    index[0] += 1;
                    index[1] = corner[1];
                }
            }
            else if (corner.length == 1) {
                this.dataFloat[i] = (float) (Math.round(((ArrayFloat.D1) data).get(i - corner[0]) * 1000) / 1000.0);
            }
          }
       }
    }
}
