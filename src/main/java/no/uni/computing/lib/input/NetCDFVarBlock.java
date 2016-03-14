package no.uni.computing.lib.input;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pat on 05.11.15.
 */
public class NetCDFVarBlock implements WritableComparable, Serializable {

    private String varName;
    private List<NetCDFVarStripe> stripes;

    private int[] startCorner;  //maybe delete
    private long length;        //maybe delete

    public NetCDFVarBlock() {}

    public NetCDFVarBlock(String varName, int[] startCorner, long length, List<NetCDFVarStripe> stripes) {
        this.varName = new String(varName);
        this.startCorner = copyArray(startCorner);
        this.length = new Long(length);
        this.stripes = new ArrayList<NetCDFVarStripe>(stripes);
    }

    private int[] copyArray(int[] in) {
        int[] out = new int[in.length];
        for (int i=0; i < in.length; i++)
            out[i] = in[i];
        return out;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        varName = Text.readString(in);

        int len = in.readInt();
        startCorner = new int[len];
        for (int i = 0; i < startCorner.length; i++)
            startCorner[i] = in.readInt();

        length = in.readLong();

        int listSize = in.readInt();
        stripes = new ArrayList<NetCDFVarStripe>(listSize);
        for (int i=0; i< listSize; i++) {
            NetCDFVarStripe varStripe = new NetCDFVarStripe();
            varStripe.readFields(in);
            stripes.add(varStripe);
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, varName);

        out.writeInt(startCorner.length);
        for (int i = 0; i < startCorner.length; i++)
            out.writeInt(startCorner[i]);

        out.writeLong(length);

        out.writeInt(stripes.size());
        for (int i=0; i< stripes.size(); i++) {
            NetCDFVarStripe varStripe = stripes.get(i);
            varStripe.write(out);
        }
    }

    public int compareTo(Object o) {
        NetCDFVarBlock other = (NetCDFVarBlock) o;
        if (other == null) return -1;

        if ( 0 != this.varName.compareTo(other.getVarName())){
            return this.varName.compareTo(other.getVarName());
        }

        if (this.length != other.length)
            return (int) (this.length - other.length);

        if (this.startCorner != null && other.startCorner != null && this.startCorner.length == other.startCorner.length)
            for (int i = 0; i < this.startCorner.length; i++) {
               if (this.startCorner[i] - other.startCorner[i] != 0)
                    return this.startCorner[i] - other.startCorner[i];
            }
        else if ((this.startCorner == null && other.startCorner != null) || (this.startCorner != null && other.startCorner == null) ||
                (this.startCorner.length != other.startCorner.length)) {
            return -1;
        }

        if (this.stripes != null && other.stripes != null && this.stripes.size() == other.stripes.size()) {
            for (int i = 0; i < this.stripes.size(); i++) {
                if (this.stripes.get(i).compareTo(other.stripes.get(i)) != 0)
                    return this.stripes.get(i).compareTo(other.stripes.get(i));
            }
        }
        else if ((this.stripes == null && other.stripes != null) || (this.stripes != null && other.stripes == null) ||
                (this.stripes.size() != other.stripes.size())) {
            return -1;
        }

        return 0;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public int[] getStartCorner() {
        return startCorner;
    }

    public void setStartCorner(int[] startCorner) {
        this.startCorner = startCorner;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public List<NetCDFVarStripe> getStripes() {
        return stripes;
    }

    public void setStripes(List<NetCDFVarStripe> stripes) {
        this.stripes = stripes;
    }
}
