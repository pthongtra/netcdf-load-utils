package no.uni.computing.lib.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pat on 05.11.15.
 */
public class NetCDFInputSplit extends InputSplit implements Writable {

    private Path path;
    private String[] hosts;
    private List<NetCDFVarBlock> varBlKArray;
    private long length;

    public NetCDFInputSplit() {}

    public NetCDFInputSplit(Path path, String[] hosts) {
        this.path = new Path(path.toString());;
        this.hosts = new String[hosts.length];
        for (int i=0; i<this.hosts.length; i++)
            this.hosts[i] = hosts[i];

        varBlKArray = new ArrayList<NetCDFVarBlock>();
    }

    public void addVarBlock(NetCDFVarBlock varBlk) {
        varBlKArray.add(varBlk); //new NetCDFVarBlock(varBlk.getVarName(), varBlk.getStartCorner(), varBlk.getLength(), varBlk.getStripes()));
        length += varBlk.getLength();
    }

    public Path getPath() {
        return path;
    }

    public List<NetCDFVarBlock> getNetCDFVarBlockArray() {
        return varBlKArray;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        if (null == hosts)
            return new String[]{};
        else
            return hosts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, path.toString());
        out.writeInt(varBlKArray.size());
        for (int i=0; i<varBlKArray.size(); i++) {
            NetCDFVarBlock varBlk = varBlKArray.get(i);
            varBlk.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(Text.readString(in));
        int listSize = in.readInt();
        varBlKArray = new ArrayList<NetCDFVarBlock>(listSize);
        for (int i=0; i< listSize; i++) {
            NetCDFVarBlock varBlk = new NetCDFVarBlock();
            varBlk.readFields(in);
            varBlKArray.add(varBlk);
        }
    }

    public int compareTo(Object o) {
        NetCDFInputSplit other = (NetCDFInputSplit) o;
        if (other == null) return -1;

        if (!this.getPath().equals(other.getPath()))
            return this.getPath().compareTo(other.getPath());

        if (this.hosts != null && other.hosts != null && this.hosts.length == other.hosts.length) {
            for (int i = 0; i < this.hosts.length; i++) {
                if (this.hosts[i].compareTo(other.hosts[i]) != 0)
                    return this.hosts[i].compareTo(other.hosts[i]);
            }
        }
        else if ((this.hosts == null && other.hosts != null) || (this.hosts != null && other.hosts == null) ||
                (this.hosts.length != other.hosts.length)) {
            return -1;
        }

        if (this.varBlKArray != null && other.varBlKArray != null && this.varBlKArray.size() == other.varBlKArray.size()) {
            for (int i = 0; i < this.varBlKArray.size(); i++) {
                if (this.varBlKArray.get(i).compareTo(other.varBlKArray.get(i)) != 0)
                    return this.varBlKArray.get(i).compareTo(other.varBlKArray.get(i));
            }
        }
        else if ((this.varBlKArray == null && other.varBlKArray != null) || (this.varBlKArray != null && other.varBlKArray == null) ||
                (this.varBlKArray.size() != other.varBlKArray.size())) {
            return -1;
        }

        return 0;
    }
}
