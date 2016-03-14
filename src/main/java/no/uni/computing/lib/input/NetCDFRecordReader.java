package no.uni.computing.lib.input;

import no.uni.computing.io.NcHdfsRaf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by pat on 07.11.15.
 */
public class NetCDFRecordReader extends RecordReader<String, NetCDFVarBlock> {

    private static final Log LOG = LogFactory.getLog(NetCDFRecordReader.class);
    private static Pattern p = Pattern.compile(".*NorKyst-800m_ZDEPTHS_avg.an.([0-9]{4})([0-9]{2})([0-9]{2})([0-9]*).nc");
    private static Pattern pRWF = Pattern.compile(".*wrfout_d0[1|2]_([0-9]{4})-([0-9]{2})-([0-9]{2})_[0-9]+"); 

    private NetCDFInputSplit split;
    private int varBlkInd;
    private Date splitDate;
    private String fileSystemConf;
    private boolean readData = false;
    private Map<String, Array> data;

    public NetCDFRecordReader() {}

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        //split of this record reader is NetCDFInputSplit
        split = (NetCDFInputSplit) genericSplit;
        fileSystemConf = context.getConfiguration().get("fs.defaultFS");
        //extract date, month, year from filename
        Matcher m1 = p.matcher(((NetCDFInputSplit) split).getPath().getName());
        Matcher m2 = pRWF.matcher(((NetCDFInputSplit) split).getPath().getName()); 
        String dateStr = "";
        if (m1.matches() || m2.matches()) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            try {
                dateStr = m1.matches() ? m1.group(1) + "-" + m1.group(2) + "-" + m1.group(3) :
			m2.group(1) + "-" + m2.group(2) + "-" + m2.group(3);
                splitDate = sdf.parse(dateStr);
            }
            catch (Exception e) {
                e.printStackTrace();
                throw new IOException("Parsing date from filename error, file " + ((NetCDFInputSplit) split).getPath().getName() + ", parsed value " + dateStr);
            }
        }
        else {
            throw new IOException("Parsing date from filename error, file " + ((NetCDFInputSplit) split).getPath().getName()); 
        } 
        varBlkInd = -1;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (varBlkInd < split.getNetCDFVarBlockArray().size() - 1) {
            varBlkInd++;
            return true;
        }
        else
            return false;
    }

    @Override
    public String getCurrentKey() {
        NetCDFVarBlock varBlk = split.getNetCDFVarBlockArray().get(varBlkInd);
        return new NetCDFKey(splitDate, varBlk.getStartCorner(), varBlk.getLength()).toString();
    }

    @Override
    public NetCDFVarBlock getCurrentValue() throws IOException, InterruptedException {
        //fetch data for the first time get value
        if (!readData)
            fetchData();

        return split.getNetCDFVarBlockArray().get(varBlkInd);
    }

    private void fetchData() throws IOException {
        data = new HashMap<String, Array>();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", fileSystemConf);
        FileSystem fs = split.getPath().getFileSystem(conf);
        NcHdfsRaf raf = new NcHdfsRaf(fs.getFileStatus(split.getPath()), conf);
        NetcdfFile ncfile = NetcdfFile.open(raf, split.getPath().toString());
        try {
            for (NetCDFVarBlock varBlk : split.getNetCDFVarBlockArray()) {
                Variable var = ncfile.findVariable(varBlk.getVarName());
                for (NetCDFVarStripe varStripe : varBlk.getStripes()) {
                    //fill in data in each var stripe
                    varStripe.setData(var.read(varStripe.getCorner(), varStripe.getReadShape()), var.getDataType());
                }
            }
        }
        catch (InvalidRangeException e) {
            throw new IOException(e);
        }
        finally {
            ncfile.close();
            raf.close();
        }
        readData = true;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return varBlkInd/split.getNetCDFVarBlockArray().size();
    }

    /*private String getKeyForVarStrip(Date date, NetCDFVarStripe varStripe) {
        return splitDate.toString() + Utils.intArrayToString(varStripe.getCorner()) + Utils.intArrayToString(varStripe.getReadShape());
    }*/
    public static void main(String[] args) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String d = "NorKyst-800m_ZDEPTHS_avg.an.2015010612.nc";
        //extract date, month, year from filename
        Matcher m1 = p.matcher(d);
        if (m1.matches()) {
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            try {
                Date dd = sdf.parse(m1.group(1) + "-" + m1.group(2) + "-" + m1.group(3));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            Date dd1 = sdf.parse("2015-10-31");
            System.out.println(dd1.toString());
            Date dd2 = sdf.parse("2015" + "-" + "10" + "-" + "25");
            System.out.println(dd2.toString());
            Date dd3 = sdf.parse("2012" + "-" + "07" + "-" + "11");
            System.out.println(dd3.toString());
            Date dd4 = sdf.parse("2012" + "-" + "06" + "-" + "27");
            System.out.println(dd4.toString());
        }
        catch (Exception e) {}
    }

}
