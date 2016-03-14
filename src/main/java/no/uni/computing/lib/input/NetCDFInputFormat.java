package no.uni.computing.lib.input;

import no.uni.computing.lib.utils.InputConfig;
import no.uni.computing.lib.utils.NetCDFUtil;
import no.uni.computing.lib.utils.WRFFile;
import no.uni.computing.io.NcHdfsRaf;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by pat on 03.11.15.
 */
public class NetCDFInputFormat extends FileInputFormat {
    private static final Log LOG = LogFactory.getLog(NetCDFInputFormat.class);

    private Integer startPoint_X; //default 0
    private Integer range_X; //default 10
    private Integer startPoint_Y; //default 0
    private Integer range_Y; //default 10
    private Integer interestedZone;
    private Date startDate = null, endDate = null;
    //private int noOfSplitInCol;
    private List<String> inputVar;
    private static int dimSize;
    private static String timesVarSteps;

    private boolean initConf = false;
    boolean filter = true;

    public NetCDFInputFormat() {

    }

    public NetCDFInputFormat(String[] input) throws Exception {
        //System.out.println("NetCDFFileInputFormat Initail " + StringNetCDFUtil.join(input));
        Properties inputProp = new Properties();
        for (int i = 0; i < input.length; i+= 2) {
            if (InputConfig.isValidKey(input[i]));
            inputProp.setProperty(input[i].toUpperCase(), input[i+1]);
        }
        initialConfigurationValue(inputProp);
        initConf = true;
    }

    //with filter start corner, zone, time period can be defined. without filter take all files
    private void initialConfigurationValue(Properties inputProp) throws Exception {
        filter = inputProp.getProperty("filter") != null ? Boolean.valueOf(inputProp.getProperty("filter")) : true;
        if (filter) {
            startPoint_X = WRFFile.getStartPoint(inputProp.getProperty(InputConfig.WEST_EAST_INPUT));
            range_X = WRFFile.getRange(inputProp.getProperty(InputConfig.WEST_EAST_INPUT)); //If not defined, it will take full chunk
            startPoint_Y = WRFFile.getStartPoint(inputProp.getProperty(InputConfig.SOUTH_NORTH_INPUT));
            range_Y = WRFFile.getRange(inputProp.getProperty(InputConfig.SOUTH_NORTH_INPUT)); //If not defined, it will take full chunk

            //do not condiser z param, take them all
            String zoneStr = inputProp.getProperty(InputConfig.ZONE_INPUT);
            if (zoneStr == null || !StringUtils.isNumeric(zoneStr) || zoneStr.equalsIgnoreCase("all"))
                interestedZone = 0;
            else
                interestedZone = Integer.valueOf(zoneStr);

            startDate = WRFFile.getStartDate(inputProp.getProperty(InputConfig.DATE_INPUT));
            endDate = WRFFile.getEndDate(inputProp.getProperty(InputConfig.DATE_INPUT));
        }
        else {
            startPoint_X = 1;
            range_X = null;
            startPoint_Y = 1;
            range_Y = null;

            interestedZone = 0;
            startDate = null;
            endDate = null;
        }

        if (inputProp.getProperty(InputConfig.VARIABLE_INPUT) == null)
            throw new Exception("VAR for interested variables must be defined");

        String[] tmp = inputProp.getProperty(InputConfig.VARIABLE_INPUT).split(",");
        inputVar = Arrays.asList(tmp);

        String tmpDimSize = inputProp.getProperty("DIM");
        if (tmpDimSize == null || !StringUtils.isNumeric(tmpDimSize)) {
            if (tmpDimSize.contains("HR") && tmpDimSize.replace("HR", "").equals("4")) {
                dimSize = 0; //4HR
            }
            else
                dimSize = 4; //by default
        }
        else
            dimSize = Integer.valueOf(tmpDimSize);
    }

    private Properties fetchInputFormatConfJobConf(Configuration conf) {
        Properties inputProp = new Properties();
        String[] keys = new String[]{ InputConfig.WEST_EAST_INPUT, InputConfig.SOUTH_NORTH_INPUT, InputConfig.ZONE_INPUT, InputConfig.VARIABLE_INPUT,
                InputConfig.DATE_INPUT, "DIM", "filter"};
        for (String key : keys) {
            inputProp.setProperty(key, conf.get(key) != null ? conf.get(key) : "");
        }
        return inputProp;
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        if (!initConf) {
            Properties inputProp = fetchInputFormatConfJobConf(job.getConfiguration());
            try {
                initialConfigurationValue(inputProp);
            } catch (Exception e) {
                throw new IOException(e);
            }
            initConf = true;
        }

        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        Collections.sort(files);

        //this solution is for the case all variables considering has the SAME dimensions & data type size
        int[] shape = null;
        int dataTypeSize;
        int dims;
        long[] strides = null;

        //1) look at corners of each var in each block, union and sorted
        //the sorted list will be shortest segment that can be merged later
        Map<Long, List<Object>> sortedCorner = new TreeMap<Long, List<Object>>(); //list key contains corner, length, blkind
        Map<String, Map<Long, Integer>> varIndToBlkInd = new TreeMap<String, Map<Long, Integer>>();
        int fIndex = 1;
        for (FileStatus f : files) {
            NcHdfsRaf raf = new NcHdfsRaf(f, job.getConfiguration());
            NetcdfFile ncfile = NetcdfFile.open(raf, f.getPath().toString());
            long blockSize = f.getBlockSize();
            for (String varName : inputVar) {
                Variable var = ncfile.findVariable(varName);
                shape = var.getShape();
                dataTypeSize = var.getDataType().getSize();
                dims = var.getDimensions().size();
                strides = NetCDFUtil.getStrides(shape);
                try {
                    //look at corners of each var in each block, union and sorted
                    //each in the sorted list will be shortest segment that can be merged later
                    findVarCorner(fIndex, var, shape, dims, dataTypeSize, strides, blockSize, sortedCorner, varIndToBlkInd);
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                    e.printStackTrace();
                }
            }
            fIndex++;
            ncfile.close();
            raf.close();
        }

        //2) update length of each segment in the sorted list
        long prevInd = 0;
        for (Long ind : sortedCorner.keySet()) {
            if (ind != 0) {
                List<Object> prevL = sortedCorner.get(prevInd);
                prevL.set(1, ind - prevInd); //update length after sorted
            }
            prevInd = ind;
        }

        //3) set reading shape in each segment (!!! this part is needed as netcdf api provides reading in one dimension!!!)
        Map<Long, List<NetCDFVarStripe>> sortedCornerReadShape = new TreeMap<Long, List<NetCDFVarStripe>>();
        for (Long ind : sortedCorner.keySet()) {
            LOG.debug("index " + ind + " corner " + NetCDFUtil.intArrayToString((int[]) sortedCorner.get(ind).get(0)) + " length " + sortedCorner.get(ind).get(1));
            int[] startCorner = new int[((int[]) sortedCorner.get(ind).get(0)).length];
            for (int i=0; i < startCorner.length; i++)
                startCorner[i] = ((int[]) sortedCorner.get(ind).get(0))[i];
            long length = (Long) sortedCorner.get(ind).get(1);
            sortedCornerReadShape.put(ind, new ArrayList<NetCDFVarStripe>());
            long readShapeLength;
            while (length > 0) {
                int currDimNonFull = NetCDFUtil.calcCurrentDim(length, strides, startCorner, shape);
                if (currDimNonFull < 0) {
                    LOG.error("There is something wrong with split, corner: " + NetCDFUtil.intArrayToString(startCorner) +
                            " stride: " + NetCDFUtil.arrayToString(strides) +
                            " shape: " + NetCDFUtil.intArrayToString(shape) + " length: " + length) ;
                }

                int steps = (int) Math.min(shape[currDimNonFull] - startCorner[currDimNonFull],
                        length / strides[currDimNonFull]);

                int[] readShape = new int[startCorner.length];
                // initial to all ones, will be updated later
                for (int i = 0; i < startCorner.length; i++)
                    readShape[i] = 1;

                readShape[currDimNonFull] = steps;
                NetCDFVarStripe varStripe = new NetCDFVarStripe(startCorner);
                String c = NetCDFUtil.intArrayToString(startCorner);

                startCorner[currDimNonFull] += readShape[currDimNonFull];
                // zero out the higher dimensions
                for (int i = currDimNonFull + 1; i < startCorner.length; i++) {
                    startCorner[i] = 0;
                    readShape[i] = shape[i];
                }
                readShapeLength = (steps * strides[currDimNonFull]);
                varStripe.setReadShape(readShape);
                varStripe.setLength(readShapeLength);
                sortedCornerReadShape.get(ind).add(varStripe);
                LOG.debug("\t\tvarStripe corner: " + NetCDFUtil.intArrayToString(varStripe.getCorner()) +
                        " readshape: " + NetCDFUtil.intArrayToString(varStripe.getReadShape()) + " len: " + varStripe.getLength());

                for (int i = startCorner.length - 1; i > 0; i--) {
                    if (startCorner[i] == shape[i]) {
                        startCorner[i - 1]++;
                        startCorner[i] = 0;
                    }
                }
                length -= readShapeLength;
            }
        }

        //4) generate block list, for each physical block
        Map<String, NetCDFInputSplit> allBLKs = new HashMap<String, NetCDFInputSplit>();
        //Map<String, List<NetCDFVarBlock>> allBLKs = new HashMap<String, List<NetCDFVarBlock>>();
        fIndex = 1;
        for (FileStatus f : files) {
            FileSystem fs = f.getPath().getFileSystem(job.getConfiguration());
            BlockLocation[] blocks = fs.getFileBlockLocations(f, 0, f.getLen());
            for (String varName : inputVar) {
                for (Long ind : sortedCorner.keySet()) {
                    int[] startCorner = (int[]) sortedCorner.get(ind).get(0);
                    long length = (Long) sortedCorner.get(ind).get(1);
                    Map<Long, Integer> indToBlkInd = varIndToBlkInd.get(varName + fIndex);
                    long varInd = NetCDFUtil.flatten(shape, startCorner);
                    int blkInd = 0;
                    for (Long l : indToBlkInd.keySet())
                        if (varInd >= l)
                            blkInd = indToBlkInd.get(l);

                    List<NetCDFVarStripe> allReadShape = sortedCornerReadShape.get(ind);
                    try {
                        String key = f.getPath().toString() + blkInd;
                        if (!allBLKs.containsKey(key))
                            allBLKs.put(key, new NetCDFInputSplit(f.getPath(), blocks[blkInd].getHosts()));
                            //allBLKs.put(key, new ArrayList<NetCDFVarBlock>());

                        allBLKs.get(key).addVarBlock(new NetCDFVarBlock(varName, startCorner, length, allReadShape));
                        //allBLKs.get(key).add(new NetCDFVarBlock(varName, startCorner, length, allReadShape));
                    } catch (Exception e) {
                        LOG.error(e.getMessage());
                        e.printStackTrace();
                    }

                }
            }
            fIndex++;
        }

        //5) pop up NetCDFInputSplit into splits
        List<NetCDFVarBlock> tmp = new ArrayList<NetCDFVarBlock>();
        for (String key : allBLKs.keySet()) {
            try {
                LOG.debug("an input split len: " + allBLKs.get(key).getLength() + " num varBlk: " + allBLKs.get(key).getNetCDFVarBlockArray().size());
            }
            catch (Exception e) {}
            splits.add(allBLKs.get(key));
            /*long totalLength = 0;
            for (int i = 0; i < allBLKs.get(key).size(); i++) {
                totalLength += allBLKs.get(key).get(i).getLength();
                tmp.add(allBLKs.get(key).get(i));
                if (totalLength >= 23400000) {
                    splits.add(new NetCDFInputSplit(f.getPath(), blocks[blkInd].getHosts()));
                }
            }*/

        }

        //For debug. Go through all NetCDFInputSplit, NetCDFVarBlock, NetCDFVarStripe
        if (LOG.isDebugEnabled()) {
            try {
                for (InputSplit s : splits) {
                    NetCDFInputSplit ns = (NetCDFInputSplit) s;
                    LOG.debug("Split path: " + ns.getPath() + " len: " + ns.getLength());
                    for (NetCDFVarBlock varBlk : ns.getNetCDFVarBlockArray()) {
                        LOG.debug("\tvar: " + varBlk.getVarName() +
                                " corner: " + NetCDFUtil.intArrayToString(varBlk.getStartCorner()) + " len: " + varBlk.getLength());
                        String vsTxt = "";
                        for (NetCDFVarStripe varStripe : varBlk.getStripes()) {
                            vsTxt += "corner: " + NetCDFUtil.intArrayToString(varStripe.getCorner()) + " readshape: " +
                                    NetCDFUtil.intArrayToString(varStripe.getReadShape()) + " len: " + varStripe.getLength() + " ,";
                        }
                        LOG.debug("\t\t" + vsTxt);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        LOG.info("Input splits to process : " + splits.size());
        //splits.clear();
        return splits;
    }

    private void findVarCorner(int fIndex, Variable var, int[] shape, int dims, int dataTypeSize, long strides[],
        long blockSize, Map<Long, List<Object>> sortedCorner, Map<String, Map<Long, Integer>> varIndToBlkInd) throws Exception {

        long cellVarLeft = 1;
        int startCorner[] = new int[dims];
        for (int i = 0; i < dims; i++) {
            startCorner[i] = 0;
            cellVarLeft *= shape[i];
        }

        varIndToBlkInd.put(var.getName() + fIndex, new TreeMap<Long, Integer>());
        while (cellVarLeft > 0) {
            long currOffset = NetCDFUtil.getCornerOffset(var, startCorner);
            int currBlkInd = (int) (currOffset / blockSize);
            long bytesInBlockLeft = blockSize - (currOffset % blockSize);
            long cellsInBlockLeft = bytesInBlockLeft / dataTypeSize;
            //!!! When length is very big, some tasks work much harder than others
            long length = Math.min(cellsInBlockLeft, cellVarLeft);
            if (length > 2340000)
                length = 2340000;
            long currVarInd = NetCDFUtil.flatten(shape, startCorner);

            //update all sorted corners
            List<Object> cornerLength = new ArrayList<Object>();
            cornerLength.add(startCorner);
            cornerLength.add(length);
            sortedCorner.put(currVarInd, cornerLength);

            //update all var index to blk index
            varIndToBlkInd.get(var.getName() + fIndex).put(currVarInd, currBlkInd);

            cellVarLeft -= length;
            if (cellVarLeft > 0) {
                startCorner = calcNextCorner(startCorner, strides, length, shape);
            }
        }
    }

    private int[] calcNextCorner(int[] currCorner, long[] strides, long length, int[] shape) {
        int nextCorner[] = new int[currCorner.length];
        int stridesStep[] = new int[currCorner.length];
        for (int i = 0; i <= currCorner.length - 1; i++) {
            nextCorner[i] = currCorner[i];
            stridesStep[i] = 0;
        }


        int currInd = 0;
        while (length > 0 && currInd <= currCorner.length - 1) {
            for (int i = currInd; i <= currCorner.length - 1; i++) {
                if (length > strides[i]) {
                    stridesStep[i] = (int) (length/strides[i]);
                    length -= stridesStep[i] * strides[i];
                    currInd = i+1;
                }
            }
        }

        for (int i = 0; i <= currCorner.length - 1; i++) {
            nextCorner[i] = currCorner[i] + stridesStep[i];
            if (nextCorner[i] >= shape[i]) {
                nextCorner[i] -= shape[i];
                nextCorner[i-1]++;
            }
        }

        return nextCorner;
    }

    private static Pattern p = Pattern.compile(".*NorKyst-800m_ZDEPTHS_avg.an.([0-9]{4})([0-9]{2})([0-9]{2})([0-9]*).nc");
    //private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public RecordReader<String, NetCDFVarBlock> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        NetCDFRecordReader reader = new NetCDFRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    public static void main(String args[]) throws Exception {
        int[] shape = {1, 17, 900, 2600};
        System.out.println(NetCDFUtil.intArrayToString(shape));
        System.out.println(NetCDFUtil.arrayToString(NetCDFUtil.getStrides(shape)));

        int[] stride = new int[shape.length];
        int product = 1;
        for (int i = 0; i <= shape.length - 1; i++) {
            final int dim = shape[i];
            stride[i] = product;
            product *= dim;
        }
        System.out.println(NetCDFUtil.intArrayToString(stride));
    }
}
