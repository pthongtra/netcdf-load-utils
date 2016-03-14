package no.uni.computing.lib.utils;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pat on 14/03/16.
 */
public class NetCDFUtil {

    public static Map<Integer, int[]> allOnes = new HashMap<Integer, int[]>();
    public static int[] allOnes1D = {1};
    public static int[] allOnes2D = {1, 1};
    public static int[] allOnes3D = {1, 1, 1};
    public static int[] allOnes4D = {1, 1, 1, 1};
    static {
        allOnes.put(1, allOnes1D);
        allOnes.put(2, allOnes2D);
        allOnes.put(3, allOnes3D);
        allOnes.put(4, allOnes4D);
    }

    public static long getCornerOffset(Variable var, int[] corner) throws Exception {
        try {
            return var.getLocalityInformation(corner, allOnes.get(corner.length)).getLong(0);
        }
        catch (InvalidRangeException e) {
            System.out.println("SOMETHING WRONG " + intArrayToString(corner));
            throw e;
        }
    }

    public static long flatten( int[] variableShape, int[] currentElement ) throws IOException {
        long[] strides = getStrides( variableShape);

        long retVal = 0;

        for( int i=0; i<currentElement.length; i++ ) {
            retVal += ( strides[i] * currentElement[i] );
        }

        return retVal;
    }

    public static long[] getStrides(int[] shape) throws IOException {
        long[] stride = new long[shape.length];
        long product = 1;
        for (int i = shape.length - 1; i >= 0; i--) {
            final int dim = shape[i];
            if (dim < 0)
                throw new IOException("Negative array size");
            stride[i] = product;
            product *= dim;
        }
        return stride;
    }

    public static String intArrayToString(int[] i) {
        String retString = "";
        int numElements = 0;
        for ( int ii : i ) {
            if ( numElements > 0) {
                retString += "," + String.valueOf(ii);
            } else {
                retString = String.valueOf(ii);
            }
            numElements++;
        }

        return retString;
    }

    public static int calcCurrentDim(long cellsLeft, long[] strides,
                                     int[] current, int[] varShape) {
        int retDim = -1;

        // check if we need to fill out any dimensions above zero
        for (int i = current.length - 1; i > 0; i--) {
            if (current[i] == 0) {
                continue; // no need to increment this level, it's full
            } else if (cellsLeft > strides[i]) {
                // dim is non-zero, non-full and there are sufficient cells left
                retDim = i;
                return retDim;
            }
        }

        // if we're still in this fucntion, start filling in, from dim-0 down
        for (int i = 0; i < current.length; i++) {
            if (strides[i] <= cellsLeft) {
                retDim = i;
                return retDim;
            }
        }

        // if we are here, something is wrong
        retDim = -1;
        return retDim;
    }

    public static String arrayToString( long[] array ) {
        String tempStr = "";

        for ( int i=0; i < array.length; i++) {
            if( i > 0)
                tempStr += ",";

            tempStr += array[i];
        }

        return tempStr;
    }
}
