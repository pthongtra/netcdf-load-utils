package no.uni.computing.lib.input.extend;

import ucar.ma2.Array;
import ucar.ma2.ArrayShort;
import ucar.ma2.Index;
import ucar.ma2.Index4D;

import java.io.Serializable;

/**
 * Created by pat on 07.11.15.
 */
public class ArrayShortD4 extends ArrayShort.D4 implements Serializable {

    public ArrayShortD4(int len0, int len1, int len2, int len3) {
        super(len0, len1, len2, len3);
    }

}
