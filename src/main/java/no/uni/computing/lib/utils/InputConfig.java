package no.uni.computing.lib.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputConfig {
	
	public final static String WEST_EAST_INPUT = "X";
	public final static String BOTTOM_TOP_INPUT = "Z";
	public final static String SOUTH_NORTH_INPUT = "Y";
	public final static String VARIABLE_INPUT = "VAR";
	public static final String ZONE_INPUT = "ZONE";
	public static final String DATE_INPUT = "DATE";
	public static final String NOSPLIT_INPUT = "NOSPLIT";
	
	public final static String WEST_EAST_DIMNAME = "west_east";
	public final static String BOTTOM_TOP_DIMNAME = "bottom_top";
	public final static String SOUTH_NORTH_DIMNAME = "south_north";
	
	public final static String Y_DIMNAME = "y";
	public final static String X_DIMNAME = "x";
	
	public final static String TIME_DIMNAME = "Time";
	public static final String TIME_VARNAME = "Times";
	
	public final static List<String> inputKey;
	
	

	static {
		inputKey = new ArrayList<String>();
		inputKey.add(WEST_EAST_INPUT);
		inputKey.add(BOTTOM_TOP_INPUT);
		inputKey.add(SOUTH_NORTH_INPUT);
		inputKey.add(VARIABLE_INPUT);
		inputKey.add("DIM");
	}
	
	public static boolean isValidKey(String key) {
		return inputKey.contains(key.toUpperCase());
	}
	
}
