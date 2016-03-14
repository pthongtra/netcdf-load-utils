package no.uni.computing.lib.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import no.uni.computing.io.NcHdfsRaf;

import no.uni.computing.lib.utils.InputConfig;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class WRFFile {
	
	public static SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

	public static int getStartPoint(String inputValue) {
		if (inputValue != null && !inputValue.isEmpty()) {
			String[] tmp = inputValue.split(",");
			if (tmp != null) {
				if (tmp.length > 0 && tmp[0] != null && !tmp[0].isEmpty())
					return Integer.valueOf(tmp[0]) - 1;
			}		
		}
		return 0;
	}

	public static Integer getRange(String inputValue) {
		if (inputValue != null && !inputValue.isEmpty()) {
			String[] tmp = inputValue.split(",");
			if (tmp != null) {
				if (tmp.length > 1 && tmp[1] != null && !tmp[1].isEmpty())
					return Integer.valueOf(tmp[1]);
			}		
		}
		return null; //For full chunk
	}
	
	public static Date getStartDate(String inputValue) {
		if (inputValue != null && !inputValue.trim().isEmpty()) {
			String[] tmp = inputValue.split(",");
			if (tmp != null) {
				try {
					if (tmp.length > 0 && tmp[0] != null && !tmp[0].isEmpty())
						return formatDate.parse(tmp[0]);
				} catch (ParseException e) {
					//e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	public static Date getEndDate(String inputValue) {
		if (inputValue != null && !inputValue.trim().isEmpty()) {
			String[] tmp = inputValue.split(",");
			if (tmp != null) {
				try {
					if (tmp.length > 1 && tmp[1] != null && !tmp[1].isEmpty())
						return formatDate.parse(tmp[1]);
				} catch (ParseException e) {
					//e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	public static boolean isFileInDateRange(String absoluteFilename, Date startDate, Date endDate) {
		String filename = absoluteFilename.substring(absoluteFilename.lastIndexOf("/") + 1);
		final Pattern pattern = Pattern.compile("^.*_.*_(.*)_.*$");
        final Matcher matcher = pattern.matcher(filename);
        String inputFileDate = null;
        if (matcher.find())
        	inputFileDate = matcher.group(1);
        if (inputFileDate != null) {
        	Date inputFileDateObj;
			try {
				inputFileDateObj = formatDate.parse(inputFileDate);
				if (startDate != null && endDate != null && (inputFileDateObj.compareTo(startDate) < 0 || inputFileDateObj.compareTo(endDate) > 0)) 
	        		return false;
	        	
	        	if (startDate != null && inputFileDateObj.compareTo(startDate) < 0) 
	        		return false;
				
				if (endDate != null && inputFileDateObj.compareTo(endDate) > 0) 
					return false;
			} catch (ParseException e) {
				System.out.println("************** Check if File in specified Date Range FAILED. This file " + absoluteFilename + " is considered. **************");
				e.printStackTrace();
				System.out.println("\n\n");
			}
        }
        
        return true;
	}
	
	public static boolean isFileInZone(Integer interestedZone, Integer gridZone) {
		if (gridZone == null)
			return false;
		if ((interestedZone != null && interestedZone > 0 && interestedZone.intValue() == gridZone.intValue()) || (interestedZone == null) || (interestedZone == 0)) 
			return true;
		return false;
	}
		
	public static Integer getGridZone(String absoluteFilename) {
		String filename = absoluteFilename.substring(absoluteFilename.lastIndexOf("/") + 1);
		int index = filename.indexOf("_d0") + 3;
		try {
			return Integer.valueOf(filename.substring(index, index + 1));
		}
		catch (NumberFormatException e) { 
			System.out.println("Skip File: " + absoluteFilename);
			return null;
		}
	}
	
	public static List<Variable> getVariable(NetcdfFile ncfile, List<String> requiredVar) {
		List<Variable> returnVar = new ArrayList<Variable>();
		List<Variable> variables = ncfile.getVariables();
		for (Variable variable : variables) {
			// skip all but the variable we want to measure here
			if (requiredVar.contains(variable.getName()))
				returnVar.add(variable);
		}
		
		return returnVar;
	}
	
	public static NetcdfFile openFile(NcHdfsRaf raf, String absoluteFilename) {
		NetcdfFile ncfile = null;
		while (ncfile == null) { //loop to open the file
			try {
				ncfile = NetcdfFile.open(raf, absoluteFilename);
				return ncfile; 
			}
			catch (IOException e) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		return ncfile;
	}
	
	private static Map<String, List<Dimension>> variableDimension = new HashMap<String, List<Dimension>>();
	private static Map<String, String[]> variableDimensionName = new HashMap<String, String[]>();
	
	private static List<Dimension> getVariableDimension(Variable var) {
		if (variableDimension.containsKey(var.getName()))
			return variableDimension.get(var.getName());
		
		
		variableDimension.put(var.getName(), var.getDimensions());	
		return variableDimension.get(var.getName());
	}
	
	public static String[] getVariableDimensionName(Variable var) {
		if (variableDimensionName.containsKey(var.getName()))
			return variableDimensionName.get(var.getName());
		
//		List<Dimension> vardims = getVariableDimension(var);
//		if (vardims == null)
//			System.out.println(" getVariableDimensionName " + var.getName() + " " + var.getDimensions());
		String[] dimsName = new String[var.getDimensions().size()];
		for (int i = 0; i <  var.getDimensions().size(); i++) {
			Dimension vardim =  var.getDimensions().get(i);
			dimsName[i] = vardim.getName();
		}
		variableDimensionName.put(var.getName(), dimsName);	
		return variableDimensionName.get(var.getName());
	}
	
	public static int[] getOriginDimension(Variable var, Integer dimensionTime, Integer dimensionX, Integer dimensionY) {
		String[] dimsName = getVariableDimensionName(var);
		int[] origin = var.getShape();
		for (int i = 0; i < dimsName.length; i++) {
			if (dimsName[i].contains(InputConfig.TIME_DIMNAME)) {
				origin[i] = dimensionTime == null ? 0 : dimensionTime.intValue();
			} else if (dimsName[i].contains(InputConfig.SOUTH_NORTH_DIMNAME) || dimsName[i].contains(InputConfig.Y_DIMNAME)) {
				origin[i] = dimensionY == null ? 0 : dimensionY.intValue();
			} else if (dimsName[i].contains(InputConfig.WEST_EAST_DIMNAME) || dimsName[i].contains(InputConfig.X_DIMNAME)) {
				origin[i] = dimensionX == null ? 0 : dimensionX.intValue();
			} else if (dimsName[i].contains(InputConfig.BOTTOM_TOP_DIMNAME)) {
				origin[i] = 0; //--> do not condiser z param, take them all
			}
		}
		//System.out.println();
		
		//System.out.println("origin " + origin[0] + " " + origin[1] + " " + origin[2] + " " + origin[3]);
		//System.out.println("shape " + shape[0] + " " + shape[1] + " " + shape[2] + " " + shape[3]);
		
		return origin;
	}
	
	public static int[] getShapeDimension(Variable var, Integer rangeTime, Integer rangeX, Integer rangeY) {
		List<Dimension> vardims = getVariableDimension(var);
		Map<String, Integer> dimsSize = new HashMap<String, Integer>();
		String[] dimsName = getVariableDimensionName(var);
		for (int i = 0; i < vardims.size(); i++) {
			Dimension vardim = vardims.get(i);
			dimsSize.put(vardim.getName(), vardim.getLength());
		}
		
		int[] shape = var.getShape();
		for (int i = 0; i < dimsName.length; i++) {
			if (dimsName[i].contains(InputConfig.TIME_DIMNAME)) {
				shape[i] = rangeTime == null ? dimsSize.get(dimsName[i]) : rangeTime.intValue();
			} else if (dimsName[i].contains(InputConfig.SOUTH_NORTH_DIMNAME) || dimsName[i].contains(InputConfig.Y_DIMNAME)) {
				shape[i] = rangeY == null ? dimsSize.get(dimsName[i]) : rangeY.intValue();
			} else if (dimsName[i].contains(InputConfig.WEST_EAST_DIMNAME) || dimsName[i].contains(InputConfig.X_DIMNAME)) {
				shape[i] = rangeX == null ? dimsSize.get(dimsName[i]) : rangeX.intValue();
			} else if (dimsName[i].contains(InputConfig.BOTTOM_TOP_DIMNAME)) {
				shape[i] = dimsSize.get(dimsName[i]); //--> do not condiser z param, take them all
			}
		}
		
		//System.out.println();
		
		//System.out.println("origin " + origin[0] + " " + origin[1] + " " + origin[2] + " " + origin[3]);
		//System.out.println("shape " + shape[0] + " " + shape[1] + " " + shape[2] + " " + shape[3]);
		
		return shape;
	}
}
