/*
 * CSE6331 Assignment 4 
 * Ravitej Urikiti 1001114648
 * Reference : https://github.com/deepshah22/hadoop-avg-temp/tree/master/src
 */

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AvgSeasonMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		
		String[] line = value.toString().split(","); // splitting by ","
		String dataPart = line[2]; // Fetching Date
		String temp = line[3]; // Fetching Temperature
		String precp = line[6]; // Fetching Precipitation
		String wind = line[13]; // Fetching Wind
		if(StringUtils.isNumeric(dataPart)){
			String year = dataPart.substring(0, 4);
			String month = dataPart.substring(4,6);
			String season = "Fall";
			if (month.equals("12") || month.equals("01") ||month.equals("02"))
		      {
		    	  season = "Winter";
		      }
		      else if (month.equals("03") || month.equals("04") ||month.equals("05"))
		      {
		    	  season = "Spring";
		      }
		      else if (month.equals("06") || month.equals("07") ||month.equals("08"))
		      {
		    	  season = "Summer";
		      }
		      else if (month.equals("09") || month.equals("10") ||month.equals("11"))
		      {
		    	  season = "Fall";
		      }
		      String seasonkey = year + season;

			// Passing all the required data
			output.collect(new Text(seasonkey),new Text(temp + "," + precp + "," + wind));
		}
	}

}
