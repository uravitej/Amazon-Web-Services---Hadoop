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

public class AvgTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

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
			// Passing all the required data
			output.collect(new Text(year),new Text(temp + "," + precp + "," + wind));
		}
	}

}
