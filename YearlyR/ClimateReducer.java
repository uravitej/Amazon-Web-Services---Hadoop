/*
 * CSE6331 Assignment 4 
 * Ravitej Urikiti 1001114648
 * Reference : https://github.com/deepshah22/hadoop-avg-temp/tree/master/src
 */


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ClimateReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
	
		int sumTemp = 0;
		int sumPrecp = 0;
		int sumWind = 0;
		int tempItems = 0;
		int precpItems = 0;
		int windItems = 0;
		
		while (values.hasNext()){
			String nextValue = values.next().toString();
			String vals[] = nextValue.split(",");
			String temp = vals[0];
			String precp = vals[1];
			String wind = vals[2];
			int itemp = Integer.parseInt(temp);
			int iprecp = Integer.parseInt(precp);
			int iwind = Integer.parseInt(wind);
			// Ignoring the data that is not recorded
			if (itemp != -9999){
				sumTemp += itemp;
				tempItems += 1;
			}
			// Ignoring the data that is not recorded
			if (iprecp != -9999){
				sumPrecp += iprecp;
				precpItems += 1;
			}
			// Ignoring the data that is not recorded
			if (iwind != -9999){
				sumWind += iwind;
				windItems += 1;
			}
			
		}
		//Calculating the average of the required data
		int avgTemp= sumTemp/tempItems;
		int avgPrecp= sumPrecp/precpItems;
		int avgWind= sumWind/windItems;
		//Combining into a final string with Comma Separated Values
		String finalresult = "," + String.valueOf(avgTemp) + "," + String.valueOf(avgPrecp) + "," + String.valueOf(avgWind);
		
		output.collect(key, new Text(finalresult));
	} 

}
