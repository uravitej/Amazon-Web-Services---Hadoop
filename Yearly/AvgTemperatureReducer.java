/*
 * CSE6331 Assignment 4 
 * Ravitej Urikiti 1001114648
 * Reference : https://github.com/deepshah22/hadoop-avg-temp/tree/master/src
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class AvgTemperatureReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
	ArrayList <Integer> temps = new ArrayList<Integer>();
	ArrayList <Integer> precps = new ArrayList<Integer>();
	ArrayList <Integer> winds = new ArrayList<Integer>();
	
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
			//Fetching the required data
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
			//Ignoring the data that is not recorded
			if (iprecp != -9999){
				sumPrecp += iprecp;
				precpItems += 1;
			}
			//Ignoring the data that is not recorded
			if (iwind != -9999){
				sumWind += iwind;
				windItems += 1;
			}

		}
		//Finding the average of the each fields collected
		int avgTemp= sumTemp/tempItems;
		int avgPrecp= sumPrecp/precpItems;
		int avgWind= sumWind/windItems;
		//Storing in the array for comparing with the previous year results
		temps.add(avgTemp);
		precps.add(avgPrecp);
		winds.add(avgWind);
		int tempind = temps.size();
		int precpind = precps.size();
		int windind = winds.size();
		String tempresults = "Cannot compare", precpresults = "Cannot compare", windresults = "Cannot compare";
		if (tempind > 1 ){
			if ( avgTemp > temps.get(tempind - 2)){
				tempresults = "Hotter than previous year";
			}
			else if (avgTemp < temps.get(tempind - 2)){
				tempresults = "Colder than previous year";
			}
			else {
				tempresults = "Temperature same as previous year";
			}
		}
		if (precpind > 1 ){
			if ( avgPrecp > precps.get(precpind - 2)){
				precpresults = "Precipitation more than previous year";
			}
			else if (avgPrecp < precps.get(precpind - 2)){
				precpresults = "Precipitation less than previous year";
			}
			else {
				precpresults = "Precipitation same as previous year";
			}
		}
		if (windind > 1 ){
			if ( avgWind > winds.get(windind - 2)){
				windresults = "Wind speed more than previous year";
			}
			else if (avgWind < winds.get(windind - 2)){
				windresults = "Wind speed less than previous year";
			}
			else {
				windresults = "Wind speed same as previous year";
			}
		}
		String finalresult = String.valueOf(avgTemp) + "     " + tempresults + "     " + String.valueOf(avgPrecp) + "     " + precpresults + "     " + String.valueOf(avgWind) + "     " + windresults;  

		output.collect(key, new Text(finalresult));
	} 

}
