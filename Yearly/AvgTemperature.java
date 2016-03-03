/*
 * CSE6331 Assignment 4 
 * Ravitej Urikiti 1001114648
 * Reference : https://github.com/deepshah22/hadoop-avg-temp/tree/master/src
 * Reference: http://stackoverflow.com/questions/1770010/how-do-i-measure-time-elapsed-in-java
 */


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class AvgTemperature {

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		JobConf conf = new JobConf(AvgTemperature.class);
		conf.setJobName("Avg Temp");
		// Number of maps needed for the job from input arguments
		conf.setNumMapTasks(Integer.parseInt(args[2]));
		//Number of reduces needed for the job from input arguments
		conf.setNumReduceTasks(Integer.parseInt(args[3]));
	
		conf.setOutputKeyClass(Text.class);
		//conf.setOutputValueClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(AvgTemperatureMapper.class);
		conf.setReducerClass(AvgTemperatureReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		 long finishTime = System.currentTimeMillis();
	     long time = (finishTime - startTime);
	     System.out.println("Time to Execute:" + time/1000 + "s");

	}

}