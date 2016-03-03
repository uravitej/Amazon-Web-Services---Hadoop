/*
 * CSE6331 Assignment 4 
 * Ravitej Urikiti 1001114648
 * Reference : https://github.com/deepshah22/hadoop-avg-temp/tree/master/src
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


public class AvgSeasonMapReduce {

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(AvgSeasonMapReduce.class);
		conf.setJobName("Climate Map Reduce");
		// Number of maps needed for the job from input arguments
		conf.setNumMapTasks(Integer.parseInt(args[2]));
		//Number of reduces needed for the job from input arguments
		conf.setNumMapTasks(Integer.parseInt(args[3]));
		
		
		conf.setOutputKeyClass(Text.class);
		//conf.setOutputValueClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(AvgSeasonMapper.class);
		conf.setReducerClass(AvgSeasonReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);

	}

}
