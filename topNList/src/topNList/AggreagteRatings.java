package topNList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggreagteRatings extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new AggreagteRatings(), args);
		System.exit(exitCode);
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			System.out.printf("Usage: WordCount <input dir> <output dir>\n");
			return -1;
		}
		/*
	     * Instantiate a Job object for your job's configuration.  
	     */
		Job job = new Job(getConf());
		
		/*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running
	     * mapper and reducer tasks.
	     */
		job.setJarByClass(AggreagteRatings.class);
		
		/*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
		job.setJobName("job1");
		
		/*
	     * Specify the paths to the input and output data based on the
	     * command-line arguments.
	     */
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/*
	     * Specify the mapper and reducer classes.
	     */
		job.setMapperClass(AggreagteRatingsMapper.class);
//		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(SumReducer.class);
		
		/*
	     * Specify the job's output key and value classes.
	     */
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
