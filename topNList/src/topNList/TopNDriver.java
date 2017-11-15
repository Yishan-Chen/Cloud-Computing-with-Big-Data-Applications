package topNList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new TopNDriver(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 3) {
			System.out.printf("Usage: WordCount <N> <input dir> <output dir>\n");
			return -1;
		}
		
		/*
	     * Instantiate a Job object for your job's configuration.  
	     */
		Job job = new Job(getConf());
		int N = Integer.parseInt(args[0]); // top N
	    job.getConfiguration().setInt("N", N);
		
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
		job.setJobName("Top N List");
		
		 /*
	     * Specify the paths to the input and output data based on the
	     * command-line arguments.
	     */
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		/*
	     * Specify the mapper, combiner and reducer classes.
	     */
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		/*
	     * Specify the job's mapper's output key and value classes.
	     */
		job.setMapOutputKeyClass(NullWritable.class);   
	    job.setMapOutputValueClass(Text.class);  
	      
	    /*
	     * Specify the job's output key and value classes.
	     */
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
	
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
