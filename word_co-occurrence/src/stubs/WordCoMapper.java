package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text wordObject = new Text();
  private IntWritable intObject = new IntWritable(1);
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
	String[] words = value.toString().split("\\W+");
	String first,second;
    for (int i=1; i<words.length; i++){
    	// receiver every near-by words
    	first = words[i-1].toLowerCase();
    	second = words[i].toLowerCase();
    	wordObject.set(first + ',' + second);
    	context.write(wordObject,intObject);
    }
  }
}
