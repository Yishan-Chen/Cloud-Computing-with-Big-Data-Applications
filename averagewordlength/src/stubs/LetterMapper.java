package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text wordObject = new Text();
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String line = value.toString();
	  for (String word : line.split("\\W+")){
		  if(word.length() > 0){
			  wordObject.set(word.substring(0,1));
			  context.write(wordObject,new IntWritable(word.length()));
		  }
	  }
   

  }
}
