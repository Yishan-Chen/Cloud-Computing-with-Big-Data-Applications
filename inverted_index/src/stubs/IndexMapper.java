package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {
  
  // Create object before mapper to reuse these objects
  Text outputKey = new Text();
  Text outputValue = new Text();
  
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  
	  // retrieve the filename which the job is currently reading from
	  String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
	  
	  // get string from input key and value
	  String lineNumber = key.toString();
	  String[] words = value.toString().split("\\W+");
	  
	  // Format the value and emit the key value pair
	  for (String word:words) {
		  word = word.toLowerCase();
		  outputKey.set(word);
		  outputValue.set(fileName + '@' + lineNumber);
		  context.write(outputKey,outputValue);
	  }
	
  }
}