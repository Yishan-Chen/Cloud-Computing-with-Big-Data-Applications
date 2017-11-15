package stubs;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  Set<String> allDocuments = new HashSet<String>();
	  
	  // remove the duplicate
	  for(Text value : values){
		  allDocuments.add(value.toString());
	  }
	  String combineValue = new String("");
	  
	  // concat the filename@linenumber for each word
	  for (String document : allDocuments){
		  combineValue = new String(combineValue.concat(document.concat(",")));
	  }
	  
	  context.write(key, new Text(combineValue));
  }
}