package topNList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

	private int N = 3; // default
	private SortedMap<Integer, String> top = new TreeMap<Integer, String>(); // sorted map to store the top-n MOVIE
	private SortedMap<String, String> movies = new TreeMap<String,String>(); // sorted map to store movie information(movieID, title) pair

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.N = context.getConfiguration().getInt("N", 3); // get N from
		File file=new File("movie_titles.txt");			    // read file from distributed cache
		FileReader fileReader = new FileReader(file);		// Use filereader and bufferreader to read the content of the file
		BufferedReader br = new BufferedReader(fileReader);
		String line = "";
		while( (line = br.readLine()) != null){				
			String[] token = line.split(",");
			movies.put(token[0], token[2]);					//put the key, movie_title into the sortedmap movies
		}
		br.close();
	}
	
	public void setConf(Configuration configuration) throws IOException {
		
	}
	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] token = value.toString().split(",");
			int ranting = Integer.parseInt(token[1]);		// token[0], token[1] are movieID, the sum of movie ranting
			String movieTitle = movies.get(token[0]);		// get the title from movieID
			top.put(ranting, movieTitle);
			if (top.size() > N) {							// remove movies with lowest ranting
				top.remove(top.firstKey());
			}
		}

		List<Integer> keys = new ArrayList<Integer>(top.keySet());
		for (int i = keys.size() - 1; i >= 0; i--) {		// emit every movie_title,ranting pair from the top list
			context.write(new IntWritable(keys.get(i)),new Text(top.get(keys.get(i))));
		}
	}
}
