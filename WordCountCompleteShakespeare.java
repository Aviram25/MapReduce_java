/**
 * WordCountCompleteShakespeare - A Hadoop MapReduce application for counting words
 * in Shakespeare's complete works while filtering out stop words.
 * 
 * Authors: 
 * - Aviram Dhagat (011)
 * - Ishaan Singh Shekhawat (021)
 * - Vineet Gupta (058)
 * 
 * Prerequisites:
 * - Stop words file (stop_words.txt) uploaded to HDFS at /user/cloudera/
 * - Input text file containing Shakespeare's works
 * 
 * Usage:
 * yarn jar wordcount.jar wordcount.WordCountCompleteShakespeare <input_path> <output_path>
 * 
 * Input:
 * - args[0]: Path to input file/directory in HDFS
 * - args[1]: Path to output directory in HDFS
 * 
 * Output:
 * - Word count pairs (word, count) excluding stop words
 * - Output distributed across 3 reducer output files
 */

package wordcount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountCompleteShakespeare extends Configured implements Tool{

	/**
	 * StopWordMapper - Mapper class that filters stop words and emits word-count pairs.
	 * 
	 * Input: <LongWritable, Text> - Line offset and line content
	 * Output: <Text, IntWritable> - Word and count of 1
	 */
	public static class StopWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		// Reusable Text object for output key to reduce object creation overhead
		public Text outputKey = new Text();
		
		// Constant IntWritable with value 1 for word count emissions
		public final static IntWritable ONE = new IntWritable(1);
		
		// Set to store stop words loaded from distributed cache
		public Set<String> stop_words = new HashSet();
		
		/**
		 * Setup method called once per mapper task before any map() calls.
		 * Loads stop words from the distributed cache file into a HashSet
		 * for efficient lookup during mapping.
		 */
		@Override
		public void setup(Context context) throws IOException, InterruptedException  {
			
			try {
				// Retrieve cache files from distributed cache
				URI[] CacheFiles = context.getCacheFiles();
				
				// Validate that cache files exist
				if (CacheFiles == null || CacheFiles.length == 0) {
					throw new IOException("Cache File not found!");
				}
				
				// Get the cache file (stop_words.txt)
				Path filter = new Path(CacheFiles[0]);
				String filename = filter.getName();
				
				// Read the stop words file
				BufferedReader reader = new BufferedReader(new FileReader(filename));
				
				// Read the first line containing comma-separated stop words
				String line = reader.readLine();
				
				// Split the line by comma delimiter
				String[] temp = StringUtils.split(line, ',');
				
				// Add all stop words to the HashSet for O(1) lookup
				Collections.addAll(stop_words, temp);
				
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		/**
		 * Map method called once per input line.
		 * Splits the line into words and emits each word (except stop words)
		 * with a count of 1.
		 */
		@Override
		public void map(LongWritable Key, Text Value, Context context) throws IOException, InterruptedException {
			// Convert Text value to String
			String CurrentLine = Value.toString();
			
			// Split line into words using space as delimiter
			String[] words = StringUtils.split(CurrentLine, ' ');
			
			// Process each word in the line
			for (String word : words) {
				// Skip stop words
				if (stop_words.contains(word)) {
					continue;
				}
				else {
					// Emit non-stop word with count of 1
					outputKey.set(word);
					context.write(outputKey, ONE);
				}
			}
		}
	}
	
	/**
	 * StopWordReducer - Reducer class that aggregates word counts.
	 * 
	 * Input: <Text, Iterable<IntWritable>> - Word and list of counts
	 * Output: <Text, IntWritable> - Word and total count
	 */
	public static class StopWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		// Reusable IntWritable object for output value
		public IntWritable outputValue = new IntWritable();
		
		/**
		 * Reduce method called once per unique key (word).
		 * Sums all the counts for a given word and emits the total.
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			
			// Initialize sum counter
			int sum = 0;
			
			// Iterate through all counts for this word and sum them
			for (IntWritable count: values) {
				sum += count.get();
			}
			
			// Set the output value to the total count
			outputValue.set(sum);
			
			// Emit the word and its total count
			context.write(key, outputValue);
		}
	}
	
	/**
	 * Run method that configures and executes the MapReduce job.
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Create a new MapReduce job with descriptive name
		Job job = Job.getInstance(getConf(), "WordCountCompleteShakespeare");
		Configuration conf = job.getConfiguration();
		
		// Set the jar class for job execution
		job.setJarByClass(getClass());
		
		// Configure input and output paths
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		// Delete output directory if it exists to avoid conflicts
		out.getFileSystem(conf).delete(out, true);
		
		// Set input and output paths for the job
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Add stop words file to distributed cache for mapper access
		job.addCacheFile(new URI("hdfs:///user/cloudera/stop_words.txt"));
		
		// Configure 3 reducer tasks for parallel processing
		job.setNumReduceTasks(3);
		
		// Set mapper and reducer classes
		job.setMapperClass(StopWordMapper.class);
		job.setReducerClass(StopWordReducer.class);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set mapper output key and value types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set final output key and value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Wait for job completion and return status (0=success, 1=failure)
		return job.waitForCompletion(true)?0:1;
		
	}
	
	/**
	 * Main method - Entry point for the MapReduce application.
	 */
	public static void main(String[] args) {
		
		int result = 0;
		try {
			// Run the MapReduce job using ToolRunner for proper configuration handling
			result = ToolRunner.run(new Configuration(), new WordCountCompleteShakespeare(), args);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		// Exit with the job result status
		System.exit(result);
	}

}