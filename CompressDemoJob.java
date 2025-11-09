package compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce job that demonstrates compression techniques
 * Searches log files for lines containing a specific search string
 * Uses Snappy compression for both intermediate and final output data
 */
public class CompressDemoJob extends Configured implements Tool {
	
	/**
	 * Mapper class that filters log entries based on a search string
	 * Input: Raw text lines from log files
	 * Output: Key = "first_word third_word", Value = entire matching line
	 */
	public static class CompressMapper extends Mapper<LongWritable, Text, Text, Text> {
		// Search string loaded from configuration
		private String searchString;
		
		// Reusable Text object for output keys
		private Text outputKey = new Text();
		
		/**
		 * Map function that searches for matching lines and emits filtered results
		 * Only processes lines that contain the search string
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the log line by spaces into individual words
			String [] words = value.toString().split(" ");
			
			// Check each word in the line
			for(String word: words) {
				// If any word contains the search string
				if(word.contains(searchString)) {
					// Create a composite key from first and third words
					// (e.g., "timestamp status" or similar log format)
					outputKey.set(words[0] + " " + words[2]);
					
					// Emit the composite key with the entire matching line
					context.write(outputKey, value);
				}
			}
		}
		
		/**
		 * Setup method called once before map tasks begin
		 * Retrieves the search string from job configuration
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Get the search string passed as a command-line argument
			searchString = context.getConfiguration().get("searchString");
		}
	}
	
	/**
	 * Reducer class that consolidates filtered log entries
	 * Input: Key = composite key, Values = list of matching log lines
	 * Output: Key = NullWritable (no key), Value = log line
	 */
	public static class CompressReducer extends Reducer<Text, Text, NullWritable, Text> {
		// NullWritable key for output (we only care about the log lines)
		private NullWritable outputKey = NullWritable.get();
		
		/**
		 * Reduce function that emits all matching log lines
		 * Simply passes through all values without additional processing
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Iterate through all log lines for this key
			while(values.iterator().hasNext()) {
				// Emit each log line with NullWritable key
				context.write(outputKey, values.iterator().next());
			}
		}
	}
	
	/**
	 * Configures and runs the MapReduce job with compression enabled
	 * @param args Command line arguments (args[0] should be the search string)
	 * @return 0 if job succeeds, 1 if it fails
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Create new MapReduce job
		Job job = Job.getInstance(getConf(), "CompressJob");
		Configuration conf = job.getConfiguration();
		
		// Store the search string (first command-line argument) in configuration
		// This makes it accessible to mapper tasks
		conf.set("searchString", args[0]);
		
		// Set the jar class
		job.setJarByClass(CompressDemoJob.class);
		
		// Set up output path and delete if it already exists
		Path out = new Path("logresults1");
		out.getFileSystem(conf).delete(out, true);
		
		// Set input and output paths
		FileInputFormat.setInputPaths(job, new Path("logfiles"));
		FileOutputFormat.setOutputPath(job, out);
		
		// Configure mapper and reducer classes
		job.setMapperClass(CompressMapper.class);
		job.setReducerClass(CompressReducer.class);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set final output key and value types (from reducer)
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// Set intermediate output key and value types (from mapper)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// ========== COMPRESSION CONFIGURATION ==========
		
		// Enable compression for intermediate data (mapper output to reducer input)
		// This reduces network traffic between map and reduce tasks
		conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
		
		// Set Snappy codec for intermediate compression
		// Snappy is fast and provides good compression for intermediate data
		conf.setClass(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, 
				SnappyCodec.class, CompressionCodec.class);
		
		// Enable compression for final output files
		// This reduces storage space for results
		conf.setBoolean(FileOutputFormat.COMPRESS, true);
		
		// Set Snappy codec for output compression
		// Final results will be stored as compressed files
		conf.setClass(FileOutputFormat.COMPRESS_CODEC, 
				SnappyCodec.class, CompressionCodec.class);
		
		// Wait for job completion and return status
		return job.waitForCompletion(true)?0:1;
	}
	
	/**
	 * Main entry point for the application
	 * @param args Command line arguments (expects search string as first argument)
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			result = ToolRunner.run(new Configuration(), new CompressDemoJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Exit with the job's return code
		System.exit(result);
	}
}
