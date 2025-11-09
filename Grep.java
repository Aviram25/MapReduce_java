package grep;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce implementation of grep functionality
 * Searches for words containing a specific substring and counts their occurrences
 * Similar to Unix 'grep' command combined with word count
 */
public class Grep extends Configured implements Tool {
	
	/**
	 * Mapper class that searches for words containing the search string
	 * Input: Raw text lines from input file
	 * Output: Key = matching word, Value = 1 (for counting)
	 */
	public static class GrepMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		// Search string to look for in words (set during setup phase)
		private String searchString;
		
		// Reusable Text object for output keys
		private Text outputValue = new Text();
		
		// Constant value of 1 for counting occurrences
		private final static IntWritable ONE = new IntWritable(1);
		
		/**
		 * Map function that filters and emits words containing the search string
		 * Each matching word is emitted with a count of 1
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("-----Inside map()-----");
			
			// Split the input line by spaces into individual words
			String [] words = StringUtils.split(value.toString(),'\\', ' ');
			
			// Iterate through each word in the line
			for(String word: words) {
				// Check if the word contains the search string (case-sensitive)
				// Using String.contains() method to check substring presence
				if(word.contains(searchString)) {
					// Set the matching word as the key
					outputValue.set(word);
					
					// Emit the word with count of 1
					context.write(outputValue, ONE);
				}
			}
		}
		
		/**
		 * Setup method called ONCE per mapper task before any map() calls
		 * Used to initialize the mapper with configuration parameters
		 * 
		 * ANSWER: This method is called ONCE per mapper task at the beginning,
		 * before processing any input records. If there are 10 mapper tasks,
		 * setup() will be called 10 times total (once per task).
		 */
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			System.out.println("-----Inside setup()-----");
			
			// Retrieve the search string from job configuration
			// This was set in the run() method from command-line arguments
			searchString = context.getConfiguration().get("searchString");
		}
	}
	
	/**
	 * Configures and runs the MapReduce job
	 * @param args Command line arguments:
	 *             args[0] = input path
	 *             args[1] = output path
	 *             args[2] = search string
	 * @return 0 if job succeeds, 1 if it fails
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Create new MapReduce job
		Job job = Job.getInstance(getConf(), "GrepJob");
		Configuration conf = job.getConfiguration();
		
		// Store the search string (third command-line argument) in configuration
		// This makes it accessible to all mapper tasks via setup() method
		conf.set("searchString", args[2]);
		
		// Set the jar class
		job.setJarByClass(getClass());
		
		// Set up input and output paths from command-line arguments
		Path in = new Path(args[0]);   // Input file path
		Path out = new Path(args[1]);  // Output directory path
		
		// Delete output directory if it already exists
		out.getFileSystem(conf).delete(out, true);
		
		// Configure input and output paths
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// Set the mapper class (our custom GrepMapper)
		job.setMapperClass(GrepMapper.class);
		
		// Set the reducer class (built-in IntSumReducer)
		// IntSumReducer is a pre-built reducer that sums all IntWritable values for each key
		// API: Takes Text keys and IntWritable values, outputs Text keys with summed IntWritable values
		// Perfect for word count scenarios where we just need to sum up the counts
		job.setReducerClass(IntSumReducer.class);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set intermediate output types (mapper output)
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set final output types (reducer output)
		// Key: Text (the word)
		job.setOutputKeyClass(Text.class);
		
		// Value: IntWritable (the total count)
		job.setOutputValueClass(IntWritable.class);
		
		// Wait for job completion and return status
		return job.waitForCompletion(true)?0:1;
	}
	
	/**
	 * Main entry point for the application
	 * @param args Command line arguments: [input_path] [output_path] [search_string]
	 * 
	 * Example usage:
	 * yarn jar grep.jar constitution.txt grep_output the
	 * 
	 * This will:
	 * 1. Read from constitution.txt
	 * 2. Search for all words containing "the"
	 * 3. Count occurrences of each matching word
	 * 4. Output results to grep_output directory
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			result = ToolRunner.run(new Configuration(), 
							new Grep(),
							args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Exit with the job's return code
		System.exit(result);
	}
}

// COMMAND TO RUN THE APPLICATION:
// yarn jar grep.jar constitution.txt grep_output the
//
// Explanation of command:
// - yarn jar grep.jar          : Execute the jar file using YARN
// - constitution.txt           : Input file (args[0])
// - grep_output               : Output directory (args[1])
// - the                       : Search string (args[2])
//
// Expected output:
// - All words containing "the" will be listed with their counts
// - Examples: "the" -> 500, "other" -> 25, "their" -> 30, etc.
