package average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce job that adds regional classification to county data
 * Appends either "Southern" or "Northern" region based on county name's first letter
 * This is a map-only job (no reducer)
 */
public class AverageJob extends Configured implements Tool {

	// Commented out: Counter enum for tracking map, combine, and reduce operations
//	public enum Counters {MAP, COMBINE, REDUCE}
//	
	// Commented out: Custom partitioner that splits data by first letter (A-M vs N-Z)
//	public static class CustomPartitioner extends Partitioner<Text, Text> {
//		public int getPartition(Text key, Text value, int numReduceTasks) {
//			
//			System.out.println("Inside Custom Partitioner");
//			// Send keys starting with A-M to partition 0
//			if (key.charAt(0) >= 'A' && key.charAt(0) <= 'M') {
//				return 0 % numReduceTasks;
//			}
//			// Send keys starting with N-Z to partition 1
//			else {
//				return 1 % numReduceTasks;
//			}
//		}
//	}

	/**
	 * Mapper class that appends regional classification to each input line
	 * Input: Raw text lines from the counties file
	 * Output: Key = NullWritable (no key needed), Value = original line + region label
	 */
	public static class AverageMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		// NullWritable key since we're only transforming data without grouping
//		public NullWritable outputKey = NullWritable.get();
		
		// Reusable Text object for output values
		public Text outputValue = new Text();
		
		// Commented out: Previously used for count tracking in average calculation
//		public final String ONE = ",1";
		
		/**
		 * Determines the region based on the first letter of the county name
		 * @param Key The county name
		 * @return ", Southern" if name starts with A-M, ", Northern" if N-Z
		 */
		protected String setRegion(String Key) {
			// Counties starting with A-M are classified as Southern
			if (Key.charAt(0) >= 'A' && Key.charAt(0) <= 'M') {
				return ", Southern";
			}
			// Counties starting with N-Z are classified as Northern
			else {
				return ", Northern";
			}
		}
		
		/**
		 * Map function that processes each input line
		 * Appends regional classification to the end of each line
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Get the current line as a string
			String currentLine = value.toString();
			
			// Split the CSV line by comma to extract fields
			String [] words = StringUtils.split(currentLine,'\\', ',');
			
			// Append the region (based on field at index 1 - county name) to the line
			currentLine = currentLine.concat(setRegion(words[1].trim()));
			
			// Set the modified line as output value
			outputValue.set(currentLine);
			
			// Emit with NullWritable key (since we don't need grouping)
			context.write(NullWritable.get(), outputValue);
			
			// Commented out: Counter to track number of map operations
//			context.getCounter(Counters.MAP).increment(1);
		}

		// Commented out: Cleanup method that would print MAP counter
//		@Override
//		protected void cleanup(Context context)
//				throws IOException, InterruptedException {
//			System.out.println("MAP counter = " + context.getCounter(Counters.MAP).getValue());
//			
//		}
	}

	// Commented out: Combiner class for local aggregation (not used in this version)
//	public static class AverageCombiner extends Reducer<Text, Text, Text, Text> {
//		private Text outputValue = new Text();
//		private String COMMA = ",";
//		
//		@Override
//		protected void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//			long sum = 0;
//			int count = 0;
//			// Iterate through values and accumulate sum and count
//			while(values.iterator().hasNext()) {
//				String current = values.iterator().next().toString();
//				String [] words = StringUtils.split(current,'\\', ',');
//				sum += Long.parseLong(words[0]);
//				count += Integer.parseInt(words[1]);
//			}
//			outputValue.set(sum + COMMA + count);
//			context.write(key, outputValue);
//			context.getCounter(Counters.COMBINE).increment(1);
//		}		
//
//		@Override
//		protected void cleanup(Context context)
//				throws IOException, InterruptedException {
//			System.out.println("COMBINE counter = " + context.getCounter(Counters.COMBINE).getValue());
//		}
//	}
//
	// Commented out: Reducer class for computing averages (not used in this version)
//	public static class AverageReducer extends Reducer<Text, Text, Text, DoubleWritable> {
//		DoubleWritable outputValue = new DoubleWritable();
//		
//		@Override
//		protected void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//			long sum = 0;
//			int count = 0;
//			// Aggregate all sums and counts
//			while(values.iterator().hasNext()) {
//				String current = values.iterator().next().toString();
//				String [] words = StringUtils.split(current,'\\',',');
//				sum += Long.parseLong(words[0]);
//				count += Integer.parseInt(words[1]);
//			}
//			// Calculate and emit average
//			outputValue.set(((double) sum)/count);
//			context.write(key, outputValue);
//			context.getCounter(Counters.REDUCE).increment(1);
//		}
//
//		@Override
//		protected void cleanup(Context context)
//				throws IOException, InterruptedException {
//			System.out.println("REDUCE counter = " + context.getCounter(Counters.REDUCE).getValue());
//		}
//	}

	/**
	 * Configures and runs the MapReduce job
	 * This is a map-only job that transforms input data by adding regional labels
	 * @param arg0 Command line arguments
	 * @return 0 if job succeeds, 1 if it fails
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		// Get configuration
		Configuration conf = super.getConf();
		
		// Create new MapReduce job
		Job job = Job.getInstance(conf, "AverageJob");
		job.setJarByClass(AverageJob.class);

		// Set up output path and delete if it already exists
		Path out = new Path("averagenew");
		out.getFileSystem(conf).delete(out, true);
		
		// Set input and output paths
		FileInputFormat.setInputPaths(job, "counties");
		FileOutputFormat.setOutputPath(job, out);
		
		// Commented out: Custom partitioner configuration (not used in this version)
//		job.setPartitionerClass(CustomPartitioner.class);
//		job.setNumReduceTasks(2);

		// Configure mapper (no reducer or combiner in this version)
		job.setMapperClass(AverageMapper.class);
		
		// Commented out: Reducer and combiner classes (this is a map-only job)
//		job.setReducerClass(AverageReducer.class);
//		job.setCombinerClass(AverageCombiner.class);
		
		// Set input and output format classes
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set output types: NullWritable key and Text value
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Wait for job completion and return status
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * Main entry point for the application
	 * @param args Command line arguments
	 */
	public static void main(String[] args) {
		int result = 0;
		try {
			// Run the job using ToolRunner
			result = ToolRunner.run(new Configuration(),  new AverageJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Exit with the job's return code
		System.exit(result);
	}
}
