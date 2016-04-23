package net.pascalalma.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created with IntelliJ IDEA. User: pascal Date: 16-07-13 Time: 12:07
 */
public class Dictionary {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "dictionary");
		job.setJarByClass(Dictionary.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(AllTranslationsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String input = "/Users/fernando/Work/Apps/Hadoop/dictionary/input.txt";
		String output = "/Users/fernando/Work/Apps/Hadoop/dictionary/output";
		
		//String input = "/user/fernando/dictionary/input.txt";
		//String output = "/user/fernando/dictionary/output";
		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}