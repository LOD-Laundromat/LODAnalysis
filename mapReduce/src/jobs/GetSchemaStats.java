package jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import reducers.GetSchemaStatsReducer;
import mappers.GetSchemaStatsMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetSchemaStats extends Configured implements Tool {

	private static Logger log = LoggerFactory.getLogger(GetSchemaStats.class);

        private static String inputPath;
        private static String outputPath;
	// Parameters
	private int numReduceTasks = 1;
	private int numMapTasks = 4;
	private int sampling = 0;
	private int resourceThreshold = 0;
	private int inputStep = 0;
	private boolean rewriteBlankNodes = true;
	private boolean noDictionary = true;

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: GetSchemaStats <input dir> <output dir>");
			System.exit(0);
		}

                inputPath  = args[0];
                outputPath = args[1];

		long time = System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new GetSchemaStats(), args);
		log.info("Import time: " + (System.currentTimeMillis() - time));
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		parseArgs(args);
		sampleCommonResources(args);
		return 0;
	}

	private Job createNewJob(String name) throws IOException {
		Configuration conf = new Configuration(this.getConf());

		Job job = new Job(conf);
		job.setJarByClass(GetSchemaStats.class);
		job.setJobName(name);
		job.setNumReduceTasks(numReduceTasks);
                // TODO: Check this out
		//SequenceFileOutputFormat.setCompressOutput(job, true);
		//SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		return job;
	}

	public void sampleCommonResources(String[] args) throws Exception {
		Job job = createNewJob("Get Schema Statistics");

		// Input
		FileInputFormat.addInputPath(job, new Path(inputPath));
		//FileInputFormat.setInputPathFilter(job, FileUtils.FILTER_ONLY_HIDDEN.getClass());

                // TODO: If input format is gz, then not splitable, otherwise its splitable
		//MultiFilesReader.setSplitable(job.getConfiguration(), false);
		job.setInputFormatClass(TextInputFormat.class);

		// Job
		job.setMapperClass(GetSchemaStatsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(GetSchemaStatsReducer.class);

		// Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Long.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		long time = System.currentTimeMillis();
		job.waitForCompletion(true);
		log.info("Job finished in " + (System.currentTimeMillis() - time));
	}

	public void parseArgs(String[] args) {

		for (int i = 0; i < args.length; ++i) {

			if (args[i].equalsIgnoreCase("--maptasks")) {
				numMapTasks = Integer.valueOf(args[++i]);
			}

			if (args[i].equalsIgnoreCase("--reducetasks")) {
				numReduceTasks = Integer.valueOf(args[++i]);
			}
		}
	}

}
