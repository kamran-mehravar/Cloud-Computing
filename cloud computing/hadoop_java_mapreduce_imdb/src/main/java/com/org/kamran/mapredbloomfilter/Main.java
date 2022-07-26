package com.org.kamran.mapredbloomfilter;

import java.lang.Math;
import java.util.*;
import java.io.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	final double FPR = 0.4; // false positive rate
	final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
	private static Path bloomfilters_output_path = new Path("/user/root/bloomfilters");
	
	/**
	* Map Class for Job 1
	*/
	public static class MapClass extends MapReduceBase
		implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable rate = new IntWritable();
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, OutputCollector<IntWritable,IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String listToken = line.split("\t")[1];
			float avg_rate_float=Float.parseFloat(listToken);
			int avg_rate_int = (int) Math.round(avg_rate_float);
			rate.set(avg_rate_int);
			output.collect(rate, one);
		}
	}
	
	/**
	 * Map Class for Job 2
	*/
	public static class MapClassForJob2 extends MapReduceBase
		implements Mapper<LongWritable, Text, IntWritable, BloomFilter> {
			private OutputCollector<IntWritable,BloomFilter> output;
			ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
		private IntWritable result = new IntWritable();
		@Override
		public void configure(JobConf job) {
			int m = 0;
			int k = 0;
			for (int i = 0; i < 10; i++) {

				m = Integer.parseInt(job.get("bloomfilter" + i + ".m"));
				k = Integer.parseInt(job.get("bloomfilter" + i + ".k"));
				
				bloomFilters.add(i, new BloomFilter(m, k));							
				
			}
			// System.out.println(bloomFilters.size());
		}
		@Override
		public void map(LongWritable key, Text value,
		OutputCollector<IntWritable,BloomFilter> out, Reporter reporter) throws IOException {
			this.output = out;
			String line = value.toString();
			
			String rating = line.split("\t")[1];
			String movieID = line.split("\t")[0];
			int roundedRate = (int) Math.round(Double.parseDouble(rating));
            bloomFilters.get(roundedRate-1).add(movieID);
		}
		@Override
  		public void close() throws IOException {
			  for (int i = 0; i < 10; i++) {
				  result.set(i+1);
				  output.collect(result, bloomFilters.get(i));
			}
		}
	}
	
	/**
 	* Map Class for Job 3 (test the implementation)
	 */
	public static class MapClassForJob3 extends MapReduceBase
	implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private OutputCollector<IntWritable,IntWritable> output;
        ArrayList<BloomFilter> bloomFilters = new ArrayList<>();
        private final int[] counter = new int[10];
		
		@Override
        public void configure(JobConf job)  {
			for (int i = 0; i < 10; i++) {
				Configuration conf = new Configuration();
				conf.setBoolean("fs.hdfs.impl.disable.cache", true);
				try (FileSystem fileSystem = FileSystem.get(conf)) {
					FSDataInputStream fsdis = fileSystem.open(new Path(bloomfilters_output_path + String.valueOf(i+1)));
					BloomFilter tmp = new BloomFilter();
					try {
						tmp.readFields(fsdis);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					bloomFilters.add(i, tmp);
				} catch (IllegalArgumentException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }	
        }
		
		@Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable,IntWritable> out, Reporter reporter) throws IOException{
			this.output = out; //initializing the output collector
			double rating = Double.parseDouble(value.toString().split("\t")[1]);
			int rounded = (int) Math.round(rating);
			String movieID = value.toString().split("\t")[0];
            for (int i=0; i<10; i++) {
				if (rounded != (i+1)){
					if (bloomFilters.get(i).contains(movieID)){
						counter[i]++;	             
					}
				}
            }
        }
		
		@Override
		public void close() throws IOException {
			for (int i = 0; i < 10; i++) {
				output.collect(new IntWritable(i+1), new IntWritable(counter[i]));
			}
		}
    }
	
	/**
	 * Reducer Class for Job 1
	 */

	public static class Reduce extends MapReduceBase
	implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private  IntWritable result = new IntWritable();
    	public void reduce(IntWritable key, Iterator<IntWritable> values,OutputCollector<IntWritable,IntWritable> output, Reporter reporter) throws IOException {
      		int sum = 0;
			  while (values.hasNext()) {
				  sum += values.next().get();
				}
      		result.set(sum);
      		output.collect(key, result);
    	}
	}
	
	/**
	* Reducer Class for Job 2
	*/
	
	public static class ReduceForJob2 extends MapReduceBase
	implements Reducer<IntWritable, BloomFilter, IntWritable, BloomFilter> {
		private BloomFilter result = new BloomFilter();
        public void reduce(IntWritable key, Iterator<BloomFilter> values, OutputCollector<IntWritable,BloomFilter> output, Reporter reporter) throws IOException {
			result = new BloomFilter(values.next());
            while(values.hasNext()) {
                result.or(values.next().getBitset());
            }
			
            // Save the final Bloom Filter in the file system
            Path outputFilePath = new Path(bloomfilters_output_path + key.toString());
            FileSystem fs = FileSystem.get(new Configuration());

            try (FSDataOutputStream fsdos = fs.create(outputFilePath)) {
				result.write(fsdos);
				
            } catch (Exception e) {
				throw new IOException("Error while writing bloom filter to file system.", e);
            }
        }
	}
	
	/**
	 * Reducer Class for Job 3
	*/
	
	static int printUsage() {
		System.out.println("main [-m <maps>] [-r <reduces>] <job_1 input> <job_1 output> <job_2 output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	* The main driver for hadoop map/reduce program.
	* Invoke this method to submit the map/reduce job.
	* @throws IOException When there is communication problems with the
	*                     job tracker.
	*/
	private static double[] getJobOutput(Configuration conf, String pathString) throws IOException {
		double[] tmp = new double[10];
		FileSystem hdfs = FileSystem.get(conf);
		FileStatus[] status = hdfs.listStatus(new Path(pathString));

		for (FileStatus fileStatus : status) {
			if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));

				br.lines().forEach(
					(line) -> {
						String[] keyValueSplit = line.split("\t");
						int key = Integer.parseInt(keyValueSplit[0]);
						int value = Integer.parseInt(keyValueSplit[1]);
						tmp[key-1] = value;
					}
				);
				br.close();
			}
		}
		return tmp;
	}
	
	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		JobConf conf = new JobConf(config, Main.class);
		List<String> other_args = new ArrayList<String>();
		
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
				args[i-1]);
				return printUsage();
			}
		}
		
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(config);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(other_args.get(1)), true);
		fs.delete(new Path(other_args.get(2)), true);
		fs.delete(new Path(other_args.get(3)), true);
		fs.delete(new Path("emptyoutput"), true);

		// remove previous generated bloomfilters
		for(int i=0;i<10;i++)
			fs.delete(new Path(bloomfilters_output_path+String.valueOf(i+1)),true);

		conf.setJobName("counting rates");
		conf.setOutputKeyClass(IntWritable.class); 		// the keys are ratings (ints)
		conf.setOutputValueClass(IntWritable.class);		// the values are counts (ints)
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setInputFormat(NLineInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		conf.setInt("mapred.line.input.format.linespermap", 800000);
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		JobClient.runJob(conf);

		// number of movies in any rate (n)
		double[] numOfMovies = getJobOutput(config, other_args.get(1));

		JobConf conf2 = new JobConf(config, Main.class);
		int[] mArray = new int[10];
		int[] kArray = new int[10];
        for (int i=0; i<10; i++) {
            double n = numOfMovies[i];
            int m = (int)Math.round((-n*Math.log(FPR))/(Math.log(2)*Math.log(2)));
            int k = (int)Math.round((m*Math.log(2))/n);
			mArray[i] = m;
			kArray[i] = k;

			conf2.setInt("bloomfilter" + i + ".m", m);
            conf2.setInt("bloomfilter" + i + ".k", k);
        }

		conf2.setJobName("filling bloom filter");
		
		conf2.setOutputKeyClass(IntWritable.class);
		conf2.setOutputValueClass(BloomFilter.class);
		conf2.setInputFormat(NLineInputFormat.class);
		
		conf2.setMapOutputKeyClass(IntWritable.class);
		conf2.setMapOutputValueClass(BloomFilter.class);

		conf2.setMapperClass(MapClassForJob2.class);
		conf2.setInt("mapred.line.input.format.linespermap", 800000);
		//conf.setCombinerClass(Combiner.class);
		conf2.setReducerClass(ReduceForJob2.class);
		
		bloomfilters_output_path = new Path(other_args.get(2));
		FileInputFormat.setInputPaths(conf2, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf2, new Path("emptyoutput"));
		JobClient.runJob(conf2);
		System.out.println("number of movies at each rate");
		System.out.println(Arrays.toString(numOfMovies));
		System.out.println("m parameter for all 10 bloom filters");
		System.out.println(Arrays.toString(mArray));
		System.out.println("k parameter for all 10 bloom filters");
		System.out.println(Arrays.toString(kArray));
		

		JobConf conf3 = new JobConf(config, Main.class);
		
		conf3.setJobName("testing bloom filter");
		conf3.setOutputKeyClass(IntWritable.class);
		conf3.setOutputValueClass(IntWritable.class);
		conf3.setInputFormat(NLineInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);
		// conf2.setInputFormat(KeyValueTextInputFormat.class);
		
		conf3.setMapOutputKeyClass(IntWritable.class);
		conf3.setMapOutputValueClass(IntWritable.class);

		conf3.setMapperClass(MapClassForJob3.class);
		conf3.setInt("mapred.line.input.format.linespermap", 760000);
		// bloomfilters_output_path = new Path(other_args.get(2));
		conf3.setReducerClass(Reduce.class);
		FileInputFormat.setInputPaths(conf3, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf3, new Path(other_args.get(3)));
		JobClient.runJob(conf3);
		//conf.setCombinerClass(Combiner.class);

		
		
		return 0;
	}
	

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
	}

}
