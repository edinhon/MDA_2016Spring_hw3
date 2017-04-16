package org.apache.hadoop.mdahw3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Iterator;


public class K_Means_Manhattan {
	
	public static int numOfPages;
	
	public static class DegreeAndDestinationSet implements Writable {
		public IntWritable degree = new IntWritable();
		public ArrayList<IntWritable> destinationSet = new ArrayList<IntWritable>();
		
		public void set(int deg, ArrayList<IntWritable> des){
			degree.set(deg);
			destinationSet = new ArrayList<IntWritable>(des);
		}
		
		public DegreeAndDestinationSet(){
			
		}
		
		public DegreeAndDestinationSet(DegreeAndDestinationSet that){
			this.degree.set(that.degree.get());
			this.destinationSet = new ArrayList<IntWritable>(that.destinationSet);
		}
		
		@Override
        public void readFields(DataInput data) throws IOException {
            this.degree.readFields(data);
			Iterator<IntWritable> it = this.destinationSet.iterator();
			while(it.hasNext()){
				it.next().readFields(data);
			}
        }
		
        @Override
        public void write(DataOutput data) throws IOException {
            this.degree.write(data);
			Iterator<IntWritable> it = this.destinationSet.iterator();
			while(it.hasNext()){
				it.next().write(data);
			}
        }
		
		@Override
		public String toString() {
			String str;
			str = String.valueOf(degree.get());
			Iterator<IntWritable> it = this.destinationSet.iterator();
			while(it.hasNext()){
				str += "," + String.valueOf(it.next().get());
			}
			return str;
		}
	}

	/*----------------------------------------
	*class GoogleMatrixMapper
	*	Read input file and parse into {i, j}, 
	*	i = page start, j = page destination.
	----------------------------------------*/
	public static class GoogleMatrixMapper 
		extends Mapper<Object, Text, IntWritable, IntWritable>{
			
		private IntWritable keyOut = new IntWritable();
		private IntWritable valueOut = new IntWritable();
			
		public void map(Object keyIn, Text valueIn, Context context
						) throws IOException, InterruptedException {
							
			//Configuration conf = context.getConfiguration();
			//int numOfPages = Integer.parseInt(conf.get("numOfPages"));
			
			StringTokenizer itr = new StringTokenizer(valueIn.toString(), " \t");
			while(itr.hasMoreTokens()){
				int i = Integer.parseInt(itr.nextToken());
				int j = Integer.parseInt(itr.nextToken());
				//if(i > numOfPages) numOfPages = i;
				//if(j > numOfPages) numOfPages = j;
				keyOut.set(i);
				valueOut.set(j);
				context.write(keyOut, valueOut);
			}
			
			//conf.set("numOfPages", String.valueOf(numOfPages));
		}
	}
	
	/*----------------------------------------
	*class GoogleMatrixReducer
	*	Take {i, j} and reduce into {(i, j), out_degree},
	*	(i, j) = index of matrix, out_degree = the real value's denominator,
	*	the real value is (1/out_degree).
	----------------------------------------*/
	public static class GoogleMatrixReducer 
		extends Reducer<IntWritable, IntWritable, IntWritable, DegreeAndDestinationSet> {
		
		private IntWritable keyOut = new IntWritable();
		private DegreeAndDestinationSet valueOut = new DegreeAndDestinationSet();
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
						) throws IOException, InterruptedException {
							
			ArrayList<IntWritable> array = new ArrayList<IntWritable>();
			Iterator<IntWritable> it = values.iterator();
			while(it.hasNext()){
				IntWritable des = new IntWritable(it.next().get());
				array.add(des);
			}
			
			keyOut.set(key.get());
			valueOut.set(array.size(), array);
			context.write(keyOut, valueOut);
		}
	}
	
	/*----------------------------------------
	*Method run(String[] args)
	*	The args should include input path of graph file in args[0], 
	*	and output path of {source, (degree, destination set)} in args[1], 
	*	and return number of pages.
	----------------------------------------*/
	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	
		if (args.length != 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		
		//conf.set("numOfPages", "0");
		
		Job job1 = new Job(conf, "GoogleMatrix");
		job1.setJarByClass(GoogleMatrix.class);
		
		job1.setMapperClass(GoogleMatrixMapper.class);
		job1.setReducerClass(GoogleMatrixReducer.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DegreeAndDestinationSet.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.waitForCompletion(true);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}