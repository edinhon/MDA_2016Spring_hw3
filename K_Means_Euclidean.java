package org.apache.hadoop.mdahw3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
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


public class K_Means_Euclidean {
	
	/*---------------------------------------------------------
	*class Vector
	*		A vector, and the distance with the centroid.
	---------------------------------------------------------*/
	public static class Vector implements WritableComparable<Vector> {
		public ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		public DoubleWritable distance = new DoubleWritable(0);
		
		public void set(ArrayList<DoubleWritable> val, double dis){
			value = new ArrayList<DoubleWritable>(val);
			distance.set(dis);
		}
		
		public Vector(){
			
		}
		
		public Vector(Vector that){
			this.value = new ArrayList<DoubleWritable>(that.value);
			this.distance.set(that.distance);
		}
		
		@Override
        public void readFields(DataInput data) throws IOException {
            this.distance.readFields(data);
			Iterator<DoubleWritable> it = this.value.iterator();
			while(it.hasNext()){
				it.next().readFields(data);
			}
        }
		
        @Override
		public void write(DataOutput data) throws IOException {
			this.distance.write(data);
			Iterator<DoubleWritable> it = this.value.iterator();
			while(it.hasNext()){
				it.next().write(data);
			}
        }
		
		@Override
		public String toString() {
			String str;
			Iterator<DoubleWritable> it = this.value.iterator();
			str += it.next().get();
			while(it.hasNext()){
				str += "," + String.valueOf(it.next().get());
			}
			return str;
		}
		
		@Override
		public int compareTo(Vector that) {
			for(int i = 0 ; i < this.value.size() ; i++){
				if(this.value.get(i).get() != that.value.get(i).get()){
					if(this.value.get(i).get() > that.value.get(i).get()){
						return 1;
					} else return -1;
				}
			}
			return 0;
		}
	}

	/*----------------------------------------
	*class EuclideanMapper
	*		Read data to parse a vector, and calculate Euclidean distance 
	*	with each centroid vector which is read in Mapper.
	*		Output key is the centroid vector which is closest with the read vector, 
	*	and output value is the read vector.
	----------------------------------------*/
	public static class EuclideanMapper 
		extends Mapper<Object, Text, IntWritable, IntWritable>{
			
		private IntWritable keyOut = new IntWritable();
		private IntWritable valueOut = new IntWritable();
			
		public void map(Object keyIn, Text valueIn, Context context
						) throws IOException, InterruptedException {
							
			
		}
	}
	
	/*----------------------------------------
	*class EuclideanReducer
	*		Take the vectors in the same cluster to calculate new centroid, 
	*	return k centroid vectors.
	*		Output key is the new centroid vector, and output value is null.
	----------------------------------------*/
	public static class EuclideanReducer 
		extends Reducer<IntWritable, IntWritable, IntWritable, DegreeAndDestinationSet> {
		
		private IntWritable keyOut = new IntWritable();
		private DegreeAndDestinationSet valueOut = new DegreeAndDestinationSet();
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context
						) throws IOException, InterruptedException {
							
			
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
			System.err.println("Usage: Euclidean <in> <out>");
			System.exit(2);
		}
		
		//conf.set("numOfPages", "0");
		
		Job job1 = new Job(conf, "Euclidean");
		job1.setJarByClass(K_Means_Euclidean.class);
		
		job1.setMapperClass(EuclideanMapper.class);
		job1.setReducerClass(EuclideanReducer.class);
		
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