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
import java.lang.Math;


public class K_Means_Euclidean {
	
	/*---------------------------------------------------------
	*class Vector
	*		A vector, and the distance with the centroid for Euclidean distance.
	---------------------------------------------------------*/
	public static class Vector implements WritableComparable<Vector> {
		public ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		public DoubleWritable distance = new DoubleWritable(0);
		
		public void set(ArrayList<DoubleWritable> val, double dis){
			value = new ArrayList<DoubleWritable>(val);
			distance.set(dis);
		}
		
		public void set(ArrayList<DoubleWritable> val){
			value = new ArrayList<DoubleWritable>(val);
		}
		
		public Vector(){
			
		}
		
		public Vector(Vector that){
			this.value = new ArrayList<DoubleWritable>(that.value);
			this.distance.set(that.distance.get());
		}
		
		public double DistanceWith(Vector that){
			double sum = 0;
			for(int i = 0 ; i < value.size() ; i++){
				double tmp = this.value.get(i).get() - that.value.get(i).get();
				tmp *= tmp;
				sum += tmp;
			}
			return Math.sqrt(sum);
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
			String str = "";
			Iterator<DoubleWritable> it = this.value.iterator();
			str += it.next().get();
			while(it.hasNext()){
				str += " " + String.valueOf(it.next().get());
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
		extends Mapper<Object, Text, Vector, Vector>{
			
		private Vector keyOut = new Vector();
		private Vector valueOut = new Vector();
			
		public void map(Object keyIn, Text valueIn, Context context
						) throws IOException, InterruptedException {
			
			ArrayList<DoubleWritable> vectorValues = new ArrayList<DoubleWritable>();
			StringTokenizer itr = new StringTokenizer(valueIn.toString(), " \t");
			while(itr.hasMoreTokens()){
				DoubleWritable tmp = new DoubleWritable(Double.parseDouble(it.next().get()));
				vectorValues.add(tmp);
			}
			
			//TODO: Calculate distance and find minima
			//Configuration conf = context.getConfiguration();
			//Path centInput = new Path(conf.get("centInput"));
			
			keyOut.set(/*Centroid*/);
			valueOut.set(vectorValues, /*distance*/);
			context.write(keyOut, valueOut);
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		
		centInput = otherArgs[0];
		centOutput = otherArgs[1];
		costOutput = otherArgs[2];
		
		//for loop begin
		
			//conf.set("numOfPages", "0");
			
			Job job1 = new Job(conf, ("Euclidean" + String.valueOf(i)));
			job1.setJarByClass(K_Means_Euclidean.class);
			
			job1.setMapperClass(EuclideanMapper.class);
			job1.setReducerClass(EuclideanReducer.class);
			
			job1.setMapOutputKeyClass(Vector.class);
			job1.setMapOutputValueClass(Vector.class);
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(DegreeAndDestinationSet.class);
			
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
			
			job1.waitForCompletion(true);
			
			centInput = centOutput;
			centOutput += String.valueOf(i);
			costOutput += String.valueOf(i);
		
		//for loop end
		
		System.exit(1);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}