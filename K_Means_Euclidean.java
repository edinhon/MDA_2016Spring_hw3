package org.apache.hadoop.mdahw3;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Math;
import java.lang.String;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

public class K_Means_Euclidean {
	
	/*---------------------------------------------------------
	*class Vector
	*		A vector, and the distance with the centroid for Euclidean distance.
	---------------------------------------------------------*/
	public static class Vector implements Writable {
		public ArrayList<DoubleWritable> value = new ArrayList<DoubleWritable>();
		public DoubleWritable distance = new DoubleWritable(0);
		
		public void setValue(ArrayList<DoubleWritable> val){
			value = new ArrayList<DoubleWritable>(val);
		}
		
		public void setDis(double dis){
			distance.set(dis);
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
			Iterator<DoubleWritable> it = this.value.iterator();
			while(it.hasNext()){
				it.next().readFields(data);
			}
			this.distance.readFields(data);
        }
		
        @Override
		public void write(DataOutput data) throws IOException {
			Iterator<DoubleWritable> it = this.value.iterator();
			while(it.hasNext()){
				it.next().write(data);
			}
			this.distance.write(data);
        }
		
		@Override
		public String toString() {
			String str = "";
			Iterator<DoubleWritable> it = this.value.iterator();
			if(it.hasNext()) str += String.valueOf(it.next().get());
			while(it.hasNext()){
				str += " " + String.valueOf(it.next().get());
			}
			return str;
		}
	}

	/*----------------------------------------
	*class EuclideanMapper
	*		Read data to parse a vector, and calculate Euclidean distance 
	*	with each centroid vector which is read in Mapper.
	*		First output key is the centroid vector which is closest with the read vector, 
	*	and output value is the read vector with distance.
	*		Second output key is a empty Vector which type is "C", used on cost function, 
	*	and output value is the read vector with distance.
	----------------------------------------*/
	public static class EuclideanMapper 
		extends Mapper<Object, Text, IntWritable, Vector>{
			
		private IntWritable keyOut = new IntWritable();
		private IntWritable keyOut2 = new IntWritable();
		private Vector valueOut = new Vector();
			
		public void map(Object keyIn, Text valueIn, Context context
						) throws IOException, InterruptedException {
			
			Vector vec = new Vector();
			ArrayList<DoubleWritable> vectorValues = new ArrayList<DoubleWritable>();
			StringTokenizer itr = new StringTokenizer(valueIn.toString(), " \t");
			while(itr.hasMoreTokens()){
				DoubleWritable tmp = new DoubleWritable(Double.parseDouble(itr.nextToken()));
				vectorValues.add(tmp);
			}
			vec.setValue(vectorValues);
			
			//Calculate distance and find minima
			double min = Double.MAX_VALUE;
			int minIndex = -1;
			Vector[] centroids = new Vector[10];
			Configuration conf = context.getConfiguration();
			String[] centroidsStr = conf.getStrings("centInput");
			
			for(int i = 0 ; i < centroidsStr.length ; i++){
				
				centroids[i] = new Vector();
				
				ArrayList<DoubleWritable> centValues = new ArrayList<DoubleWritable>();
				StringTokenizer itr2 = new StringTokenizer(centroidsStr[i], " \t");
				while(itr2.hasMoreTokens()){
					DoubleWritable tmp = new DoubleWritable(Double.parseDouble(itr2.nextToken()));
					centValues.add(tmp);
				}
				centroids[i].setValue(centValues);
				
				double dis = vec.DistanceWith(centroids[i]);
				if(dis <= min) {
					min = dis;
					minIndex = i;
				}
			}
			
			if(minIndex == -1) {
				System.err.println("There no any centroid or something wrong!");
				return;
			}
			
			//Output two types: used on cluster and on cost function.
			vec.setDis(min);
			
			valueOut = new Vector(vec);
			
			keyOut.set(minIndex);
			context.write(keyOut, valueOut);
			
			keyOut2.set(-1);
			context.write(keyOut2, valueOut);
			
			System.out.println(valueOut.toString());
		}
	}
	
	/*----------------------------------------
	*class EuclideanReducer
	*		Take the vectors in the same cluster to calculate new centroid and cost function.
	*	Cluster Output key is the new centroid vector, and output value is null. Cost Output 
	*	key is null, output value is cost function.
	----------------------------------------*/
	public static class EuclideanReducer 
		extends Reducer<IntWritable, Vector, Text, Vector> {
		
		private Text keyOut = new Text("");
		private Vector valueOut = new Vector();
		
		private MultipleOutputs<Text, Vector> out;

		@Override
		public void setup(Context context) {
			out = new MultipleOutputs<Text, Vector>(context);
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			out.close();
		}
		
		public void reduce(IntWritable key, Iterable<Vector> values, Context context
						) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			String centOutputPath = conf.get("centOutputPath");
			String costOutputPath = conf.get("costOutputPath");
			System.out.println(centOutputPath +"+"+costOutputPath);
			
			ArrayList<Vector> valuesArray = new ArrayList<Vector>();
			Iterator<Vector> it = values.iterator();
			while(it.hasNext()){
				//Vector v = new Vector(it.next());
				//valuesArray.add(v);
				System.out.println(it.next().toString());
				//System.out.println(v.toString());
			}
			/*
			if(key.get() != -1){
				System.out.println("Hi Im here");
				System.out.println(String.valueOf(valuesArray.get(0).value.size()));
				for(int i = 0 ; i < valuesArray.get(0).value.size() ; i++){
					System.out.println("Hi Im here");
					double sum = 0;
					for(int j = 0 ; j < valuesArray.size() ; j++){
						sum += valuesArray.get(j).value.get(i).get();
					}
					sum /= (double)valuesArray.size();
					
					DoubleWritable dw = new DoubleWritable();
					dw.set(sum);
					valueOut.value.add(dw);
				}
				keyOut.set("");
				out.write(keyOut, valueOut, centOutputPath+"/cent");
			}
			//Cost function
			else if(key.get() == -1){
				double sum = 0;
				for(int i = 0 ; i < valuesArray.size() ; i++){
					double tmp = valuesArray.get(i).distance.get();
					sum += (tmp * tmp);
				}
				keyOut.set(String.valueOf(sum));
				valueOut = new Vector();
				out.write(keyOut, valueOut, costOutputPath+"/cost");
			}*/
		}
	}
	
	/*----------------------------------------
	*Method main(String[] args)
	*		Run the chain for loop job and output the new centroid and cost function.
	----------------------------------------*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		
		String dataInputPath = otherArgs[0];
		String centInputPath = otherArgs[1];
		String outputPath = otherArgs[2];
		String centOutputPath = otherArgs[3];
		String costOutputPath = otherArgs[4];
		
		for(int i = 0 ; i < 1 ; i++){
			
			String[] centroidsStr = new String[10];
			Path path = new Path(centInputPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			
			int j = 0;
			String line;
			line = br.readLine();
			while (line != null){
				if(j >= 10) {
					System.err.println("Too many centroids!");
					break;
				}
				centroidsStr[j] = line;
				j++;
				line = br.readLine();
			}
			conf.setStrings("centInput", centroidsStr);
			conf.set("centOutputPath", centOutputPath + String.valueOf(i));
			conf.set("costOutputPath", costOutputPath + String.valueOf(i));
			
			Job job1 = new Job(conf, ("Euclidean" + String.valueOf(i)));
			job1.setJarByClass(K_Means_Euclidean.class);
			
			job1.setMapperClass(EuclideanMapper.class);
			job1.setReducerClass(EuclideanReducer.class);
			
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Vector.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Vector.class);
			
			job1.setInputFormatClass(TextInputFormat.class);
			//job1.setOutputFormatClass(TextOutputFormat.class);
			LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
			FileInputFormat.addInputPath(job1, new Path(dataInputPath));
			FileOutputFormat.setOutputPath(job1, new Path(outputPath));
			
			job1.waitForCompletion(true);
			
			centInputPath = outputPath + "/" + centOutputPath + String.valueOf(i);
		}
		
		System.exit(1);
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}