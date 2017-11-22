package comp9313.ass4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
* Project 4 for COMP9313
* Author: Xinhong Wu
* ZID: z5089853
*/



public class SetSimJoin {
	
	/**
	* Utilize a IntPair Class to help sort the output in the final MapReduce
	*/

	public static class IntPair implements WritableComparable<IntPair>{

		private int first;
		private int second;
		public IntPair(){

		}
		public IntPair(int f, int s){
			if (f <= s){
				set(f, s);
			} else {
				set(s, f);
			}
		}
		
		public void set(int f, int s){
			this.first = f;
			this.second = s;
		}
		public int getFirst(){
			return first;
		}
		public int getSecond(){
			return second;
		}
		
		public String output(){
			String result = "(" + first + "," + second + ")";
			return result;
		}
		
		public int compareTo(IntPair target) {
			if (first == target.getFirst()){
				return second - target.getSecond();
			} else{
				return first - target.getFirst();
			}
		}
		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			first = arg0.readInt();
			second = arg0.readInt();
			
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeInt(first);
			arg0.writeInt(second);
			
		}
	}




	/**
	* The first Map stage, find the prefix and output the prefix as key, the ID with items as value
	* 		Input format:
	*			ID 		item1	item2	item3	...
	*		Output format:
	*			key: prefix_item1	value: ID|item1,item2,item3,...
	*			key: prefix_item2	value: ID|item1,item2,item3,...
	*			key: prefix_item2	value: ID|item1,item2,item3,...
	*			...
	*/

	public static class FirstMapper extends Mapper<Object, Text, Text, Text> {	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			ArrayList<String> data = new ArrayList<String>();
			Double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
			
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			while (itr.hasMoreTokens()) {
				data.add(itr.nextToken());
			}
			String keyValuePair = data.get(0) + "|";
			for(int i = 1; i < data.size(); i++){
				keyValuePair += data.get(i) + ",";
			}
			keyValuePair = keyValuePair.substring(0, keyValuePair.length() - 1);

			double prefixLength = data.size() - 1 - Math.ceil((data.size() - 1) * threshold) + 1;
			
			for(int i = 1; i <= prefixLength; i++){
				context.write(new Text(data.get(i)), new Text(keyValuePair));
			} 
		}		
	}
	
	/**
	* Partitioner used to balance the workload
	*/
	public static class FirstPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text arg0, Text arg1, int numOfPartition) {
			return Math.abs((Integer.parseInt(arg0.toString())) % numOfPartition);
		}
		
	}
	
	/**
	* The first Reduce stage, calculate the similarity with same prefix item
	* 		Input format:
	*			key: prefix_item1
	*			value: ID1|item1,item2,item3,... ; ID2|item1,item2,item3,...  ; ID3|item1,item2,item3,...  ; ...
	*		Output format: 
	*			ID1 	ID2 	similarity
	*			ID1 	ID3 	similarity
	* 			ID2 	ID2 	similarity
	* 			...
	*
	*/
	
	public static class FirstReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
			ArrayList<String> dataList = new ArrayList<String>();
			

			for (Text val : values) {
				dataList.add(val.toString());
			}
			
			for(int i = 0; i < dataList.size() - 1; i++){
				for(int j = i+1; j < dataList.size(); j++){
					
					double numOfCommon = 0.0; 
					double similarity = 0.0;
					
					int id1 = Integer.parseInt(StringUtils.split(dataList.get(i), '|')[0]);
					int id2 = Integer.parseInt(StringUtils.split(dataList.get(j), '|')[0]);
					
					String id1Data = StringUtils.split(dataList.get(i), '|')[1];
					String id2Data = StringUtils.split(dataList.get(j), '|')[1];
					String id1DataList[] = StringUtils.split(id1Data, ',');
					String id2DataList[] = StringUtils.split(id2Data, ',');
						
					int id1DataLength = id1DataList.length;
					for(int k = 0; k < id2DataList.length; k++){
						if(ArrayUtils.contains(id1DataList, id2DataList[k])){
							numOfCommon++;
						}else{
							id1DataLength++;
						}
					}
						
					similarity = numOfCommon / id1DataLength;
						
					if(similarity >= threshold){
						context.write(new Text("" + id1), new Text(id2 + "\t" + similarity));	
					}
				}
			}
		}

	}

	/**
	* The final Mapper, read the two set IDs and similarity, transform the set IDs to IntPair type, then output with similarity
	* Input format:
	*		id1 	id2 	similarity
	* Output format:
	* 		key: IntPair(id1, id2)		value: Text(similarity)
	*/

	public static class FinalMapper extends Mapper<Object, Text, IntPair, Text> {	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			ArrayList<String> line = new ArrayList<String>();
			StringTokenizer token = new StringTokenizer(value.toString(), "\t");
			
			while (token.hasMoreTokens()) {
				line.add(token.nextToken());
			}
			context.write(new IntPair(Integer.parseInt(line.get(0)), Integer.parseInt(line.get(1))), new Text(line.get(2)));
		}		
	}

	/**
	* Partitioner used to balance the workload
	*/

	public static class FinalPartitioner extends Partitioner<IntPair, Text>{

		@Override
		public int getPartition(IntPair arg0, Text arg1, int numOfPartition) {
			return Math.abs((arg0.getFirst() + arg0.getSecond()) % numOfPartition);
		}
		
	}

	/**
	* The final Reducer, remove the Duplicates
	* Input format:
	* 		IntPair(id1, id2)	similarity
	* Output format:
	*		key: Text(id1)		value: Text(id2 	similarity)
	*/

	public static class FinalReducer extends Reducer<IntPair, Text, Text, Text> {

		public void reduce(IntPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String sim = "";
			for (Text val : values) {
				sim = val.toString();
				break;
			}
			String outputKey = key.output();
			context.write(new Text(outputKey), new Text(sim));
		}
	}
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		String input = args[0];
		String output = args[1];
		String threshold = args[2];
		int tasks = Integer.parseInt(args[3]);
		conf.set("threshold", threshold);

		Job job = Job.getInstance(conf, "First");
		job.setNumReduceTasks(tasks);
	
		// stage_2 map reduce
		
		 
		job.setJarByClass(SetSimJoin.class);
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		Path temp = new Path(output + "_temp");
		FileOutputFormat.setOutputPath(job, temp);
		
		job.waitForCompletion(true);
		
		// stage_3 map reduce
	
		job = Job.getInstance(conf, "Final");
		job.setNumReduceTasks(tasks);
		job.setJarByClass(SetSimJoin.class);
		job.setMapperClass(FinalMapper.class);
		job.setReducerClass(FinalReducer.class);
		job.setPartitionerClass(FinalPartitioner.class);
		job.setOutputKeyClass(IntPair.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, temp);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
