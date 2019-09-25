package comp9313.ass4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import java.util.StringTokenizer;


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


public class SetSimJoin {
    public static enum count_records{
    	COUNT;
    }
    //input folder
    public static String OUT = "output";
    //output folder
    public static String IN = "input";
    //similarity threshold 
    public static String T="0.6";
    //number of reducers
    public static int REDUCERS = 1;
	
	  public static class RidPair implements WritableComparable<RidPair>{

			private Integer left,right;
			public RidPair(){
			
				}
			public RidPair(Integer int1, Integer int2){
				set(int1,int2);
			}
			public void set(Integer int1,Integer int2){
				left = int1;
				right = int2;
			}
			public Integer getleft(){
				return left;
			}
			public Integer getright(){
				return right;
			}


			@Override
			public void readFields(DataInput in) throws IOException {
				// TODO Auto-generated method stub
	
				
				left = in.readInt();
				right = in.readInt();
				
				
			}
			@Override
			public void write(DataOutput out) throws IOException {
				// TODO Auto-generated method stub
		
				out.writeInt(left);
				out.writeInt(right);
			}
			@Override
			public int compareTo(RidPair o) {
				// TODO Auto-generated method stub
				int cmp = left.compareTo(o.getleft());
				if(cmp != 0){
					return cmp;
				}
				return right.compareTo(o.getright());
				
				
			}




		}
	  
		public static class Stage2Mapper extends Mapper<Object, Text, Text, Text> {
			
			public ArrayList<Object> get_list(Text value){
				ArrayList<Object> result = new ArrayList<Object>();
//				String value_str = value.toString();
				String content = "";
				StringTokenizer itr = new StringTokenizer(value.toString(), " ");
				//get the list of key and values
				ArrayList<String> tokens = new ArrayList<String>();
				int counter=0;
				while (itr.hasMoreTokens()) {
					String token = itr.nextToken();
				
					tokens.add(token);
					if(counter>0){
						content+=token+" ";
					}
					counter+=1;
					
				}
				result.add(tokens);
				result.add(content);
				return result;
			}
			public Integer get_prefix(ArrayList<String> tokens,Double T){
				Integer length = tokens.size()-1;
				
//				Double intersect = length*T;
				Double prefix = Math.ceil(length-Math.floor(length*T)+1);
				Integer prefix_int = prefix.intValue();
				if(prefix_int> length){
					prefix_int = length;
				}
				//prefix_int = length;
				//System.out.println(length+", "+T+", "+intersect+", "+prefix+", "+prefix_int);
				//System.out.println(tokens+": "+prefix_int);
				return prefix_int;
			}
			
			ArrayList<Object> list;
			ArrayList<String> tokens;
			//map
			@SuppressWarnings("unchecked")
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        	Configuration conf = context.getConfiguration();
	        	
//	        	String T_str = conf.get("T");
	        	Double T  = Double.parseDouble(conf.get("T"));
				//System.out.println("T is: "+ T);
				//System.out.println("REducer is: "+ REDUCERS);
				//get a list containing tokens and signatures
				list = get_list(value);
				//get tokens from the first element of the list
//				@SuppressWarnings("unchecked")
				tokens = (ArrayList<String>)list.get(0);
				//get signatures from the second element of the list
//				String content = (String)list.get(1);
				//compute the prefix
				Integer prefix = get_prefix(tokens,T);
				//emit prefix tokens as keys and rid as values
				for (int i =1;i<prefix+1;i++){
//					if(i>cr){
//						return;
//					}
//					String token = tokens.get(i);
//					String rid = tokens.get(0);
//					System.out.println(token+" ## "+rid+";"+content);
					context.write(new Text(tokens.get(i)), new Text(tokens.get(0)+" "+(String)list.get(1)));
					
					
				}
				
//				emit(prefix,tokens,content,context,T);
			}
		
		}

		public static class Stage2Reducer extends Reducer<Text, Text, Text, Text> {

//			public ArrayList<String> cache_values(Iterable<Text> values){
//				//cache the content in values
//				ArrayList<String> tmp_list = new ArrayList<String>();
//				for(Text val:values){
//					
//					//System.out.println("key: "+key.toString()+" val: "+val.toString());
//					tmp_list.add(val.toString());
//					
//				}
//				return tmp_list;
//			}

			//reduce
			ArrayList<String> tmp_list = new ArrayList<String>();
			String[] key_content_1;
			String[] key_content_2;
			HashSet<String> first_set =  new HashSet<String>();
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
	        	Configuration conf = context.getConfiguration();
	        	
	        	String T_str = conf.get("T");
	        	Double T  = Double.parseDouble(T_str);
				
//				ArrayList<String> tmp_list = cache_values(values);
//				ArrayList<String> tmp_list = new ArrayList<String>();
	        	tmp_list.clear();
				for(Text val:values){
					
					//System.out.println("key: "+key.toString()+" val: "+val.toString());
					tmp_list.add(val.toString());
					
				}
				//System.out.println("finished caching");
				String key_str = key.toString();
				//int count=0;
				Integer list_size = tmp_list.size();
				for(int i = 0;i<list_size;i++){
//					String pair1_origin = tmp_list.remove(0);	
//					System.out.println(pair1);
					key_content_1 = tmp_list.remove(0).split(" ");
					
					int len1 = key_content_1.length-1;
					
					first_set.clear();
					for(int no = 0; no<len1; no++){
//						String ele = ;
						first_set.add(key_content_1[no+1]);
					}
					
//					int union_size = len1;
					//String pair1_str = pair1.toString();
					//System.out.println("key: "+key.toString()+"; pair: "+pair1);
					
					for (int j=0; j<tmp_list.size();j++){
//						String pair2_origin = tmp_list.get(j); 
						//count++;
						//System.out.println(count);
						//context.getCounter(count_records.COUNT).increment(1);
						
						//emit_similar(pair1,pair2,context,T);
						
						
						key_content_2 = tmp_list.get(j).split(" ");
//						System.out.println(pair1_origin);
						
						
//						key_content_1 = null;
//						key_content_2 = null;
//						key1 = null;
//						key2 = null;
////						//System.out.println("key: "+key.toString()+"; pair2: "+pair2_str);
////						//System.out.println("key1: "+ key1+": "+key_content_1[1]+"; "+"key2: "+ key2+": "+key_content_2[1]);
						
						int len2 = key_content_2.length-1;
						
						//length filter
						//if (key1 < key2){
							if (len1>len2 && len1*T > len2 || len2>len1 && len2*T>len1){
								continue;
							}
							int is_met_first = 0;
							int union_size = 0;
							int intersec_size = 0;
							String first_inter  = "";
							
							for(int no = 0; no < len2; no++){
//								String ele = key_content_2[no+1];
								if (first_set.contains(key_content_2[no+1])){
									intersec_size+=1;
									if(is_met_first==0){
										is_met_first=1;
										first_inter = key_content_2[no+1];

//										System.out.println("first: "+ele);
									}
								}else{
									union_size+=1;
								}
								
							}
							if(! key_str.equals(first_inter)){
								continue;
							}
							
							union_size += len1;
//							Integer key_int = Integer.valueOf(key.toString());
							
							
							
							Double sim  = ((double) intersec_size/(double) union_size);
//							System.out.println(intersec_size+": "+union_size);
							if(sim >= T){
//								Integer key1 = Integer.parseInt(key_content_1[0]);
//								Integer key2 = Integer.parseInt(key_content_2[0]);
							//context.getCounter(count_records.COUNT).increment(1);
//							
//							context.getCounter(count_records.COUNT).increment(1);
//							//System.out.println("uniq_set_size: "+uniq_set.size());
							if(Integer.parseInt(key_content_1[0])<Integer.parseInt(key_content_2[0])){
								context.write(new Text(key_content_1[0]+","+key_content_2[0]), new Text(sim.toString()));
							}else{
								context.write(new Text(key_content_2[0]+","+key_content_1[0]), new Text(sim.toString()));
							}
							
						}
							
							
							

						
						
					}
				}
			}
		}
		public static class RidPairPartitioner extends Partitioner<RidPair,Text>{

			@Override
			public int getPartition(RidPair rpair, Text similarity, int num_partitions) {
				// TODO Auto-generated method stub
				return Math.abs((rpair.getleft().hashCode()+rpair.getright().hashCode()) % num_partitions);
			}
			
		}

		//mapper of stage3
		public static class Stage3Mapper extends Mapper<Object, Text, RidPair, Text> {
//			public HashSet<RidPair> uniq_set = new HashSet<RidPair>();
			RidPair pair = new RidPair();
			Text sim = new Text();
			String[] val_arr;
			String[] key_arr;
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//				String val  = value.toString();
				//System.out.println("stage3_mapper val: "+val);
				val_arr = value.toString().split("\t");
				key_arr = val_arr[0].split(",");
//				String similarity = val_arr[1];
//				RidPair rpair = new RidPair(Integer.parseInt(key_arr[0]),Integer.parseInt(key_arr[1]));
//				if(! uniq_set.contains(rpair)){
//					uniq_set.add(rpair);
//					context.write(rpair, new Text(similarity));
//				}
				pair.set(Integer.parseInt(key_arr[0]), Integer.parseInt(key_arr[1]));
				sim.set(val_arr[1]);
//				context.write(new RidPair(Integer.parseInt(key_arr[0]),Integer.parseInt(key_arr[1])), new Text(val_arr[1]));
				context.write(pair, sim);

			}
		}
//		public static class Stage3Combiner extends Reducer<RidPair, Text, RidPair, Text> {
//			public void reduce(RidPair key, Iterable<Text> values, Context context) 
//					throws IOException, InterruptedException {
//				Iterator<Text> itr = values.iterator();
//				
//				
//				//String sim = itr.next().toString();
//				//System.out.println("stage3_reducer"+key_str+": "+sim);
//				context.write(key,itr.next());
//			}
//		}
		
		//reducer of stage3
		public static class Stage3Reducer extends Reducer<RidPair, Text, Text, Text> {
			Iterator<Text> itr;
			public void reduce(RidPair key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				itr = values.iterator();
//				String key_str = "("+key.getleft().toString()+","+key.getright().toString()+")";
				
				//String sim = itr.next().toString();
				//System.out.println("stage3_reducer"+key_str+": "+sim);
				context.write(new Text("("+key.getleft().toString()+","+key.getright().toString()+")"),itr.next());
			}
		}
	public static void main(String[] args) throws Exception{
        IN = args[0];
        OUT = args[1];
        T = args[2];
        //System.out.println("T is: "+ T);
        REDUCERS = Integer.parseInt(args[3]);
        //System.out.println("REDUCERS is: "+ REDUCERS);
        String input = IN;
        
        String output = OUT + "similarity";
		Configuration conf = new Configuration();
		conf.set("T", T);
		
		
		Job job = Job.getInstance(conf, "get_similarity");
		job.setJarByClass(SetSimJoin.class);
		job.setMapperClass(Stage2Mapper.class);
		job.setReducerClass(Stage2Reducer.class);
		//This is the output of the mapper
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(REDUCERS);
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
//		Counters cuns = job.getCounters();
//		Counter c1 = cuns.findCounter(count_records.COUNT);
//		long counter = c1.getValue();
//		System.out.println("Numer of records: "+counter);
		
		
		//next phase, remove duplication
        input = output;
        output = OUT + "remove_dup";
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "remove_duplication");
		job2.setJarByClass(SetSimJoin.class);
		job2.setMapperClass(Stage3Mapper.class);
		job2.setReducerClass(Stage3Reducer.class);
//		job2.setCombinerClass(Stage3Combiner.class);
		//This is the output of the mapper
		job2.setOutputKeyClass(RidPair.class);
		job2.setOutputValueClass(Text.class);
//		job2.setPartitionerClass(RidPairPartitioner.class);
		FileInputFormat.addInputPath(job2, new Path(input));
		FileOutputFormat.setOutputPath(job2, new Path(output));
		job2.setNumReduceTasks(REDUCERS);
		//job2.waitForCompletion(true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
        

	}

}
