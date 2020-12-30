import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashMap.*;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class invertedindex {
  public static class MapCounter 
       extends Mapper<LongWritable, Text, WordPair, IntWritable>{
    private Text File;
	private WordPair wordPair = new WordPair();
    private Map<String, Integer> map;       
    
	
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
		if (map == null) {
	        map = new HashMap<String, Integer>();
	    }
	    StringTokenizer iteration = new StringTokenizer(value.toString());
		File = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
		
	    while(iteration.hasMoreTokens()){
	        
	        String word = iteration.nextToken();
	        
	        if(map != null) {
				if(map.containsKey(word)){
	            int total = map.get(word) + 1;
	            map.put(word, total);
				}
	        }
			if (map.containsKey(word) == false || map == null){
	            map.put(word, 1);
	        }
	    }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (map == null) {
	        map = new HashMap<String, Integer>();
        }

	
		Iterator<Map.Entry<String, Integer>> itr = map.entrySet().iterator();
        while (itr.hasNext()) {
				Map.Entry<String, Integer> entry = itr.next();
                String keystring = entry.getKey();
                wordPair.setWord(keystring);
                wordPair.setNeighbor(File.toString());

                context.write(wordPair, new IntWritable(entry.getValue()));
        }
    }
    
    

   
  }
  public static class ReduceCounter extends Reducer<WordPair,IntWritable,Text,Text> {
	  private Map<String, String> map;
    private IntWritable var = new IntWritable();
    private String previous = "";
    
	
    public void reduce(WordPair key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		int sum = 0;
                           
      if (map == null) {
	        map = new HashMap<String, String>();
      }
      
      for (IntWritable value : values) {
		  
          if (!key.getWord().toString().equals(previous) && !previous.equals("")) {
			  
              sum += value.get();
              var.set(sum);
              
              Map.Entry<String, String> entry = map.entrySet().iterator().next();
              String keyn = entry.getKey();
              String valuen = entry.getValue();
              
              context.write(new Text(keyn), new Text(valuen));
              map.clear();
          }
          String keys = map.get(key.getWord().toString()) + ";" + key.getNeighbor().toString() + ":" + value.get();

          if(map.containsKey(key.getWord().toString())) {
              map.put(key.getWord().toString(), keys);
          } else {
              map.put(key.getWord().toString(), key.getNeighbor().toString() + ": " + value.get());
          }
          
          previous = key.getWord().toString();
        
      }

    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Map.Entry<String, String> entry = map.entrySet().iterator().next();
        String keys = entry.getKey();
        String values = entry.getValue();
        
        context.write(new Text(keys), new Text(values));
        map.clear();
    }
    
  }
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
     System.err.println("Usage: invertedindex <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Inverted Index");
    job.setJarByClass(invertedindex.class);
 
    job.setMapOutputKeyClass(WordPair.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(MapCounter.class);   
    job.setReducerClass(ReduceCounter.class);
    job.setNumReduceTasks(3);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}