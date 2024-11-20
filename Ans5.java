import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Ans5 {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text femaleKey = new Text("FemaleVoters");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length == 4) {
                String gender = fields[2].trim(); 
                
                if (gender.equalsIgnoreCase("Female")) {
                    context.write(femaleKey, one);
                }
            }
        }
    }


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(new Text("No. of female voters are : "), new IntWritable(sum));
        }
    }

    
    public static void main(String[] args) throws Exception {
    	 Path input = new Path(args[0]);
         Path output = new Path(args[1]);
          
         
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "WordCount with Average");
         
        
         job.setJarByClass(Ans5.class);
         
         job.setMapperClass(Map.class);
         job.setReducerClass(Reduce.class);
         
     
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(IntWritable.class);  
         
    
         job.setInputFormatClass(TextInputFormat.class);
         job.setOutputFormatClass(TextOutputFormat.class);
         
         
         job.setNumReduceTasks(1);
         
    
         FileInputFormat.setInputPaths(job, input);
         FileOutputFormat.setOutputPath(job, output);
         
       
         System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
