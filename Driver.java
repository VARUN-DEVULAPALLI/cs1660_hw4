import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
  
public class Driver { 
  
    public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, 
                                  args).getRemainingArgs(); 
  
        // if less than two paths  
        // provided will show error 
        if (otherArgs.length < 2)  
        { 
            System.err.println("Error: please provide two paths"); 
            System.exit(2); 
        } 
  
        Job job = Job.getInstance(conf, "Least 5"); 
        job.setJarByClass(Driver.class); 
  
        job.setMapperClass(least_5_mapper.class); 
        job.setReducerClass(least_5_reducer.class); 
  
        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 
  
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        //remove if you only have one input
        FileInputFormat.addInputPath(job, new Path(otherArgs[1])); 
        FileInputFormat.addInputPath(job, new Path(otherArgs[2])); 
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 
