
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCount extends Configured implements Tool {

   static int printUsage() {
       System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
       ToolRunner.printGenericCommandUsage(System.out);
       return -1;
   }

   public static class WordCountMapper
           extends Mapper<Object, Text, Text, DoubleWritable> {

       // so we don't have to do reallocations
       private final static IntWritable one = new IntWritable(1);
       private Text fair_amount = new Text();
       private Text date_as_key = new Text();

       // to check for only alphanumeric
       String expression1 = "\\d{1,3}(\\.\\d{1,3})?";
       Pattern pattern1 = Pattern.compile(expression1);

       String expression2 = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
       Pattern pattern2 = Pattern.compile(expression2);

       public void map(Object key, Text value, Context context
       ) throws IOException, InterruptedException {
           StringTokenizer itr = new StringTokenizer(value.toString(),  ",");
           int number_cnt = 0;
           while (itr.hasMoreTokens()) {
               String nextToken = itr.nextToken ();
               Matcher num_matcher = pattern1.matcher(nextToken);
               Matcher date_matcher = pattern2.matcher(nextToken);

               if (num_matcher.matches()){
                   number_cnt += 1;
                   if(number_cnt == 6){
                       fair_amount.set(nextToken);
                       double fair_amount_double = Double.parseDouble(fair_amount.toString());
                       DoubleWritable n = new DoubleWritable(fair_amount_double);
                       context.write(date_as_key, n);
                       number_cnt = 0;
                   }
               }

               else if(date_matcher.matches()){
                   nextToken = nextToken.substring(0, 10);
                   date_as_key.set(nextToken);
               }
           }
       }
   }

   public static class WordCountReducer
           extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
       private DoubleWritable result = new DoubleWritable();

       public void reduce(Text key, Iterable<DoubleWritable> values,
                          Context context
       ) throws IOException, InterruptedException {
           double sum = 0.0;
           for (DoubleWritable val : values) {
               sum += val.get();
           }
           context.write(key, new DoubleWritable(sum));
       }
   }

   public int run(String[] args) throws Exception {

       Configuration conf = new Configuration();
       Job job = Job.getInstance(conf, "word count");
       job.setJarByClass(WordCount.class);
       job.setMapperClass(WordCountMapper.class);
       job.setCombinerClass(WordCountReducer.class);
       job.setReducerClass(WordCountReducer.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(DoubleWritable.class);

       List<String> other_args = new ArrayList<String>();
       for(int i=0; i < args.length; ++i) {
           try {
               if ("-r".equals(args[i])) {
                   job.setNumReduceTasks(Integer.parseInt(args[++i]));
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
       // Make sure there are exactly 2 parameters left.
       if (other_args.size() != 2) {
           System.out.println("ERROR: Wrong number of parameters: " +
                   other_args.size() + " instead of 2.");
           return printUsage();
       }
       FileInputFormat.setInputPaths(job, other_args.get(0));
       FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
       return (job.waitForCompletion(true) ? 0 : 1);
   }

   public static void main(String[] args) throws Exception {
       int res = ToolRunner.run(new Configuration(), new WordCount(), args);
       System.exit(res);
   }

}
