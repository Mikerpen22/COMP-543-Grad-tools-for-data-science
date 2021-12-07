import java.io.IOException;
import java.sql.Array;
import java.util.*;

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

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCount extends Configured implements Tool {

    static int printUsage() {
        System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }



    public static class WordCountMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text driver_license_as_key = new Text();
        private Text fair_amount = new Text();
        private Object CustomComparer;
        private PriorityQueue<Tuple> top_5_revenue = new PriorityQueue<Tuple>( new Comparator<Tuple>() {
            public int compare(Tuple o1, Tuple o2) {
                return o1.rev < o2.rev ? -1 : 1;
            }
        });
        public static class Tuple{
            String id;
            double rev;
            public Tuple(String id, double rev){
                this.id = id;
                this.rev = rev;
            }
            public Text toText(){
                Text t = new Text();
                String newStr = this.id + "," + String.valueOf(this.rev);
                t.set(newStr);
                return t;
            }
            public Tuple fromText(Text taxi_text){
                String[] splitStr = taxi_text.toString().split(",");
                // this if statement is for debugging
                Tuple taxi = new Tuple("", 0);

                if (splitStr.length == 2){
                    taxi.id = splitStr[0];
                    taxi.rev = Double.parseDouble(splitStr[1]);
                }
                return taxi;
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException{
            Text taxiText = new Text();
            Text placeHolder = new Text("one");

            while(top_5_revenue.size() > 0){
                Tuple top = top_5_revenue.poll();
                String taxiStr = top.id + "," + String.valueOf(top.rev);
                taxiText.set(taxiStr);
                context.write(placeHolder, taxiText);
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
            int temp_cnt = 0;
            String currKey = "";
            double currValue = .0;
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                temp_cnt += 1;
                if (temp_cnt == 1){
                    currKey = nextToken;
                }
                else if (temp_cnt == 2){
                    currValue = Double.parseDouble(nextToken);
                    top_5_revenue.add(new Tuple(currKey, currValue));
                    if (top_5_revenue.size() > 5){
                        top_5_revenue.remove();
                    }
                    temp_cnt = 0;
                }
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text,Text,Text,Text> {
        private PriorityQueue<WordCountMapper.Tuple> top_5_result = new PriorityQueue<WordCountMapper.Tuple>(new Comparator<WordCountMapper.Tuple>() {
            public int compare(WordCountMapper.Tuple o1, WordCountMapper.Tuple o2) {
                return o1.rev < o2.rev ? -1 : 1;
            }
        });
        public void reduce(Text key, Iterable<Text> taxiTextItr, Context context) throws IOException, InterruptedException {
            double max_rev = .0;
            String max_driverID = "";
            for (Text t : taxiTextItr) {
                String[] dummy = t.toString().split(",");
                String taxi_key = dummy[0];
                double taxi_revenue = Double.parseDouble(dummy[1]);
                top_5_result.add(new WordCountMapper.Tuple(taxi_key, taxi_revenue));
                if (top_5_result.size() > 5){
                    top_5_result.remove();
                }
//                if (Double.parseDouble(dummy[1]) > max_rev){
//                    max_rev = Double.parseDouble((dummy[1]));
//                    max_driverID = dummy[0];
//                }
            }

            while(top_5_result.size() > 0){
                WordCountMapper.Tuple result_top = top_5_result.poll();
                context.write(new Text(result_top.id), new Text(String.valueOf(result_top.rev)));
//                top_5_result.remove();
            }

        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
//        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
