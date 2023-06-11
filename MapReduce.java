import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.text.DecimalFormat;

public class MapReduce {

  public static class MaxProfitMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] line = value.toString().split(",");

      // don't emit the header file
      if (line[0].equals("Year")) return;

      String year = line[0], month = line[1];
      double amount = Double.parseDouble(line[2]);

      if (amount > 0) {
        word.set(year + "-" + month);
        context.write(word, new DoubleWritable(amount));
      }
    }
  }

  public static class MaxProfitReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private Text maxSumMonth = new Text();
    private DoubleWritable maxSum = new DoubleWritable(Double.MIN_VALUE);

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double sum = 0.0;

      for (DoubleWritable val : values) {
        sum += val.get();
      }

      if (sum > maxSum.get()) {
        maxSumMonth.set(key);
        DecimalFormat df = new DecimalFormat("0.00");
        sum = Double.valueOf(df.format(sum));
        maxSum.set(sum);
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      // round max sum to 2 decimal places
      context.write(maxSumMonth, maxSum);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "mapreduce");
    job.setJarByClass(MapReduce.class);
    job.setMapperClass(MaxProfitMapper.class);
    job.setCombinerClass(MaxProfitReducer.class);
    job.setReducerClass(MaxProfitReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
