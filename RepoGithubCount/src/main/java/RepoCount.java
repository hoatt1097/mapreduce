import java.io.IOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RepoCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text idRepo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject data = new JsonParser().parse(value.toString()).getAsJsonObject();
            if(data.get("repo") != null) {
                JsonObject dataRepo = new JsonParser().parse(data.get("repo").toString()).getAsJsonObject();
                idRepo.set(dataRepo.get("id").toString());
                context.write(idRepo, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private int count = 0;;
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            ++count;
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            result.set(count);
            context.write(new Text("Total repo: "), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count repo");
        job.setNumReduceTasks(1);

        job.setJarByClass(RepoCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
// $HADOOP_HOME/bin/hadoop jar /home/thienhoa/RepoGithubCount/target/RepoGithubCount-1.0.jar RepoCount /git-data /apps/repo-count/output