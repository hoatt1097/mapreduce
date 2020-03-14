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

public class CountGitRepoCommit {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text idRepo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject data = new JsonParser().parse(value.toString()).getAsJsonObject();
            if(data.get("type") != null && data.get("type").getAsString().equals("PushEvent")) {
                JsonObject objectRepo = new JsonParser().parse(data.get("repo").toString()).getAsJsonObject();
                idRepo.set(objectRepo.get("id").toString());
                JsonObject objectPayload = new JsonParser().parse(data.get("payload").toString()).getAsJsonObject();
                int numberCommit = objectPayload.get("commits").getAsJsonArray().size();
                context.write(idRepo, new IntWritable(numberCommit));
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private int count = 0;;
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count = count + sum;
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            result.set(count);
            context.write(new Text("Total commit: "), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count git repo commit");

        job.setJarByClass(CountGitRepoCommit.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
// $HADOOP_HOME/bin/hadoop jar /home/thienhoa/CountGitRepoCommit/target/CountGitRepoCommit-1.0.jar CountGitRepoCommit /git-data /apps/repo-commit/output