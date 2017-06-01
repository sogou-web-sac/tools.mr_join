/**
 * Created by zhufangze on 2017/6/1.
 * @param: user.input.path1
 * @param: user.input.path2
 * @param: user.param.sitemap.type
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MrJoin extends Configured implements Tool {

    public int run(String[] arg0) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, conf.get("mapred.job.name"));

        job.setJarByClass(MrJoin.class);
        job.setReducerClass(MrJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String input_path1 = conf.get("user.input.path1");
        String input_path2 = conf.get("user.input.path2");
        MultipleInputs.addInputPath(job, new Path(input_path1), TextInputFormat.class, MrJoinMapper1.class);
        MultipleInputs.addInputPath(job, new Path(input_path2), TextInputFormat.class, MrJoinMapper2.class);

        if (job.waitForCompletion(true) && job.isSuccessful()) {
            return 0;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new MrJoin(), args);
        System.exit(res);
    }
}
