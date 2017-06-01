/**
 * Created by zhufangze on 2017/6/1.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MrJoinMapper2
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(2);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter("user_mapper2", "TOTAL").increment(1L);

        try {
            String parts[] = value.toString().split("\t");
            if (parts.length != 2) {
                context.getCounter("user_mapper2", "INVALID_LINE").increment(1L);
            }
            String url = parts[0].trim();
            k.set(url);
            context.write(k, v);
        }
        catch(Exception e) {
            context.getCounter("user_mapper2", "EXCEPTION").increment(1L);
        }
    }
}
