/**
 * Created by zhufangze on 2017/6/1.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MrJoinMapper1
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    private String UNKNOWN = "unknown";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter("user_mapper1", "TOTAL").increment(1L);

        try {
            String SITEMAP_TYPE = context.getConfiguration().get("user.param.sitemap.type", UNKNOWN);

            if (SITEMAP_TYPE.equals(UNKNOWN)) {
                context.getCounter("user_mapper1", "INVALID_PARAM").increment(1L);
                return;
            }

            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length != 8 || !(parts[7].trim().equals(SITEMAP_TYPE)) ) {
                context.getCounter("user_mapper1", "INVALID_LINE").increment(1L);
                return;
            }

            String url = parts[3].trim();
            k.set(url);
            context.write(k, v);
        }
        catch(Exception e) {
            context.getCounter("user_mapper1", "EXCEPTION").increment(1L);
        }
    }
}

