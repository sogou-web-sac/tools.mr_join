/**
 * Created by zhufangze on 2017/6/1.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MrJoinReducer
        extends Reducer<Text, IntWritable, Text, Text> {

    Text k = new Text("ROOT_SITEMAP");
    Text v = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        context.getCounter("user_reducer", "TOTAL").increment(1L);

        try {
            boolean is_from_mapper1 = false;
            boolean is_from_mapper2 = false;

            for (IntWritable i : values) {
                int from_where = Integer.parseInt(i.toString());
                if (from_where == 1) {
                    is_from_mapper1 = true;
                }
                if (from_where == 2) {
                    is_from_mapper2 = true;
                }
            }

            if (is_from_mapper1 && is_from_mapper2) {
                String url = key.toString();
                v.set(url);
                context.write(k, v); // ROOT_SITEMAP\turl
                context.getCounter("user_reducer", "OUTPUT").increment(1L);
            }
        }
        catch (Exception e) {
            context.getCounter("user_reducer", "Exception").increment(1L);
        }
    }
}
