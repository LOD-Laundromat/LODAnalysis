package reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetAllPredicatesReducer extends Reducer<Text, NullWritable, Text, LongWritable> {
  LongWritable oValue = new LongWritable();
  @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      long counter = 0;

      for (NullWritable none : values) {
        counter++;
      }

      oValue.set (counter);
      context.write (key, oValue);

    }
}
