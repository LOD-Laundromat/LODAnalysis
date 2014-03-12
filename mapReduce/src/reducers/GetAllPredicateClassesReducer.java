package reducers;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class GetAllPredicateClassesReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

  private final NullWritable nothing = NullWritable.get();
  int firstVal;
  /* ----------------------------------------- */
  @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      /*
       * Removing duplicates in the input
       */
      Iterator<IntWritable> inputItr = values.iterator();
      if (inputItr.hasNext()) {
        firstVal = inputItr.next().get();
      }
      while (inputItr.hasNext()) {
        if (inputItr.next().get() != firstVal) {
          context.write (key, nothing);
          break;
        }
      }
    }
}
