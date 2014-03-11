package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* ================================================ */
public class GetAllPredicateClassesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text oKey   = new Text();
  private IntWritable oValue = new IntWritable();
  /* ----------------------------------- */
  private static final String delim = "\\s";
  /* ----------------------------------- */
  private static boolean validTriple (String str) {
    return str.matches (
        "^"                                +
        "\\s?(\".*\"\\^\\^)?<.*>"          + // Subject URI
        "\\s+(\".*\"\\^\\^)?<.*>"          + // Predicate URI
        "\\s+((\".*\"\\^\\^)?<.*>|\".*\")" + // Object URIs or Literal
        "\\s+\\."                          + // Dot
        "$");
  }
  /* ----------------------------------- */
  @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      /*
       * Convert the triple to String, so that we can check it
       * more easily
       */
      String inputTriple = value.toString();
      /*
       * Drop all triples that are not in the format we expect.
       */
      if (! validTriple (inputTriple)) return;
      /*
       * Now we split the triples into [s, p, o].
       */
      String[] terms = value.toString().split(delim);
      /* ----------------------------------- */
      oKey.set (terms[1]);
      oValue.set (1);
      context.write (oKey, oValue);
      /* ----------------------------------- */
      oKey.set (terms[2]);
      oValue.set (2);
      context.write (oKey, oValue);
    }
}
