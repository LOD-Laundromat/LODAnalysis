package mappers;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* ================================================ */
public class GetAllPredicatesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private NullWritable nothing = NullWritable.get();
  private Text oKey = new Text();
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
      oKey.set (terms[1]);   // We are only interested in predicates
      context.write (oKey, nothing);
    }
}
