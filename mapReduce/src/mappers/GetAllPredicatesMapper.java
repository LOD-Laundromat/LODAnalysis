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
  private final String delim = "\\s?(<|\")";
  /* ----------------------------------- */
  private final String rdfOnly   = "http://www.w3.org/.*rdf-syntax-ns#";
  private final String xmlSchema = "http://www.w3.org/.*/rdf-schema#";
  private final String rdfSchema = "http://www.w3.org/.*/XMLSchema#";
  private final String owlSchema = "http://www.w3.org/.*/07/owl#";
  /* ----------------------------------- */
  private boolean validTriple (String str) {
    return str.matches ("^"                       +
                        "\\s?(\".*\"\\^\\^)?<.*>" +
                        "\\s+(\".*\"\\^\\^)?<.*>" +
                        "\\s+(\".*\"\\^\\^)?<.*>" +
                        "\\s+\\."                 +
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
