package data;

import java.io.IOException;
/* ----------------------------- */
public class Triple extends Object {

  private static final String delim = "\\s";
  /* ----------------------------- */
  private String subject   = null;
  private String predicate = null;;
  private String object    = null;;
  /* ----------------------------- */
  private boolean isValidTriple (String tripleString) {
	  return true;
//    return tripleString.matches (
//        "^"                                +
//        "\\s?(\".*\"\\^\\^)?<.*>"          + // Subject URI
//        "\\s+(\".*\"\\^\\^)?<.*>"          + // Predicate URI
//        "\\s+((\".*\"\\^\\^)?<.*>|\".*\")" + // Object URIs or Literal
//        "\\s+\\."                          + // Dot
//        "$");
  }
  /* ----------------------------- */
  public Triple (String input) throws Exception {
    if (!isValidTriple(input)) {
      throw new Exception("Invalid Triple");
    }
    /* ----------------------------- */
    String[] terms = input.split(delim);
    /* ----------------------------- */
    subject   = terms[0];
    predicate = terms[1];
    object    = terms[2];
  }
  /* ----------------------------- */
  public String getSubject() {
    return subject;
  }
  /* ----------------------------- */
  public String getPredicate() {
    return predicate;
  }
  /* ----------------------------- */
  public String getObject() {
    return object;
  }
  /* ----------------------------- */
}
