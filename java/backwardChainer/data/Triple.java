package data;

import java.lang.Exception;
/* ----------------------------- */
public class Triple extends Object {

  private static final String delim = "\\s";
  /* ----------------------------- */
  private String subject   = null;
  private String predicate = null;;
  private String object    = null;;
  /* ----------------------------- */
  boolean validity = true;
  /* ----------------------------- */
  private boolean isValidTriple (String tripleString) {
    return tripleString.matches (
        "^"                                +
        "\\s?(\".*\"\\^\\^)?<.*>"          + // Subject URI
        "\\s+(\".*\"\\^\\^)?<.*>"          + // Predicate URI
        "\\s+((\".*\"\\^\\^)?<.*>|\".*\")" + // Object URIs or Literal
        "\\s+\\."                          + // Dot
        "$");
  }
  /* ----------------------------- */
  public Triple (String input) throws Exception {
  /*
   * For the performance reasons, I'm ganna skip this step,
   * Anyways, Wouter is going to check the validity of triples.
   */
  //  if (!isValidTriple(input)) {
  //    validity = false;
  //    return;
  //  }
    /* ----------------------------- */
    String[] terms = input.split(delim);
    /* ----------------------------- */
    if (terms.length > 3) {
      subject   = terms[0];
      predicate = terms[1];
      object    = terms[2];
    } else {
      throw new Exception("invalid Triple");
    }
  }
  /* ----------------------------- */
  public boolean isValid() {
    return validity;
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
  @Override
  public String toString() {
   return subject + " " + predicate + " " + object + " .";
  }
}
