package lodanalysis.backwardChainer.data;

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
  public Triple (String input) {
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
    subject   = terms[0];
    predicate = terms[1];
    object    = terms[2];
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
