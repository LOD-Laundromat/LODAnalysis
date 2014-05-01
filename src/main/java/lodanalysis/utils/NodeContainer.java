package lodanalysis.utils;

import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;

public class NodeContainer {
	@SuppressWarnings("unused")
	private static Pattern IGNORE_ALL_URI_ITERATORS = Pattern.compile(".*[#/]_\\d+>$");

	private static final String IGNORE_RDF_URI_PREFIX = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#_";
        private static final String BNODE_SUBSTRING = "/.well-known/genid/";
        private static final String RDFS_URI = "http://www.w3.org/2000/01/rdf-schema";
        private static final String RDF_URI  = "http://www.w3.org/1999/02/22-rdf-syntax-ns";
        private static final String XML_URI  = "http://www.w3.org/2001/XMLSchema";
        private static final String OWL_URI  = "http://www.w3.org/2002/07/owl";


	public enum Position {SUB, PRED, OBJ};
	private Position position;
        private String schemaURI = null;
	public String stringRepresentation;
	//calculated stuff:

	public String ns = null;
	public String datatype = null;
	public Boolean isLiteral = null;
	public Boolean isUri = null;
	public Boolean isBnode = false;
	public boolean isSchema = false;
	public String langTag = null;
	public String langTagWithoutReg = null;
	public boolean ignoreIri = false;

	public NodeContainer(String stringRepresentation, Position position) {
		this.stringRepresentation = stringRepresentation;
		this.position = position;
		calcInfo();
	}

	/**
	 * do this once a-priori, as our counters often re-use info
	 * NOTE: we can afford to optimize these calculations (i.e. avoid regex), because we know how exactly we serialize the ntriples.
	 * This makes detection of things like lang tags and datatypes very easy and very fast.
	 */
	private void calcInfo() {
		this.isLiteral = stringRepresentation.startsWith("\"");
		this.isUri = stringRepresentation.startsWith("<");
		if (isUri && stringRepresentation.contains(BNODE_SUBSTRING)) {
			//we rewrite each bnode to uri. check whether this is one of these uris
			this.isBnode = true;
			this.isUri = false;
		}
		if (isSchemaNode(stringRepresentation)) {
			isSchema = true;
		}
		calcIgnoreUri();
		if (!ignoreIri && this.isUri) {
			this.ns = getNs(stringRepresentation);
		}

		if (position == Position.OBJ && isLiteral) {
			//only for literals
			getDataType();
			getLangTagInfo();
		}

	}

	private void calcIgnoreUri() {
		//note: ignoreIri's default val is false
		if (stringRepresentation.startsWith(IGNORE_RDF_URI_PREFIX)) {
			String postFix = stringRepresentation.substring(IGNORE_RDF_URI_PREFIX.length(), stringRepresentation.length()-1);
			this.ignoreIri = NumberUtils.isDigits(postFix);
		}
	}

        /**
         * Checks to see if current node belongs to any of RDF, RDFS, OWL, or XML
         * vocabularies or not?
         */
	private boolean isSchemaNode(String stringRepresentation) {
		int startIdx = stringRepresentation.indexOf ('<');
		int hashSignIdx = stringRepresentation.lastIndexOf('#');
		if (startIdx < hashSignIdx && startIdx >= 0 && hashSignIdx > 0) {
			String uri = stringRepresentation.substring (startIdx + 1, hashSignIdx);
			if (uri.equals (RDFS_URI) ||
					uri.equals (OWL_URI)  ||
					uri.equals (RDF_URI)  ||
					uri.equals (XML_URI)) {
				int endCharIdx = stringRepresentation.lastIndexOf (">");
				if (endCharIdx > startIdx + 1) {
					schemaURI = stringRepresentation.substring (startIdx + 1, endCharIdx);
					return true;
				}
					}
		}
		return false;
	}
        /**
         * If this is a schema node, then this routine can be used to get the
         * vocabulary URI that this schema belongs to.
         */
        public String getSchemaURI() {
          return schemaURI;
        }

	public static String getNs(String stringRepresentation) {
		String ns = null;
		if (stringRepresentation.charAt(0) == '<') {
			//this is a uri
			ns = stringRepresentation.substring(1, stringRepresentation.length() - 1); //initialize with ns as whole URI
			
			int hashTagIndex = ns.lastIndexOf('#');
			int slashIndex = ns.lastIndexOf('/');
			if (hashTagIndex > 6 || slashIndex > 6) {
				//ok, this has a namespace, and not something like http://google.com
				int nsLength = Math.max(hashTagIndex, slashIndex);
				ns = ns.substring(0, nsLength);
			}
		}
		return ns;
	}

	private void getDataType() {
		if (stringRepresentation.startsWith("\"") && stringRepresentation.contains("^^")) {
			int closingQuote = stringRepresentation.lastIndexOf("\"");

			if (
					stringRepresentation.length() <= closingQuote + 2
					|| stringRepresentation.charAt(closingQuote+1) != '^'
					|| stringRepresentation.charAt(closingQuote+2) != '^'
					|| stringRepresentation.charAt(closingQuote+3) != '<') {
				//ah, no lang tag after all!! either nothing comes after the quote, or something else than an '^^' follows
			} else {
				StringBuilder dataTypeBuilder = new StringBuilder();
				for(int i = closingQuote + 4; i < stringRepresentation.length(); i++) {
				   char c = stringRepresentation.charAt(i);
				   if (c == '>') break;
				   dataTypeBuilder.append(c);
				}
				if (dataTypeBuilder.length() > 0) {
					this.datatype = dataTypeBuilder.toString();
				}

			}
		}
	}

	private void getLangTagInfo() {
		this.langTag = null;

		if (stringRepresentation.startsWith("\"") && stringRepresentation.contains("@")) {
			//this is probably a literal

			int closingQuote = stringRepresentation.lastIndexOf("\"");
			if (stringRepresentation.length() == closingQuote + 1 || stringRepresentation.charAt(closingQuote+1) != '@') {
				//ah, no lang tag after all!! either nothing comes after the quote, or something else than an '@' follows
			} else {
				StringBuilder langTagBuilder = new StringBuilder();
				for(int i = closingQuote + 2; i < stringRepresentation.length(); i++) {
				   char c = stringRepresentation.charAt(i);
				   if (c == ' ') break;

				   langTagBuilder.append(c);
				}
				if (langTagBuilder.length() > 0) {
					this.langTag = langTagBuilder.toString();
				}
			}
		}
		if (this.langTag != null && this.langTag.contains("-")) {
			this.langTagWithoutReg = this.langTag.substring(0, this.langTag.indexOf('-'));
		} else {
			this.langTagWithoutReg = this.langTag;
		}
	}



	public String toString() {
		return
			"orig string: " + stringRepresentation + "\n" +
			"ns: " + ns + "\n" +
			"datatype: " + datatype + "\n" +
			"lang tag: " + langTag + "\n" +
			"lang tag (without reg): " + langTagWithoutReg + "\n" +
			"isLiteral: " + (isLiteral? "yes": "no") + "\n" +
			"isUri: " + (isUri? "yes": "no") + "\n" +
			"ignore: " + (ignoreIri? "yes": "no") + "\n";
	}
	public static void main(String[] args) {
//		System.out.println(new NodeContainer("<http://google.com_1>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://google.co/df/fdm_1111>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://google.co/df#fdm_>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://google.co/df#_12332>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://google.co/df/_12332>", Position.OBJ).toString());
		System.out.println(new NodeContainer("<http://www.w3.org/1999/02/22-rdf-syntax-ns#_111>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://www.w3.org/1999/02/22-rdf-syntax-ns#_>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"^^<http://www.w3.org/2001/XMLSchema#string>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"@en", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"@en-be", Position.OBJ).toString());
	}
}
