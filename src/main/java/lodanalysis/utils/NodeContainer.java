package lodanalysis.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.math.NumberUtils;

public class NodeContainer {
//	private static Pattern NS_PATTERN = Pattern.compile("<(.*)[#/].*>");
	@SuppressWarnings("unused")
	private static Pattern IGNORE_ALL_URI_ITERATORS = Pattern.compile(".*[#/]_\\d+>$");
	
	private static String IGNORE_RDF_URI_PREFIX = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#_";
	private static String BNODE_SUBSTRING = "/.well-known/genid/";


	public enum Position {SUB, PRED, OBJ};
	private Position position;
	public String stringRepresentation;
	//calculated stuff:

	public String ns = null;
	public String datatype = null;
	public Boolean isLiteral = null;
	public Boolean isUri = null;
	public Boolean isBnode = false;
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
	 */
	private void calcInfo() {
		this.isLiteral = stringRepresentation.startsWith("\"");
		this.isUri = stringRepresentation.startsWith("<");
		if (isUri && stringRepresentation.contains(BNODE_SUBSTRING)) {
			//we rewrite each bnode to uri. check whether this is one of these uris
			this.isBnode = true;
			this.isUri = false;
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
