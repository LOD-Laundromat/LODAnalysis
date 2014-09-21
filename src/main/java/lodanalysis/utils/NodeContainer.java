package lodanalysis.utils;

import java.util.regex.Pattern;

import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

public class NodeContainer {
	@SuppressWarnings("unused")
	private static Pattern IGNORE_ALL_URI_ITERATORS = Pattern.compile(".*[#/]_\\d+>$");

	//TODO: make complete string (starts with)
	private static final String BNODE_SUBSTRING = "/.well-known/genid/";

	private final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	
	private static final String W3C_URI_PREFIX = "http://www.w3.org/";


	public enum Position {SUB, PRED, OBJ};
	private Position position;
	//calculated stuff:
	
	public PatriciaNode nsTicket = null;
	public PatriciaNode datatype = null;
	public boolean isLiteral = false;
	public boolean isUri = true; //default: assume the node is a uri
	public boolean isBnode = false;
	public PatriciaNode langTag = null;
//	public String langTagWithoutReg = null;
	public boolean ignoreIri = false;
	public PatriciaNode ticket;
	public int uriLength = -1;
	public int literalLength = -1;
	
	public boolean isRdf_type = false;
//	public boolean isRdfs_domain = false;
//	public boolean isRdfs_range = false;
//	public boolean isRdfs_subClassOf = false;
//	public boolean isRdfs_subPropertyOf = false;
	private Vault<String, PatriciaNode> vault;

	private int dataTypeLength = 0;
	private int langTagLength = 0;
	public NodeContainer(Vault<String, PatriciaNode> vault, String stringRepresentation, Position position) {
		this.position = position;
		this.vault = vault;
		calcInfo(stringRepresentation);
		this.ticket = vault.store(stringRepresentation);
	}

	/**
	 * do this once a-priori, as our counters often re-use info
	 * NOTE: we can afford to optimize these calculations (i.e. avoid regex), because we know how exactly we serialize the ntriples.
	 * This makes detection of things like lang tags and datatypes very easy and very fast.
	 */
	private void calcInfo(String stringRepresentation) {
		if (stringRepresentation.startsWith("\"")) {
			this.isLiteral = true;
			this.isUri = false;
		}
		
		if (isUri) {
			if (stringRepresentation.contains(BNODE_SUBSTRING)) {
				System.out.println(stringRepresentation);
				System.out.println("AH, we need to change string contains to starts-with for optimization");
				System.exit(1);
				//we rewrite each bnode to uri. check whether this is one of these uris
				this.isBnode = true;
				this.isUri = false;
			} else {
				uriLength = stringRepresentation.length();
				String ns = getNs(stringRepresentation);
				this.nsTicket = vault.store(ns);
				getSchemaInfo(stringRepresentation, ns);
			}
			
		}
		
		
		

		if (position == Position.OBJ && isLiteral) {
			
			//only for literals
			getDataType(stringRepresentation);
			getLangTagInfo(stringRepresentation);
			getLiteralLength(stringRepresentation);
			
		}

	}

	private void getLiteralLength(String stringRepresentation) {
		this.literalLength = stringRepresentation.length() - 2; //subtract the two quotes
		
		if (dataTypeLength > 0) {
			//subtract datatype length, plus the two ^^
			this.literalLength -= dataTypeLength - 2;
		} else if (langTagLength > 0) {
			//also subtract the @
			this.literalLength -= langTagLength - 1;
		}
		
	}

	private void getSchemaInfo(String stringRepresentation, String ns) {
		if (position == Position.PRED && ns.equals(W3C_URI_PREFIX)) {
			//ok, now do the expensive checks after pruning
			if (stringRepresentation.equals(RDF_TYPE)) {
				isRdf_type = true;
//			} else if (stringRepresentation.equals(RDFS_DOMAIN)) {
//				isRdfs_domain = true;
//			} else if (stringRepresentation.equals(RDFS_RANGE)) {
//				isRdfs_range = true;
//			} else if (stringRepresentation.equals(RDFS_SUBCLASSOF)) {
//				isRdfs_subClassOf = true;
				//ignore: we now use the void definition of a property, which is something which occurs in pred position
//			} else if (stringRepresentation.equals(RDFS_SUBPROPERTYOF)) {
//				isRdfs_subPropertyOf = true;
			}
		}
	}


	public static String getNs(String stringRepresentation) {
		int hashTagIndex = stringRepresentation.lastIndexOf('#');
		int slashIndex = stringRepresentation.lastIndexOf('/');
		if (hashTagIndex > 6 || slashIndex > 6) {
			//ok, this has a namespace, and not something like http://google.com
			return stringRepresentation.substring(0, Math.max(hashTagIndex, slashIndex));
		} else {
			return stringRepresentation; //initialize with ns as whole URI
		}
	}

	private void getDataType(String stringRepresentation) {
		if (stringRepresentation.contains("^^")) {
			//probably a datatype
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
					this.dataTypeLength = dataTypeBuilder.length();
					this.datatype = vault.store(dataTypeBuilder.toString());
				}

			}
		}
	}

	private void getLangTagInfo(String stringRepresentation) {
		this.langTag = null;

		if (stringRepresentation.contains("@")) {
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
					this.langTagLength = langTagBuilder.length();
					this.langTag = vault.store(langTagBuilder.toString());
				}
			}
		}
		//not using the lang tag without reg for now
//		if (this.langTag != null && this.langTag.contains("-")) {
//			this.langTagWithoutReg = this.langTag.substring(0, this.langTag.indexOf('-'));
//		} else {
//			this.langTagWithoutReg = this.langTag;
//		}
	}



	public String toString() {
		return
//			"ns: " + ns + "\n" +
			"datatype: " + datatype + "\n" +
			"lang tag: " + langTag + "\n" +
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
//		System.out.println(new NodeContainer("<http://www.w3.org/1999/02/22-rdf-syntax-ns#_111>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://www.w3.org/1999/02/22-rdf-syntax-ns#_>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"^^<http://www.w3.org/2001/XMLSchema#string>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"@en", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"@en-be", Position.OBJ).toString());
	}
}
