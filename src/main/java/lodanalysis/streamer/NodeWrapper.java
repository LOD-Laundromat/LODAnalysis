package lodanalysis.streamer;

import java.util.regex.Pattern;

import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;
import org.apache.xerces.util.XMLChar ;
public class NodeWrapper {
	@SuppressWarnings("unused")
	private static Pattern IGNORE_ALL_URI_ITERATORS = Pattern.compile(".*[#/]_\\d+>$");

	public static final String BNODE_SUBSTRING = "http://lodlaundromat.org/.well-known/";

	private final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	
	private static final String W3C_URI_PREFIX = "http://www.w3.org/";
	private static final String RDF_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class";
	private static final String OWL_CLASS = "http://www.w3.org/2002/07/owl#Class";
	private static final String RDF_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	private static final String OWL_OBJECT_PROPERTY = "http://www.w3.org/2002/07/owl#ObjectProperty";
	private static final String OWL_DATATYPE_PROPERTY = "http://www.w3.org/2002/07/owl#DatatypeProperty";
	

	public enum Position {SUB, PRED, OBJ};
	private Position position;
	//calculated stuff:
	private Boolean hasW3cUriPrefix = null;
	public PatriciaNode nsTicket = null;
	public PatriciaNode datatype = null;
	public boolean isLiteral = false;
	public boolean isUri = true; //default: assume the node is a uri
	public boolean isBnode = false;
	public PatriciaNode langTag = null;
	public boolean ignoreIri = false;
	public PatriciaNode ticket;
	public int uriLength = -1;
	public int literalLength = -1;
	private String stringRepresentation;
	public boolean isRdf_type = false;
//	public boolean isRdfs_domain = false;
//	public boolean isRdfs_range = false;
//	public boolean isRdfs_subClassOf = false;
//	public boolean isRdfs_subPropertyOf = false;
	private Vault<String, PatriciaNode> vault;

	private int dataTypeLength = 0;
	private int langTagLength = 0;
	public NodeWrapper(Vault<String, PatriciaNode> vault, String stringRepresentation, Position position) {
		this.stringRepresentation = stringRepresentation;
		this.position = position;
		this.vault = vault;
		calcInfo();
		this.ticket = vault.store(stringRepresentation);
	}

	/**
	 * do this once a-priori, as our counters often re-use info
	 * NOTE: we can afford to optimize these calculations (i.e. avoid regex), because we know how exactly we serialize the ntriples.
	 * This makes detection of things like lang tags and datatypes very easy and very fast.
	 */
	private void calcInfo() {
		if (stringRepresentation.charAt(0) == '"') {
			this.isLiteral = true;
			this.isUri = false;
		}
		
		if (isUri) {
			if (stringRepresentation.startsWith(BNODE_SUBSTRING)) {
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
			getDataType();
			getLangTagInfo();
			getLiteralLength();
			
		}

	}

	private void getLiteralLength() {
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
		if (position == Position.PRED && stringRepresentation.equals(RDF_TYPE)) {
			isRdf_type = true;
		}
	}
	

//	public static String getNs(String uri) {
//		int hashTagIndex = uri.lastIndexOf('#');
//		int slashIndex = uri.lastIndexOf('/');
//		int colonIndex = uri.lastIndexOf(':');
//		if (hashTagIndex > 6 || slashIndex > 6 || colonIndex > 6) {
//			//ok, this has a namespace, and not something like http://google.com
//			return uri.substring(0, Math.max(Math.max(hashTagIndex, slashIndex), colonIndex));
//		} else {
//			return uri; //initialize with ns as whole URI
//		}
//	}
	
	//borrowed from jena
public static String getNs(String uri) {
        
        // XML Namespaces 1.0:
        // A qname name is NCName ':' NCName
        // NCName             ::=      NCNameStartChar NCNameChar*
        // NCNameChar         ::=      NameChar - ':'
        // NCNameStartChar    ::=      Letter | '_'
        // 
        // XML 1.0
        // NameStartChar      ::= ":" | [A-Z] | "_" | [a-z] | [#xC0-#xD6] |
        //                        [#xD8-#xF6] | [#xF8-#x2FF] |
        //                        [#x370-#x37D] | [#x37F-#x1FFF] |
        //                        [#x200C-#x200D] | [#x2070-#x218F] |
        //                        [#x2C00-#x2FEF] | [#x3001-#xD7FF] |
        //                        [#xF900-#xFDCF] | [#xFDF0-#xFFFD] | [#x10000-#xEFFFF]
        // NameChar           ::= NameStartChar | "-" | "." | [0-9] | #xB7 |
        //                        [#x0300-#x036F] | [#x203F-#x2040]
        // Name               ::= NameStartChar (NameChar)*
        
        char ch;
        int lg = uri.length();
        if (lg == 0)
            return "";
        int i = lg-1 ;
        for ( ; i >= 1 ; i--) {
            ch = uri.charAt(i);
            if (notNameChar(ch)) break;
        }
        
        int j = i + 1 ;

        if ( j >= lg )
            return uri ;
        
        // Check we haven't split up a %-encoding.
        if ( j >= 2 && uri.charAt(j-2) == '%' )
            j = j+1 ;
        if ( j >= 1 && uri.charAt(j-1) == '%' )
            j = j+2 ;
        
        // Have found the leftmost NCNameChar from the
        // end of the URI string.
        // Now scan forward for an NCNameStartChar
        // The split must start with NCNameStart.
        for (; j < lg; j++) {
            ch = uri.charAt(j);
//            if (XMLChar.isNCNameStart(ch))
//                break ;
            if (XMLChar.isNCNameStart(ch))
            {
                // "mailto:" is special.
                // Keep part after mailto: at least one charcater.
                // Do a quick test before calling .startsWith
                // OLD: if ( uri.charAt(j - 1) == ':' && uri.lastIndexOf(':', j - 2) == -1)
                if ( j == 7 && uri.startsWith("mailto:"))
                    continue; // split "mailto:me" as "mailto:m" and "e" !
                else
                    break;
            }
        }
//        return j;
        return uri.substring( 0, j );
    }

/**
answer true iff this is not a legal NCName character, ie, is
a possible split-point start.
*/
public static boolean notNameChar( char ch )
{ return !XMLChar.isNCName( ch ); }


	private void getDataType() {
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
	
	public boolean isDefinedClass() {
		return startsWithW3cUri() && (stringRepresentation.equals(RDF_CLASS) || stringRepresentation.equals(OWL_CLASS));
	}
	public boolean isDefinedProperty() {
		return startsWithW3cUri() && (stringRepresentation.equals(RDF_PROPERTY) || stringRepresentation.equals(OWL_DATATYPE_PROPERTY) || stringRepresentation.equals(OWL_OBJECT_PROPERTY));
	}

	private boolean startsWithW3cUri() {
		if (hasW3cUriPrefix == null) hasW3cUriPrefix = stringRepresentation.startsWith(W3C_URI_PREFIX);
		return hasW3cUriPrefix;
	}
	private void getLangTagInfo() {
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
	    System.out.println(getNs("test"));
	    System.out.println(getNs("http://google"));
	    System.out.println(getNs("http://google/"));
	    System.out.println(getNs("http://google/test"));
	    System.out.println(getNs("http://google/test?sdgds"));
	    System.out.println(getNs("http://google/test#sdgds"));
	    System.out.println(getNs("http://google/test:sdgds"));
	    
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
