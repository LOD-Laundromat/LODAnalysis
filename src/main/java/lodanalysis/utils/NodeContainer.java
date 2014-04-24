package lodanalysis.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NodeContainer {
	private static Pattern NS_PATTERN = Pattern.compile("<(.*)[#/].*>");
	private static Pattern DATA_TYPE_PATTERN = Pattern.compile(".*\"\\^\\^<(.*)>$");
	private static Pattern LANG_TAG_PATTERN = Pattern.compile(".*\"@(.*)\\s*$");
	
	public enum Position {SUB, PRED, OBJ};
	private Position position;
	public String stringRepresentation;
	//calculated stuff:
	
	public String ns = null;
	public String datatype = null;
	public Boolean isLiteral = null;
	public String langTag = null;
	public String langTagWithoutReg = null;;
	
	public NodeContainer(String stringRepresentation, Position position) {
		this.stringRepresentation = stringRepresentation;
		this.position = position;
		calcInfo();
	}
	
	
	/**
	 * do this once a-priori, as our counters often re-use info
	 */
	private void calcInfo() {
		this.ns = getNs(stringRepresentation);
		this.isLiteral = stringRepresentation.startsWith("\"");
		if (position == Position.OBJ && isLiteral) {
			//only for literals
			getDataType();
			getLangTagInfo();
		}
		
	}
	public static String getNs(String stringRepresentation) {
		String ns = null;
		if (stringRepresentation.startsWith("<")) {
			//this is a uri
			if (stringRepresentation.lastIndexOf('#') > 7 || stringRepresentation.lastIndexOf('/') > 7) {
				//ok, this has a namespace, and not something like http://google.com
				Matcher m = NS_PATTERN.matcher(stringRepresentation);
				if (m.find()) {
					ns = m.group(1);
				}
			} else {
				ns = stringRepresentation;
			}
		}
		return ns;
	}

	private void getDataType() {
		String type = null;
		if (stringRepresentation.startsWith("\"")) {
			//this is a literal
			Matcher m = DATA_TYPE_PATTERN.matcher(stringRepresentation);
			if (m.find()) {
				type = m.group(1);
			}
		}
		this.datatype = type;
	}
	
	private void getLangTagInfo() {
		String langTag = null;
		if (stringRepresentation.startsWith("\"")) {
			//this is a literal
			Matcher m = LANG_TAG_PATTERN.matcher(stringRepresentation);
			if (m.find()) {
				langTag = m.group(1);
			}
		}
		this.langTag = langTag;
		
		if (langTag != null && langTag.contains("-")) {
			this.langTagWithoutReg = langTag.substring(0, langTag.indexOf('-'));
		} else {
			this.langTagWithoutReg = langTag;
		}
	}
	
	
	
	
	public String toString() {
		return 
			"orig string: " + stringRepresentation + "\n" + 
			"ns: " + ns + "\n" +
			"datatype: " + datatype + "\n" + 
			"lang tag: " + langTag + "\n" +
			"lang tag (without reg): " + langTagWithoutReg + "\n" +
			"isLiteral: " + (isLiteral? "yes": "no") + "\n";
	}
	public static void main(String[] args) {
//		System.out.println(new NodeContainer("<http://google.com>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://google.co/df/fdm>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("<http://google.co/df#fdm>", Position.OBJ).toString());
//		System.out.println(new NodeContainer("\"That Seventies Show\"^^<http://www.w3.org/2001/XMLSchema#string>", Position.OBJ).toString());
		System.out.println(new NodeContainer("\"That Seventies Show\"", Position.OBJ).toString());
		System.out.println(new NodeContainer("\"That Seventies Show\"@en", Position.OBJ).toString());
		System.out.println(new NodeContainer("\"That Seventies Show\"@en-be", Position.OBJ).toString());
	}
}
