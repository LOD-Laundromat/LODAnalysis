package lodanalysis;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Entry {
//	List<String> possibleAnalysis;
	CommandLine line;

	public Entry(String[] args)  {
//		Arrays.asList(new String[]{"authority"});
		parseArgs(args);
	}
	
	public File[] getDatasetDirs() {
		File datasetParentDir = new File(line.getOptionValue("path"));
		return datasetParentDir.listFiles();
	}
	
	public File getDatasetParentDir() {
		return new File(line.getOptionValue("path"));
	}

	private void parseArgs(String[] args) {
		Options options = getOptions();
		CommandLineParser parser = new GnuParser();
		HelpFormatter help = new HelpFormatter();
		try {
			line = parser.parse(getOptions(), args);
			validateParameters();
			run();
		} catch (ParseException e) {
			String jarName = new java.io.File(Entry.class.getProtectionDomain()
					  .getCodeSource()
					  .getLocation()
					  .getPath())
					.getName();
			String header = "Java entry class to run our LOD cloud experiments. Usage: " + jarName + " [[OPTION]]... [[com.package.ClassToRun]]";
			if (e.getMessage().length() > 0) System.err.println("Wrong parameters: " + e.getMessage() + "\n");
			help.printHelp(header, options);
			System.exit(1);
		} catch (ClassNotFoundException e) {
			System.err.println("Could not initialize class " + e.getMessage());
			System.exit(1);
		} catch (ReflectiveOperationException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void run() throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		for (Object arg: line.getArgList()) {
			String argString = (String)arg;
			Class myClass = Class.forName(argString);
			Class[] types = {this.getClass()};
			Constructor constructor = myClass.getConstructor(types);

			Object[] parameters = {this};
			constructor.newInstance(parameters);
		}
	}

	private void validateParameters() throws ParseException{
		if (line.hasOption("help")) throw new ParseException("");
		if (line.getArgList().size() == 0) throw new ParseException("You forgot to tell me what class(es) you want to run!");
		if (!line.hasOption("path")) throw new ParseException("Please specify the path where we can find the dataset directories");
		
		if (!new File(line.getOptionValue("path")).exists()) throw new ParseException("The datasets path you specified does not exist");
		if (!new File(line.getOptionValue("path")).isDirectory()) throw new ParseException("The datasets path you specified is not a directory");
		
	}

	public boolean isVerbose() {
		return line.hasOption("verbose");
	}

	private Options getOptions() {
		Options options = new Options();
		Option verbose = new Option("verbose", "be extra verbose");
		@SuppressWarnings("static-access")
		 Option path = OptionBuilder
		 .withArgName("path")
		 .hasArg()
		 .withDescription("Path containing all the dataset directories")
		 .create("path");
		Option help = new Option("help", "print this message");

		options.addOption(help);
		options.addOption(verbose);
		 options.addOption(path);
		return options;
	}

	public static void main(String[] args)  {
		new Entry(args);
	}
}
