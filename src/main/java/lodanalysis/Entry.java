package lodanalysis;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Entry {
	private static Properties DEFAULTS = new Properties();
	private enum OptionKeys {help, threads, dataset, datasets, nostrict, verbose, metrics, force};
	private Map<String, String> args = new HashMap<String, String>();
	private boolean strict = true;
//	private CommandLine line;
	private Set<File> datasetDirs = new HashSet<File>();
	private File metricsDir;
	private List classesToExec;
	public Entry(String[] args) throws IOException  {
		DEFAULTS.load(getClass().getClassLoader().getResourceAsStream("defaults.properties"));
		parseArgs(args);
	}

	public Set<File> getDatasetDirs() {
		return datasetDirs;
	}
	
	public boolean strict() {
		return strict;
	}
	public int getNumThreads() {
		return Integer.parseInt(args.get(OptionKeys.threads.toString()));
	}

	public File getDatasetParentDir() {
		File parentDir = null;
		if (args.containsKey(OptionKeys.datasets.toString())) {
			parentDir = new File(args.get(OptionKeys.datasets.toString()));
		} else {
			for (File datasetDir: datasetDirs) {
				parentDir = datasetDir.getParentFile();
				break;
			}
		}
		return parentDir;
	}
	public File getMetricsDir() {
		return metricsDir;
	}
	private void processParameters() {
		if (args.containsKey(OptionKeys.datasets.toString()))  {
			for (File dataset: new File(args.get(OptionKeys.datasets.toString())).listFiles()) {
				if (dataset.isDirectory()) datasetDirs.add(dataset);
			}
		}
		metricsDir = new File(args.get(OptionKeys.metrics.toString()));
		if (args.containsKey(OptionKeys.dataset.toString())) datasetDirs.add(new File(args.get(OptionKeys.dataset.toString())));
		strict = !args.containsKey(OptionKeys.nostrict.toString());
	}
	private void mergeDefaults(CommandLine commandLine) {
		for (Object key: DEFAULTS.keySet()) {
			String keyString = (String)key;
			String val = DEFAULTS.getProperty(keyString).trim();
			if (val != null && val.length() > 0) {
				if (val.equals("true")) {
					args.put(keyString.trim(), null);
				} else if (val.equals("false")) {
					//do not add to args!
				} else {
					args.put(keyString.trim(), val);
				}
			}
		}
		for (Option option: commandLine.getOptions()) {
			args.put(option.getArgName(), option.getValue());//getvalue sets null if not exists
		}
	}
	private void parseArgs(String[] argsArray) {
		Options options = getOptions();
		CommandLineParser parser = new GnuParser();
		HelpFormatter help = new HelpFormatter();
		try {
			CommandLine cmdLine = parser.parse(getOptions(), argsArray);
			classesToExec = cmdLine.getArgList();
			mergeDefaults(cmdLine);
			validateParameters();
			processParameters();
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
		for (Object arg: classesToExec) {
			Date startTime = new Date();
			System.out.println("Started: " + startTime.toString());
			String argString = (String)arg;
			Class myClass = Class.forName(argString);
			Class[] types = {this.getClass()};
			Constructor constructor = myClass.getConstructor(types);

			Object[] parameters = {this};
			constructor.newInstance(parameters);
			Date endTime = new Date();
			long diff = endTime.getTime() - startTime.getTime();
			long diffSeconds = diff / 1000 % 60;  
			long diffMinutes = diff / (60 * 1000) % 60; 
			System.out.println("Finished: " + endTime.toString() + " (duration: " + diffMinutes + " min, " + diffSeconds + " seconds)");
		}
	}

	private void validateParameters() throws ParseException{
		if (args.containsKey(OptionKeys.help.toString())) throw new ParseException("");
		if (classesToExec == null || classesToExec.size() == 0) throw new ParseException("You forgot to tell me what class(es) you want to run!");
		if (!args.containsKey((OptionKeys.datasets.toString())) && !args.containsKey(OptionKeys.dataset.toString())) throw new ParseException("Please specify the path where we can find the dataset directories");
		if (!args.containsKey(OptionKeys.metrics.toString())) {
			throw new ParseException("No metrics directory specified");
		} else {
			if (!new File(args.get(OptionKeys.metrics.toString())).exists()) {
				throw new ParseException("Metrics directory " + args.get(OptionKeys.metrics.toString()) + " does not exist");
			}
		}
		if (args.containsKey(OptionKeys.datasets.toString())) {
			if (!new File(args.get(OptionKeys.datasets.toString())).exists()) throw new ParseException("The datasets path you specified does not exist: " + args.get(OptionKeys.datasets.toString()));
			if (!new File(args.get(OptionKeys.datasets.toString())).isDirectory()) throw new ParseException("The datasets path you specified is not a directory" + args.get(OptionKeys.datasets.toString()));
		}
		if (args.containsKey(OptionKeys.dataset.toString())) {
			if (!new File(args.get(OptionKeys.dataset.toString())).exists()) throw new ParseException("The dataset you specified does not exist: " + args.get(OptionKeys.dataset.toString()));
			if (!new File(args.get(OptionKeys.dataset.toString())).isDirectory()) throw new ParseException("The dataset you specified is not a directory" + args.get(OptionKeys.dataset.toString()));
		}

	}

	public boolean isVerbose() {
		return args.containsKey(OptionKeys.verbose.toString());
	}
	public boolean forceExec() {
		return args.containsKey(OptionKeys.force.toString());
	}
	
	private String getOptionTextWithDefault(OptionKeys key, String msg) {
		if (DEFAULTS.contains(key)) {
			msg += " (default: ";
			String val = DEFAULTS.getProperty(key.toString()).trim();
			if (val.length() == 0) {
				msg += "null";
			} else {
				msg += val;
			}
			msg += ")";
		}
		return msg;
	}
	
	@SuppressWarnings("static-access")
	private Options getOptions() {
		Options options = new Options();
		
		Option verbose = new Option(OptionKeys.verbose.toString(), getOptionTextWithDefault(OptionKeys.verbose, "be extra verbose"));
		Option noStrict = new Option(OptionKeys.nostrict.toString(), getOptionTextWithDefault(OptionKeys.nostrict, "disable some checking, such as whether certain files exist. useful for debugging"));
		Option force = new Option(OptionKeys.force.toString(), getOptionTextWithDefault(OptionKeys.force, "force execution (i.e. ignore delta id)"));
		Option datasets = OptionBuilder.withArgName(OptionKeys.datasets.toString()).hasArg().withDescription(getOptionTextWithDefault(OptionKeys.datasets, "Path containing all the dataset directories")).create(OptionKeys.datasets.toString());
		Option metrics = OptionBuilder.withArgName(OptionKeys.metrics.toString()).hasArg().withDescription(getOptionTextWithDefault(OptionKeys.metrics, "Directory to write metrics")).create(OptionKeys.metrics.toString());
		Option threads = OptionBuilder.withArgName(OptionKeys.threads.toString()).hasArg().withDescription(getOptionTextWithDefault(OptionKeys.threads, "Number of threats to use")).create(OptionKeys.threads.toString());
		Option dataset = OptionBuilder.withArgName(OptionKeys.dataset.toString()).hasArg().withDescription(getOptionTextWithDefault(OptionKeys.dataset, "Dataset directory. Useful for debugging, when you only want to analyze 1 dataset")).create(OptionKeys.dataset.toString());
		Option help = new Option(OptionKeys.help.toString(), "print this message");
		
		
		options.addOption(help);
		options.addOption(verbose);
		options.addOption(force);
		options.addOption(datasets);
		options.addOption(metrics);
		options.addOption(dataset);
		options.addOption(threads);
		options.addOption(noStrict);
		return options;
	}

	public static void main(String[] args) throws IOException  {
		new Entry(args);
	}
}
