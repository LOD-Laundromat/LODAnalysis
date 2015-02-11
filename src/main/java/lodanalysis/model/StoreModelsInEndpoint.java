package lodanalysis.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lodanalysis.Entry;
import lodanalysis.Paths;
import lodanalysis.RuneableClass;
import lodanalysis.utils.Utils;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

public class StoreModelsInEndpoint  extends RuneableClass{
	private static String SPARQL_GET_EXISTING_METRICS = "SELECT DISTINCT ?doc WHERE {?doc <http://lodlaundromat.org/metrics/ontology/metrics> []}";
    private final int offset = 20000;
    private String sparqlEndpointUrl;
	private String graphUpdateUrl;
    private Set<String> alreadyDone = new HashSet<String>();
	private String metricsNamedGraph;
	public StoreModelsInEndpoint(Entry entry) throws IOException {
		super(entry);
		metricsNamedGraph =  entry.getMetricNamedGraph();
		File[] metricDirs = entry.getMetricsDir().listFiles();
		
		this.sparqlEndpointUrl = entry.getSparqlUrl();
		this.graphUpdateUrl = entry.getGraphUpdateUrl();

		if (this.sparqlEndpointUrl == null || this.graphUpdateUrl == null) {
			System.out.println("NOT storing metrics remotely. No SPARQL endpoint and no graph update URL passed as argument");
		} else {
			storeMetricsInEndpoint(metricDirs);
		}
	}
	
	
	private void sendMetrics(File fileToUpload) throws IOException {
		String urlToConnect = entry.getGraphUpdateUrl() + "?graph-uri=" + URLEncoder.encode(metricsNamedGraph, "UTF-8");
		String paramToSend = "res-file";
		String boundary = Long.toHexString(System.currentTimeMillis()); // Just generate some unique random value.

		URLConnection connection = new URL(urlToConnect).openConnection();
		connection.setDoOutput(true); // This sets request method to POST.
		connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
		PrintWriter writer = null;
		try {
		    writer = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));

		    writer.println("--" + boundary);
		    writer.println("Content-Disposition: form-data; name=\"graph-uri\"");
		    writer.println(metricsNamedGraph);
		    
		    writer.println(paramToSend);

		    writer.println("--" + boundary);
		    writer.println("Content-Disposition: form-data; name=\"res-file\"; filename=\"" + fileToUpload.getName() + "\"");
		    writer.println("Content-Type: application/octet-stream");
		    writer.println();
		    BufferedReader reader = null;
		    try {
		        reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileToUpload), "UTF-8"));
		        for (String line; (line = reader.readLine()) != null;) {
		            writer.println(line);
		        }
		    } finally {
		        if (reader != null) try { reader.close(); } catch (IOException logOrIgnore) {}
		    }

		    writer.println("--" + boundary + "--");
		} finally {
		    if (writer != null) writer.close();
		}

		// Connection is lazily executed whenever you request any status.
		int responseCode = ((HttpURLConnection) connection).getResponseCode();
		if (responseCode >= 300) {
			System.err.println("Failed to store ntriple for dataset " + fileToUpload.getAbsolutePath() +". " + responseCode);
		}
	}
	
	private void storeMetricsInEndpoint(File[] metricDirs) throws IOException {
		boolean force = entry.forceExec();
		if (!force) {
			//in this case, skip the ones we already stored
		    while(getExistingMetricDatasets());
			System.out.println("" + alreadyDone.size() + " already stored metrics");
		}
		int totalCount = metricDirs.length;
		int processed = 0;
		for (File metricDir: metricDirs) {
			Utils.printProgress("storing description in endpoint (" + metricsNamedGraph + ")", totalCount, processed);
			processed++;
			if (force || !alreadyDone.contains(metricDir.getName())) {
				File descrFile = new File(metricDir, Paths.DESCRIPTION_NT);
				if (!descrFile.exists()) {
					System.err.println(descrFile.getAbsolutePath() + " does not exist. Skip sending");
					continue;
				}
				sendMetrics(descrFile);
			}
		}
		
	}
	private boolean getExistingMetricDatasets() throws ClientProtocolException, IOException {
		HttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = new HttpPost(sparqlEndpointUrl);
		httppost.addHeader("Accept", "text/csv");
		List<NameValuePair> params = new ArrayList<NameValuePair>(2);
		String query = SPARQL_GET_EXISTING_METRICS + " LIMIT " + offset + " OFFSET " + alreadyDone.size();
		params.add(new BasicNameValuePair("query", query));
		params.add(new BasicNameValuePair("default-graph-uri", metricsNamedGraph));
		httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
		HttpResponse response = httpclient.execute(httppost);
		if (response.getStatusLine().getStatusCode() >= 300) {
			throw new IOException("Failed to retrieve the existing list of metrics. " + response.getStatusLine().toString());
		}
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
		String line;
		boolean firstLine = true;
		boolean somethingFound = false;
		while ((line = reader.readLine()) != null) {
		    line = line.trim();
		    if (firstLine) {
        		firstLine = false;//skip first, as this one contains the var name
        		continue;
		    }
		    
		    if (line.length() > 0) {
			somethingFound = true;
			alreadyDone.add(line.substring(line.lastIndexOf("/") + 1, line.length() - 1));//extract md5, and remove final quote of string
		    }
		}
		if (alreadyDone.size() == 0) {
		    System.err.println("No existing metrics found. Is this correct??");
		} else {
		    System.out.println("Example of fetched existing metric URIs: " + alreadyDone.iterator().next());
		}
		return somethingFound;
	}
	
	
}


