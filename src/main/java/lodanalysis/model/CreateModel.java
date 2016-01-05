package lodanalysis.model;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.model.statements.Degree;
import lodanalysis.model.statements.DegreeIn;
import lodanalysis.model.statements.DegreeOut;
import lodanalysis.model.statements.LengthLiteral;
import lodanalysis.model.statements.LengthUri;
import lodanalysis.model.statements.LengthUriObj;
import lodanalysis.model.statements.LengthUriPred;
import lodanalysis.model.statements.LengthUriSub;
import lodanalysis.model.statements.NumAllLiterals;
import lodanalysis.model.statements.NumAllUris;
import lodanalysis.model.statements.NumBnodes;
import lodanalysis.model.statements.NumDistinctBnodes;
import lodanalysis.model.statements.NumDistinctClasses;
import lodanalysis.model.statements.NumDistinctDataTypes;
import lodanalysis.model.statements.NumDistinctDefinedClasses;
import lodanalysis.model.statements.NumDistinctDefinedProperties;
import lodanalysis.model.statements.NumDistinctEntities;
import lodanalysis.model.statements.NumDistinctLanguages;
import lodanalysis.model.statements.NumDistinctLiterals;
import lodanalysis.model.statements.NumDistinctObjects;
import lodanalysis.model.statements.NumDistinctProperties;
import lodanalysis.model.statements.NumDistinctSubjects;
import lodanalysis.model.statements.NumDistinctTriples;
import lodanalysis.model.statements.NumDistinctUris;
import lodanalysis.model.statements.NumObjUris;
import lodanalysis.model.statements.NumSubUris;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

public class CreateModel implements Runnable {
    public enum Namespace {
        LL("ll", "http://lodlaundromat.org/resource/"), LLO("llo", "http://lodlaundromat.org/ontology/"), LLM("llm",
                "http://lodlaundromat.org/metrics/ontology/"), VOID("void", "http://rdfs.org/ns/void#"), VOID_EXT("void-ext", "http://ldf.fi/void-ext#"), RDF(
                "rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"), RDFS("rdfs", "http://www.w3.org/2000/01/rdf-schema#"), PROV("prov",
                "http://www.w3.org/ns/prov#"), FOAF("foaf", "http://xmlns.com/foaf/0.1/"), DS("ds", "http://bio2rdf.org/bio2rdf.dataset_vocabulary:");

        private String prefix;
        private String url;

        Namespace(String prefix, String url) {
            this.prefix = prefix;
            this.url = url;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getUrl() {
            return url;
        }
    }

    public Model model;
    public Resource voidDoc;
    public File metricDir;
    private Property typeProperty;

    public CreateModel(File metricDir) throws IOException {
        this.metricDir = metricDir;

    }

    private void initModel() {
        model = ModelFactory.createDefaultModel();
        // set namespaces
        for (Namespace ns : Namespace.values()) {
            model.setNsPrefix(ns.getPrefix(), ns.getUrl());
        }
        typeProperty = model.createProperty(Namespace.RDF.getUrl(), "type");
        // init actual metrics resource
        voidDoc = model.createResource(Namespace.LL.getUrl() + metricDir.getName() + "/metrics");
        // link regular LL resource with metric resource
        model.createResource(Namespace.LL.getUrl() + metricDir.getName()).addProperty(model.createProperty(Namespace.LLM.getUrl(), "metrics"), voidDoc);

        // set proper types
        voidDoc.addProperty(typeProperty, model.createResource(Namespace.LLM.getUrl() + "Dataset"));
        voidDoc.addProperty(typeProperty, model.createResource(Namespace.PROV.getUrl() + "Entity"));
    }

    private void setProvenance() throws IOException {
        File sysInfo = new File(metricDir, ".sysinfo");
        if (!sysInfo.exists())
            return;
        // get git info from sysinfo file
        String gitRepo = null;
        String commitHash = null;
        LineIterator it = FileUtils.lineIterator(sysInfo);
        int count = 1;
        int gitRepoLine = 1;
        int commitHashLine = 3;
        while (it.hasNext()) {
            String line = it.next();
            if (count == gitRepoLine) {
                gitRepo = line;
            } else if (count == commitHashLine) {
                commitHash = line;
            }
            count++;
        }

        // add prov class to metric resource
        voidDoc.addProperty(model.createProperty(Namespace.PROV.getUrl(), "used"), model.createResource(Namespace.LL.getUrl() + metricDir.getName()));

        // init provenance plan (the resource referencing the github resource)
        Resource plan = model.createResource("http://lodlaundromat.org/metrics/resource/" + commitHash);
        plan.addProperty(typeProperty, model.createResource(Namespace.PROV.getUrl() + "Plan"));
        plan.addProperty(typeProperty, model.createResource(Namespace.PROV.getUrl() + "Entity"));
        plan.addProperty(model.createProperty(Namespace.LLM.getUrl() + "gitCommitId"), commitHash, XSDDatatype.XSDstring);
        plan.addProperty(model.createProperty(Namespace.LLM.getUrl() + "gitRepository"), model.createResource(gitRepo));
        ;

        // add metric calculation prov activity (referencing the prov plan)
        Resource calculationActivity = model.createResource(Namespace.LL.getUrl() + metricDir.getName() + "/metricCalculation");
        calculationActivity.addProperty(typeProperty, model.getResource(Namespace.PROV.getUrl() + "Activity"));
        calculationActivity.addProperty(model.createProperty(Namespace.PROV.getUrl() + "generated"), voidDoc);

        Resource qualifiedAssociationBnode = model.createResource();
        qualifiedAssociationBnode.addProperty(typeProperty, model.createResource(Namespace.PROV.getUrl() + "Association"));
        qualifiedAssociationBnode.addProperty(model.createProperty(Namespace.PROV.getUrl() + "hadPlan"), plan);

        calculationActivity.addProperty(model.createProperty(Namespace.PROV.getUrl() + "qualifiedAssociation"), qualifiedAssociationBnode);
    }

    private void addStructuralProps() throws IOException {
        CreateModelStatement[] descriptions = new CreateModelStatement[] { new NumDistinctTriples(this), new NumDistinctBnodes(this),
                new NumDistinctClasses(this), new NumDistinctEntities(this), new NumDistinctLiterals(this), new NumDistinctObjects(this),
                new NumDistinctProperties(this), new NumDistinctSubjects(this), new NumDistinctDefinedClasses(this), new NumDistinctDefinedProperties(this),
                new NumAllLiterals(this), new NumAllUris(this), new NumDistinctUris(this), new NumBnodes(this), new NumObjUris(this),
                new NumSubUris(this),
                new NumDistinctDataTypes(this),
                new NumDistinctLanguages(this),
                // new PartitionPropsVoid(this),
                // new PartitionTypesDs(this),
                // new PartitionTypesVoid(this),
                new Degree(this), new DegreeIn(this), new DegreeOut(this), new LengthUri(this), new LengthUriObj(this), new LengthUriPred(this),
                new LengthUriSub(this), new LengthLiteral(this), };
        for (CreateModelStatement description : descriptions) {
            description.createDescription();
        }
    }

    @Override
    public void run() {
        try {
            initModel();
            setProvenance();
            addStructuralProps();

            model.write(new FileOutputStream(new File(metricDir, Paths.DESCRIPTION_TTL)), "TURTLE");
            model.write(new FileOutputStream(new File(metricDir, Paths.DESCRIPTION_NT)), "N-TRIPLE");
            CreateModels.PROCESSED_COUNT++;
            CreateModels.printProgress(metricDir);
        } catch (Exception e) {
            CreateModels.PROCESSED_COUNT++;
            e.printStackTrace();
        }

    }

}
