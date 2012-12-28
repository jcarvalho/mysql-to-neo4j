package pt.ist.ff.neo;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import pt.ist.fenixframework.pstm.dml.FenixDomainModel;
import dml.DmlCompiler;
import dml.DomainClass;
import dml.DomainModel;

public class BootStrapTest {

    private DomainModel model;

    private GraphDatabaseService db;

    @Before
    public void initDomainModel() {
	try {

	    URL dml = this.getClass().getResource("/domain_model.dml");
	    URL scheduler = this.getClass().getResource("/scheduler-plugin.dml");
	    List<URL> urls = new ArrayList<URL>();
	    urls.add(scheduler);
	    urls.add(dml);
	    model = DmlCompiler.getDomainModelForURLs(FenixDomainModel.class, urls);

	    db = new GraphDatabaseFactory().newEmbeddedDatabase("/Users/jpc/Desktop/ff");

	    // Delete everything!

	    Transaction tx = db.beginTx();
	    try {

		GlobalGraphOperations at = GlobalGraphOperations.at(db);

		for (Relationship relationship : at.getAllRelationships()) {
		    relationship.delete();
		}

		for (Node node : at.getAllNodes()) {
		    if (node.getId() != 0)
			node.delete();
		}
		tx.success();
	    } finally {
		tx.finish();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    @Test
    public void create() {

	Connection con = null;

	String url = "jdbc:mysql://localhost:3306/bennu";
	String user = "root";
	String password = "";

	try {
	    con = DriverManager.getConnection(url, user, password);

	    Bootstrap.bootStrap(con, db, model);

	    for (DomainClass domainClass : model.getDomainClasses()) {
		ClassImporter.importClass(db, domainClass, con);
	    }
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}

    }

    @After
    public void tear() {
	db.shutdown();
    }
}
