package pt.ist.ff.neo;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import pt.ist.fenixframework.pstm.dml.FenixDomainModel;
import dml.DmlCompiler;
import dml.DomainClass;
import dml.DomainModel;

public class BootStrapTest {

    private DomainModel model;

    private GraphDatabaseService db;

    @Before
    public void initDomainModel() {

	InputStream stream = this.getClass().getResourceAsStream("/dmls.conf");
	try (Scanner scanner = new Scanner(stream)) {

	    List<URL> urls = new ArrayList<URL>();

	    while (scanner.hasNext()) {
		urls.add(new URL(scanner.nextLine()));
	    }

	    model = DmlCompiler.getDomainModelForURLs(FenixDomainModel.class, urls);

	    String graphDbLocation = System.getProperty("graphDBLocation", "/sandbox/ff");

	    db = new GraphDatabaseFactory().newEmbeddedDatabase(graphDbLocation);

	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    @Test
    public void create() {

	Connection con = null;

	String url = "jdbc:mysql://localhost:3306/dot";
	String user = "root";
	String password = "";

	try {
	    con = DriverManager.getConnection(url, user, password);

	    Bootstrap.bootStrap(con, db, model);

	    for (DomainClass domainClass : model.getDomainClasses()) {
		ClassImporter.importClass(db, domainClass, con);
	    }
	} catch (SQLException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}

    }

    @After
    public void tear() {
	db.shutdown();
    }
}
