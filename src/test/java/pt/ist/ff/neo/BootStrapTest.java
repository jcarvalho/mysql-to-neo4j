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
import org.neo4j.unsafe.batchinsert.BatchInserters;

import pt.ist.fenixframework.pstm.dml.FenixDomainModel;
import dml.DmlCompiler;
import dml.DomainClass;
import dml.DomainModel;
import dml.DomainRelation;

public class BootStrapTest {

    private DomainModel model;

    private GraphDatabaseService db;

    private Connection con = null;

    @Before
    public void initDomainModel() {

	try (InputStream stream = this.getClass().getResourceAsStream("/dmls.conf"); Scanner scanner = new Scanner(stream)) {

	    List<URL> urls = new ArrayList<URL>();

	    while (scanner.hasNext()) {
		urls.add(new URL(scanner.nextLine()));
	    }

	    model = DmlCompiler.getDomainModelForURLs(FenixDomainModel.class, urls);

	    String graphDbLocation = System.getProperty("graphDBLocation", "/sandbox/ff");

	    db = BatchInserters.batchDatabase(graphDbLocation);

	    String url = "jdbc:mysql://localhost:3306/dot";
	    String user = "root";
	    String password = "";
	    con = DriverManager.getConnection(url, user, password);

	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    @Test
    public void create() {

	try {
	    Bootstrap.bootStrap(con, db, model);

	    for (final DomainClass domainClass : model.getDomainClasses()) {
		ClassImporter.importClass(db, domainClass, con);
	    }

	    for (final DomainRelation relation : model.getDomainRelations()) {
		RelationImporter.importRelation(db, relation, con);
	    }

	} catch (SQLException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}

    }

    @After
    public void tear() throws SQLException {
	db.shutdown();
	con.close();
    }
}
