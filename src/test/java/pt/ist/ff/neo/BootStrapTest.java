package pt.ist.ff.neo;

import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import pt.ist.fenixframework.pstm.dml.FenixDomainModel;
import dml.DmlCompiler;
import dml.DomainClass;
import dml.DomainModel;
import dml.DomainRelation;

public class BootStrapTest {

    private DomainModel model;

    private BatchInserter db;

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

	    db = BatchInserters.inserter(graphDbLocation);

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
	    {
		BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
		Bootstrap.bootStrap(con, db, indexProvider, model);
		indexProvider.shutdown();
	    }

	    for (final DomainClass domainClass : model.getDomainClasses()) {
		BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
		ClassImporter.importClass(db, indexProvider, domainClass, con);
		indexProvider.shutdown();
	    }

	    for (final DomainRelation relation : model.getDomainRelations()) {
		BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(db);
		RelationImporter.importRelation(db, indexProvider, relation, con);
	    }

	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}

    }

    @After
    public void tear() {
	try {
	    db.shutdown();
	    con.close();
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }
}
