package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import dml.DomainClass;
import dml.Slot;

public class ClassImporter {

    private static final Logger logger = LogManager.getLogger(ClassImporter.class);

    private static final int maxTxSize = Integer.parseInt(System.getProperty("maxTxSize", "10000"));

    public static void importClass(GraphDatabaseService db, DomainClass domainClass, Connection con) throws SQLException {

	Node domainClassNode = db.index().forNodes("className").get("className", domainClass.getFullName()).getSingle();

	Index<Node> oidIndex = db.index().forNodes("oid");

	if (domainClassNode == null && domainClass.getName().equals("PersistentRoot")) {
	    return;
	}

	if (domainClassNode == null) {
	    throw new RuntimeException("Bootstrapping went wrong, node missing for class: " + domainClass.getFullName());
	}

	DomainClass topClass = Util.getTopClass(domainClass);

	String dbName = Util.getDBName(topClass.getName());

	Statement st = con.createStatement();
	ResultSet rs;

	DatabaseMetaData md = con.getMetaData();

	try (ResultSet colRs = md.getColumns(null, null, dbName, "OJB_CONCRETE_CLASS")) {
	    if (colRs.next()) {
		rs = st.executeQuery("SELECT * FROM " + dbName + " WHERE OJB_CONCRETE_CLASS = '" + domainClass.getFullName()
			+ "' OR OJB_CONCRETE_CLASS = ''");
	    } else {
		rs = st.executeQuery("SELECT * FROM " + dbName);
	    }
	}

	logger.info("Retrieving instances of " + domainClass.getFullName() + " from SQL table " + dbName);

	int count = 0;

	Transaction tx = db.beginTx();

	while (rs.next()) {
	    count++;

	    Node newNode = db.createNode();

	    domainClassNode.createRelationshipTo(newNode, Relations.INSTANCE_OF);

	    DomainClass cls = domainClass;

	    while (cls != null) {

		for (Slot slot : cls.getSlotsList()) {
		    Object obj = rs.getObject(Util.getDBName(slot.getName()));

		    if (obj instanceof Timestamp) {
			Timestamp timestamp = (Timestamp) obj;

			obj = new DateTime(timestamp.getTime()).toString();
		    }

		    if (obj != null)
			newNode.setProperty(slot.getName(), obj);
		}

		cls = (DomainClass) cls.getSuperclass();
	    }

	    Long oid = rs.getLong("OID");

	    newNode.setProperty("oid", oid);

	    oidIndex.add(newNode, "oid", oid);

	    /*
	     * Commit the Transaction every "maxTxSize" records.
	     */
	    if (count % maxTxSize == 0) {
		logger.trace("Committing Neo4j Transaction. Got " + count + " objects so far.");
		tx.success();
		tx.finish();
		tx = db.beginTx();
	    }
	}
	tx.success();
	tx.finish();

	rs.close();
	st.close();

	logger.info("Finished importing. Copied " + count + " objects.");

    }

}
