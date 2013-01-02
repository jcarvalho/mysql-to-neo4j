package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import pt.ist.fenixframework.pstm.PersistentRoot;
import dml.DomainClass;
import dml.DomainModel;

public class Bootstrap {

    private static final Logger logger = LogManager.getLogger(Bootstrap.class);

    public static void bootStrap(Connection con, GraphDatabaseService db, DomainModel model) throws SQLException {
	Transaction tx = db.beginTx();
	try {

	    Node rootNode = db.getNodeById(0);

	    Index<Node> classNameIndex = db.index().forNodes("className");

	    // Ensure OID index exists
	    db.index().forNodes("oid");

	    logger.trace("Acquired root node, created indexes. Now boostrapping " + model.getDomainClasses().size()
		    + " domain classes.");

	    for (DomainClass domainClass : model.getDomainClasses()) {

		logger.trace("\t-> Bootstrapping " + domainClass.getFullName());

		if (domainClass.getFullName().equals(PersistentRoot.class.getName())) {

		    logger.trace("\t\t-> Warning, class not bootstrapped!");
		    // TODO How to handle this?
		    continue;
		}

		ResultSet rs = con.createStatement().executeQuery(
			"SELECT * FROM FF$DOMAIN_CLASS_INFO WHERE DOMAIN_CLASS_NAME = '" + domainClass.getFullName() + "'");

		rs.next();

		Node classNode = db.createNode();

		Object classId = rs.getObject("DOMAIN_CLASS_ID");

		classNode.setProperty("className", domainClass.getFullName());
		classNode.setProperty("classId", classId);
		classNameIndex.add(classNode, "className", domainClass.getFullName());

		rootNode.createRelationshipTo(classNode, Relations.DOMAIN_CLASS);

		if (logger.isTraceEnabled()) {
		    logger.trace("\t\t-> Mapped class to ID: " + classId);
		}
	    }

	    tx.success();
	} finally {
	    tx.finish();
	    logger.trace("--> Finished Bootstrapping classes <--");
	}
    }
}
