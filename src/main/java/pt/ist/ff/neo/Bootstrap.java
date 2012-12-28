package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import dml.DomainClass;
import dml.DomainModel;

public class Bootstrap {

    public static void bootStrap(Connection con, GraphDatabaseService db, DomainModel model) throws SQLException {
	Transaction tx = db.beginTx();
	try {

	    Node rootNode = db.getNodeById(0);

	    Index<Node> classNameIndex = db.index().forNodes("className");

	    // Ensure OID index exists
	    db.index().forNodes("oid");

	    for (DomainClass domainClass : model.getDomainClasses()) {
		Node classNode = db.createNode();

		ResultSet rs = con.createStatement().executeQuery(
			"SELECT * FROM FF$DOMAIN_CLASS_INFO WHERE DOMAIN_CLASS_NAME = '" + domainClass.getFullName() + "'");

		rs.next();

		classNode.setProperty("className", domainClass.getFullName());
		classNode.setProperty("classId", rs.getObject("DOMAIN_CLASS_ID"));
		classNameIndex.add(classNode, "className", domainClass.getFullName());

		rootNode.createRelationshipTo(classNode, Relations.DOMAIN_CLASS);
	    }

	    tx.success();
	} finally {
	    tx.finish();
	}
    }
}
