package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import dml.DomainClass;
import dml.Slot;

public class ClassImporter {

    public static void importClass(GraphDatabaseService db, DomainClass domainClass, Connection con) throws SQLException {

	Node domainClassNode = db.index().forNodes("className").get("className", domainClass.getFullName()).getSingle();

	Index<Node> oidIndex = db.index().forNodes("oid");

	if (domainClassNode == null) {
	    System.out.println("NULL!!!");
	    throw new RuntimeException();
	}

	DomainClass topClass = getTopClass(domainClass);

	String dbName = getDBName(topClass.getName());

	Statement st = con.createStatement();
	ResultSet rs;

	DatabaseMetaData md = con.getMetaData();

	ResultSet colRs = md.getColumns(null, null, dbName, "OJB_CONCRETE_CLASS");

	if (colRs.next()) {
	    rs = st.executeQuery("SELECT * FROM " + dbName + " WHERE OJB_CONCRETE_CLASS = '" + domainClass.getFullName()
		    + "' OR OJB_CONCRETE_CLASS = ''");
	} else {
	    rs = st.executeQuery("SELECT * FROM " + dbName);
	}

	colRs.close();

	System.out.println("Class " + domainClass.getFullName() + " maps to " + dbName);

	int count = 0;

	Transaction tx = db.beginTx();
	try {

	    while (rs.next()) {
		count++;

		Node newNode = db.createNode();

		domainClassNode.createRelationshipTo(newNode, Relations.INSTANCE_OF);

		DomainClass cls = domainClass;

		while (cls != null) {

		    for (Slot slot : cls.getSlotsList()) {
			Object obj = rs.getObject(getDBName(slot.getName()));

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
		 * ResultSetMetaData md = rs.getMetaData();
		 * 
		 * for (int i = 1; i <= md.getColumnCount(); i++) {
		 * 
		 * String colName = md.getColumnName(i); if
		 * (colName.equals("ID_INTERNAL") ||
		 * colName.equals("OJB_CONCRETE_CLASS")) continue;
		 * 
		 * if (colName.startsWith("OID_")) { continue; } Object value;
		 * 
		 * switch (md.getColumnType(i)) { case (Types.BIGINT): value =
		 * rs.getObject(i) == null ? null : rs.getLong(i); break;
		 * default: value = rs.getObject(i); }
		 * 
		 * if (colName.equals("OID")) colName = "oid";
		 * 
		 * if (value != null) newNode.setProperty(colName, value); }
		 */
	    }
	    tx.success();
	} finally {
	    tx.finish();
	}

	rs.close();
	st.close();

	System.out.println(dbName + " got " + count);
    }

    private static DomainClass getTopClass(DomainClass domainClass) {
	if (domainClass.getSuperclass() == null)
	    return domainClass;
	return getTopClass((DomainClass) domainClass.getSuperclass());
    }

    private static String getDBName(String name) {

	StringBuilder str = new StringBuilder();

	for (int i = 0; i < name.length(); i++) {
	    char c = name.charAt(i);

	    if (Character.isUpperCase(c) && i != 0) {
		str.append("_");
	    }

	    str.append(Character.toUpperCase(c));
	}

	return str.toString();
    }

}
