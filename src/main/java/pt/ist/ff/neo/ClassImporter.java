package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import dml.DomainClass;
import dml.Slot;

public class ClassImporter {

    private static final Logger logger = LogManager.getLogger(ClassImporter.class);

    public static void importClass(BatchInserter db, BatchInserterIndexProvider indexProvider, DomainClass domainClass,
	    Connection con) throws SQLException {

	Long classNode = indexProvider.nodeIndex("className", null).get("className", domainClass.getFullName()).getSingle();

	BatchInserterIndex oidIndex = indexProvider.nodeIndex("oid", null);

	if (classNode == null && domainClass.getName().equals("PersistentRoot")) {
	    return;
	}

	if (classNode == null) {
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

	while (rs.next()) {
	    count++;

	    Map<String, Object> properties = new HashMap<>();

	    DomainClass cls = domainClass;

	    while (cls != null) {

		for (Slot slot : cls.getSlotsList()) {
		    Object obj = rs.getObject(Util.getDBName(slot.getName()));

		    if (obj instanceof Timestamp) {
			Timestamp timestamp = (Timestamp) obj;

			obj = new DateTime(timestamp.getTime()).toString();
		    }

		    if (obj != null)
			properties.put(slot.getName(), obj);
		}

		cls = (DomainClass) cls.getSuperclass();
	    }

	    Long oid = rs.getLong("OID");

	    properties.put("oid", oid);

	    long newNode = db.createNode(properties);

	    db.createRelationship(classNode, newNode, Relations.INSTANCE_OF, null);

	    Map<String, Object> indexProperties = new HashMap<>();
	    indexProperties.put("oid", oid);

	    oidIndex.add(newNode, indexProperties);

	}

	oidIndex.flush();

	rs.close();
	st.close();

	logger.info("Finished importing. Copied " + count + " objects.");

    }
}
