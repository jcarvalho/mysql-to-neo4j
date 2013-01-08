package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import dml.DomainClass;
import dml.DomainModel;

public class Bootstrap {

    private static final Logger logger = LogManager.getLogger(Bootstrap.class);

    public static void bootStrap(Connection con, BatchInserter db, BatchInserterIndexProvider indexProvider, DomainModel model)
	    throws SQLException {

	/*
	 * Create the className index, and cache its contents.
	 */
	Map<String, String> luceneProperties = MapUtil.stringMap("type", "exact");
	BatchInserterIndex classIndex = indexProvider.nodeIndex("className", luceneProperties);
	classIndex.setCacheCapacity("className", model.getDomainClasses().size());

	// Ensure OID index exists

	logger.info("Acquired root node, created indexes. Now boostrapping " + model.getDomainClasses().size()
		+ " domain classes.");

	for (DomainClass domainClass : model.getDomainClasses()) {

	    logger.trace("\t-> Bootstrapping " + domainClass.getFullName());

	    if (domainClass.getFullName().equals("pt.ist.fenixframework.pstm.PersistentRoot")) {

		logger.warn("\t\t-> Warning, class not bootstrapped: " + domainClass.getFullName());
		// TODO How to handle this?
		continue;
	    }

	    indexProvider.nodeIndex(domainClass.getFullName(), luceneProperties).flush();

	    ResultSet rs = con.createStatement().executeQuery(
		    "SELECT * FROM FF$DOMAIN_CLASS_INFO WHERE DOMAIN_CLASS_NAME = '" + domainClass.getFullName() + "'");

	    rs.next();

	    Object classId = rs.getObject("DOMAIN_CLASS_ID");

	    Map<String, Object> properties = new HashMap<>();

	    properties.put("className", domainClass.getFullName());
	    properties.put("classId", classId);

	    long classNode = db.createNode(properties);
	    db.createRelationship(0, classNode, Relations.DOMAIN_CLASS, null);

	    Map<String, Object> indexProperties = new HashMap<>();
	    indexProperties.put("className", domainClass.getFullName());

	    classIndex.add(classNode, indexProperties);

	    if (logger.isTraceEnabled()) {
		logger.trace("\t\t-> Mapped class to ID: " + classId);
	    }

	    rs.close();
	}

	classIndex.flush();
	logger.info("--> Finished Bootstrapping classes <--");
    }
}
