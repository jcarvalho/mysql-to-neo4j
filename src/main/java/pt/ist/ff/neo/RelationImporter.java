package pt.ist.ff.neo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import dml.DomainClass;
import dml.DomainRelation;
import dml.Role;

public class RelationImporter {

    private static final Logger logger = LogManager.getLogger(RelationImporter.class);

    private static final Map<String, String> CONFIG_MAP = MapUtil.stringMap("type", "exact");

    public static void importRelation(BatchInserter db, BatchInserterIndexProvider indexProvider, final DomainRelation relation,
	    Connection con) throws SQLException {

	logger.info("Importing relation " + relation.getName());


	Role dataRole = getDataRole(relation);

	if (dataRole == null) {
	    logger.warn("Ignoring many-to-many relation for now: " + relation.getFullName());
	    return;
	}

	Role otherRole = dataRole.getOtherRole();

	BatchInserterIndex oneIndex = indexProvider.nodeIndex(dataRole.getType().getFullName(), CONFIG_MAP);
	BatchInserterIndex otherIndex = indexProvider.nodeIndex(otherRole.getType().getFullName(), CONFIG_MAP);

	if (otherRole.getName() == null) {
	    logger.warn("Ignoring anonymous relations for now: " + relation.getFullName());
	    return;
	}

	RelationshipType type = new RelationshipType() {

	    @Override
	    public String name() {
		return relation.getName();
	    }
	};

	DomainClass classToGet = Util.getTopClass((DomainClass) dataRole.getType());

	String dbName = Util.getDBName(classToGet.getName());

	if (dbName.equals("PERSISTENT_ROOT")) {
	    logger.warn("Not handling connections to Persistent Root yet...");
	    return;
	}

	String relationCol = "OID_" + Util.getDBName(otherRole.getName());

	logger.trace("\tRelation is on table " + dbName + ". Column: " + relationCol);

	try (Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("SELECT OID, " + relationCol + " FROM " + dbName + " WHERE " + relationCol
			+ " IS NOT NULL")) {

	    int count = 0;

	    while (rs.next()) {
		count++;

		Long oid = rs.getLong("OID");
		Long otherOid = rs.getLong(relationCol);

		Long one = oneIndex.get("oid", oid).getSingle();
		Long other = otherIndex.get("oid", otherOid).getSingle();

		if (one == null || other == null) {
		    logger.error("\t\tNode does not exist! Original OID: " + oid + ". Other OID : " + otherOid);
		    logger.error("\t\t\tOne: " + one + " Other: " + other);
		    continue;
		}

		db.createRelationship(one, other, type, null);
		db.createRelationship(other, one, type, null);

	    }

	    logger.info("Finished importing relations. Imported " + count + " instances.");

	}

    }

    private static Role getDataRole(DomainRelation relation) {

	if (relation.getFirstRole().getMultiplicityUpper() != 1 && relation.getSecondRole().getMultiplicityUpper() != 1)
	    return null;

	if (relation.getFirstRole().getMultiplicityUpper() != 1)
	    return relation.getFirstRole();

	if (relation.getSecondRole().getMultiplicityUpper() != 1)
	    return relation.getSecondRole();

	return relation.getFirstRole();
    }

}
