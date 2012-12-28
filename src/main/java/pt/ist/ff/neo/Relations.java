package pt.ist.ff.neo;

import org.neo4j.graphdb.RelationshipType;

public enum Relations implements RelationshipType {

    DOMAIN_CLASS,

    INSTANCE_OF;

}
