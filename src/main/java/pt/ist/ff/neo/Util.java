package pt.ist.ff.neo;

import dml.DomainClass;

public class Util {

    public static DomainClass getTopClass(DomainClass domainClass) {
	if (domainClass.getSuperclass() == null)
	    return domainClass;
	return getTopClass((DomainClass) domainClass.getSuperclass());
    }

    public static String getDBName(String name) {

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
