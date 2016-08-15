package org.apache.ddlutils.model;

import java.util.ArrayList;

/**
 * Created by jyuan on 10/7/16.
 */
public class UniqueConstraint extends UniqueIndex {

    /**
     * Unique ID for serialization purposes.
     */
    private static final long serialVersionUID = -2097503126750294993L;


    public static UniqueConstraint createUniqueConstraint(UniqueIndex index) {
        UniqueConstraint result = new UniqueConstraint();
        result._name = index._name;
        result._columns = index._columns;

        return result;
    }
    /**
     * {@inheritDoc}
     */
    public Index getClone() throws ModelException {
        UniqueConstraint result = new UniqueConstraint();

        result._name = _name;
        result._columns = (ArrayList) _columns.clone();

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equalsIgnoreCase(Index other) {
        if (other instanceof UniqueConstraint) {
            UniqueIndex otherIndex = (UniqueIndex) other;

            boolean checkName = (_name != null) && (_name.length() > 0) &&
                    (otherIndex._name != null) && (otherIndex._name.length() > 0);

            if ((!checkName || _name.equalsIgnoreCase(otherIndex._name)) &&
                    (getColumnCount() == otherIndex.getColumnCount())) {
                for (int idx = 0; idx < getColumnCount(); idx++) {
                    if (!getColumn(idx).equalsIgnoreCase(otherIndex.getColumn(idx))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public String toVerboseString() {
        StringBuffer result = new StringBuffer();

        result.append("Unique constraint [");
        result.append(getName());
        result.append("] columns:");
        for (int idx = 0; idx < getColumnCount(); idx++) {
            result.append(" ");
            result.append(getColumn(idx).toString());
        }

        return result.toString();
    }
    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Unique constraint [name=");
        result.append(getName());
        result.append("; ");
        result.append(getColumnCount());
        result.append(" columns]");

        return result.toString();
    }
}
