package org.irods.jargon.core.query;

/**
 * Represents an order by element of a builder query.
 * 
 * @author Mike Conway - DICE (www.irods.org)
 * 
 */
public class GenQueryOrderByField extends GenQueryField {

	public enum OrderByType {
		NONE, ASC, DESC
	}

	private final OrderByType orderByType;

	/**
	 * Create an order by field in a query.
	 * 
	 * @param selectFieldColumnName
	 *            <code>String</code> with the name of the column
	 * @param selectFieldSource
	 *            {@link SelectFieldSource} indicates the type of query field
	 * @param selectFieldNumericTranslation
	 *            <code>String</code> with the iRODS protocol translation of a
	 *            gen query field
	 * @param orderByType
	 *            {@link OrderByType} that indicates the order by type.
	 * @return
	 */
	static GenQueryOrderByField instance(final String selectFieldColumnName,
			final SelectFieldSource selectFieldSource,
			final String selectFieldNumericTranslation,
			final OrderByType orderByType) {
		return new GenQueryOrderByField(selectFieldColumnName,
				selectFieldSource, selectFieldNumericTranslation, orderByType);
	}

	/**
	 * @param selectFieldColumnName
	 * @param selectFieldSource
	 * @param selectFieldNumericTranslation
	 * @param orderByType
	 */
	private GenQueryOrderByField(final String selectFieldColumnName,
			final SelectFieldSource selectFieldSource,
			final String selectFieldNumericTranslation,
			final OrderByType orderByType) {

		super(selectFieldColumnName, selectFieldSource,
				selectFieldNumericTranslation);

		if (orderByType == null) {
			throw new IllegalArgumentException("null orderByType");
		}

		this.orderByType = orderByType;
	}

	/**
	 * @return the orderByType
	 */
	public OrderByType getOrderByType() {
		return orderByType;
	}

}
