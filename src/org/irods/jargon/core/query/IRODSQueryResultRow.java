/**
 * 
 */
package org.irods.jargon.core.query;

import java.util.List;

import org.irods.jargon.core.exception.JargonException;

/**
 * Represents a row in a query response, with convenience methods to access
 * attributes
 * 
 * @author Mike Conway - DICE (www.irods.org)
 * 
 */
public class IRODSQueryResultRow {

	private final List<String> queryResultColumns;
	/**
	 * index of this record in the results
	 */
	private final int recordCount;
	private final boolean lastResult;
	private final List<String> columnNames;

	/**
	 * Build a result row from a column of results produced by an IRODS GenQuery
	 * 
	 * @param queryResultColumns
	 *            <code>List</code> of query result columns
	 * @param columnNames
	 *            <code>List<String></code> containing the column names.
	 * @return <code>IRODSQueryResultRow</code> with the data for this row.
	 * @throws JargonException
	 */
	public static IRODSQueryResultRow instance(
			final List<String> queryResultColumns,
			final List<String> columnNames) throws JargonException {
		return new IRODSQueryResultRow(queryResultColumns, columnNames, 0,
				false);
	}

	/**
	 * Build a result row from a column of results produced by an IRODS
	 * GenQuery. This initializer will add information about the position of the
	 * record to assist in re-query operations
	 * 
	 * @param queryResultColumns
	 *            <code>List<String</code> with the values for each column of
	 *            the query
	 * @param translatedIRODSQuery
	 *            {@link TranslatedIRODSQuery} that produced this result
	 * @param recordCount
	 *            <code>int</code> with the index of the current record
	 * @param lastResult
	 *            <code>boolean</code> of <code>true</code> if there are more
	 *            records to page
	 * @return
	 * @throws JargonException
	 */
	public static IRODSQueryResultRow instance(
			final List<String> queryResultColumns,
			final List<String> columnNames, final int recordCount,
			final boolean lastResult) throws JargonException {
		return new IRODSQueryResultRow(queryResultColumns, columnNames,
				recordCount, lastResult);
	}

	/**
	 * Build a result row from a column of results produced by an IRODS
	 * GenQuery. This initializer will add information about the position of the
	 * record to assist in re-query operations
	 * 
	 * @param queryResultColumns
	 *            <code>List<String</code> with the values for each column of
	 *            the query
	 * @param translatedIRODSQuery
	 *            {@link TranslatedIRODSQuery} that produced this result
	 * @param recordCount
	 *            <code>int</code> with the index of the current record
	 * @param lastResult
	 *            <code>boolean</code> of <code>true</code> if there are more
	 *            records to page
	 * @return
	 * @throws JargonException
	 */
	public static IRODSQueryResultRow instance(
			final List<String> queryResultColumns,
			final List<String> columnNames, final int recordCount,
			final boolean lastResult, final int totalRecords)
			throws JargonException {
		return new IRODSQueryResultRow(queryResultColumns, columnNames,
				recordCount, lastResult);
	}

	/**
	 * Private constructor
	 * 
	 * @param queryResultColumns
	 *            <code>List<String</code> with the values for each column of
	 *            the query
	 * @param translatedIRODSQuery
	 *            {@link TranslatedIRODSQuery} that produced this result
	 * @param recordCount
	 *            <code>int</code> with the index of the current record
	 * @param lastResult
	 *            <code>boolean</code> of <code>true</code> if there are more
	 *            records to page
	 * @throws JargonException
	 */
	private IRODSQueryResultRow(final List<String> queryResultColumns,
			final List<String> columnNames, final int recordCount,
			final boolean lastResult) throws JargonException {

		if (queryResultColumns == null) {
			throw new JargonException("queryResultColumns is null");
		}

		if (columnNames == null) {
			throw new JargonException("columnNames is null");
		}

		this.queryResultColumns = queryResultColumns;
		this.lastResult = lastResult;
		this.recordCount = recordCount;
		this.columnNames = columnNames;

	}

	/**
	 * Given a columnNumber, return the value of the column in the result set.
	 * 
	 * @param columnName
	 *            <code>int</code> with the location of the desired field.
	 * @return <code>String</code> containing the value of the column. It is up
	 *         to the caller to cast to the appropriate type.
	 * @throws JargonException
	 *             Indicates that the column could not be located in the
	 *             results.
	 */
	public String getColumn(final int columnNumber) throws JargonException {
		if (columnNumber < 0 || columnNumber >= queryResultColumns.size()) {
			throw new JargonException("column out of range");
		}
		return queryResultColumns.get(columnNumber);
	}

	/**
	 * Given a columnName, return the value of the column in the result set.
	 * 
	 * @param columnName
	 *            <code>String</code> with the name of the desired field. The
	 *            search is case-insensitive.
	 * @return <code>String</code> containing the value of the column. It is up
	 *         to the caller to cast to the appropriate type.
	 * @throws JargonException
	 *             Indicates that the column could not be located in the
	 *             results.
	 */
	public String getColumn(final String columnName) throws JargonException {
		if (columnName == null || columnName.length() == 0) {
			throw new JargonException("columnName is null or empty");
		}

		int idx = getColumnNamePosition(columnName);

		if (idx == -1) {
			throw new JargonException("column name not found in result set:"
					+ columnName);
		}

		return queryResultColumns.get(idx);

	}

	/**
	 * Returns the index of the column with the given name, or -1 if not found.
	 * The match is case-insensitive;
	 * 
	 * @param columnName
	 * @return
	 */
	protected int getColumnNamePosition(final String columnName) {
		int colPos = -1;
		int i = 0;
		for (String colNameInList : columnNames) {
			if (columnName.equals(colNameInList)) {
				colPos = i;
				break;
			}
			i++;
		}

		return colPos;
	}

	/**
	 * Handy method to just get the results as a <code>List</code> for use in
	 * forEach and other constructs
	 * 
	 * @return <code>List<String></code> containing the columns for this row.
	 */
	public List<String> getColumnsAsList() {
		return queryResultColumns;
	}

	public List<String> getQueryResultColumns() {
		return queryResultColumns;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public boolean isLastResult() {
		return lastResult;
	}

}
