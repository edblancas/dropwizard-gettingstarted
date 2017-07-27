/*
 *      File: AbstractHBaseDAO.java
 *    Author: Horacio Ferro <horacio.ferro@amk-technologies.com>
 *      Date: Jun 01, 2017
 * Copyright: AMK Technologies, S.A. de C.V. 2017
 */
package edblancas.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class used to create HBase Data Access Objects. This class defines common behavior, like put, scan and methods
 * that must be overridden.
 *
 * @author Horacio Ferro &lt;horacio.ferro@amk-technologies.com&gt;
 * @version 1.0.0
 * @param <K> The object used as key for the record.
 * @param <V> Type of object that must be converted to Put or from Row.
 * @since 1.0.0
 */
public abstract class AbstractHBaseDAO<K, V> {
    /** Byte used to add to the last row key. */
    protected static final byte[] CERO_BYTE_ARRAY = {0x00};
    /** Class logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHBaseDAO.class);
    /** Byte array containing the name of the column family containing the original row key in the reverse index. */
    private static final byte[] ROWKEY_COLUMN_FAMILY = Bytes.toBytes("k");
    /** The column name containing the original row key. */
    private static final byte[] ROWKEY_COLUMN_NAME = Bytes.toBytes("r");
    /** The column family containing the counters. */
    private static final byte[] COUNTER_COLUMN_FAMILY = Bytes.toBytes("c");
    /** The name of the column with the actual counter. */
    private static final byte[] COUNTER_COLUMN_NAME = Bytes.toBytes("c");
    /** Table last identifier. */
    private static final byte[] IDENTIFIER_COLUMN_NAME = Bytes.toBytes("i");
    /** Table used for serialization. */
    private final transient Table table;
    /** Table used as reverse index. */
    private transient Table reverseIndex;
    /** Table used to store the table counts. */
    private transient Table countersTable;
    /** Flag used to test if the paging can be done in reverse. */
    private transient boolean useReverseIndex;

    /**
     * Sets the table to interact with at creation. If the reverseIndex parameter is null then its assumed that
     * there is no reverse index, and no backward pagination will be available.
     *
     * @param table The HBase <code>Table</code> object. Must be initialized.
     * @param reverseIndex The HBase <code>Table</code> used as reverse index for pagination.
     * @param countersTable The HBase <code>Table</code> used to preserve the count for objects.
     */
    public AbstractHBaseDAO(final Table table, final Table reverseIndex, final Table countersTable) {
        this.table = table;
        this.reverseIndex = reverseIndex;
        this.countersTable = countersTable;
        useReverseIndex = reverseIndex != null;
    }

    public AbstractHBaseDAO(Table table) {
        this.table = table;
    }

    /**
     * Inserts a single object into the table.
     *
     * @param object The object to insert.
     * @throws IOException In case of errors while serializing the record to HBase.
     */
    public final void put(final V object) throws IOException {
        LOGGER.debug("Inserting: {}", object);
        final Put put = createPut(object);
        table.put(put);
        countersTable.incrementColumnValue(table.getName().toBytes(), COUNTER_COLUMN_FAMILY, COUNTER_COLUMN_NAME, 1);
        if (useReverseIndex) {
            reverseIndex.put(createReversePut(createReverseRowKeyFromObject(object), createRowKeyFromObject(object)));
            countersTable.incrementColumnValue(reverseIndex.getName().toBytes(),
                    COUNTER_COLUMN_FAMILY, COUNTER_COLUMN_NAME, 1);
        }
    }

    /**
     * Inserts a list of objects into the HBase table.
     *
     * @param objects List of objects.
     * @throws IOException In case of errors while serializing the record in HBase.
     */
    public final void put(final List<V> objects) throws IOException {
        LOGGER.debug("Inserting {} objects", objects.size());
        final List<Put> puts = new ArrayList<>(objects.size());
        for (final V object : objects) {
            LOGGER.debug("Converting to Put: {}", object);
            puts.add(createPut(object));
        }
        table.put(puts);
        countersTable.incrementColumnValue(table.getName().toBytes(),
                COUNTER_COLUMN_FAMILY, COUNTER_COLUMN_NAME, puts.size());
        if (useReverseIndex) {
            LOGGER.debug("Generating reverse index");
            puts.clear();
            for (final V object : objects) {
                puts.add(createReversePut(createReverseRowKeyFromObject(object), createRowKeyFromObject(object)));
            }
            reverseIndex.put(puts);
            countersTable.incrementColumnValue(reverseIndex.getName().toBytes(),
                    COUNTER_COLUMN_FAMILY, COUNTER_COLUMN_NAME, puts.size());
        }
    }

    /**
     * Retrieves a single record using the row key.
     *
     * @param rowKey The record row key.
     * @return The found object or null if not found.
     * @throws IOException in case of errors while recovering the record from HBase.
     */
    public final V get(final K rowKey) throws IOException {
        final byte[] rowKeyArray = createRowKey(rowKey);
        LOGGER.debug("Get to table: {}, Row key: {}", table.getName(), new String(rowKeyArray));
        final Get get = new Get(rowKeyArray);
        final Result result = table.get(get);

//        Scan scan = new Scan();
//        ResultScanner scanner = table.getScanner(scan);
//        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next())
//            System.out.println("Found row : " + result2);
//        scanner.close();

        return (result == null || result.isEmpty()) ? null : createObject(result);
    }

    /**
     * Returns the total count of rows in the actual table.
     *
     * @return The count of rows.
     * @throws IOException In case of errors while querying HBase.
     */
    public final long count() throws IOException {
        final Get get = new Get(table.getName().toBytes());
        final Result count = table.get(get);
        final long rowCount;
        if (count == null) {
            rowCount = 0;
        } else {
            rowCount = Bytes.toLong(count.getValue(COUNTER_COLUMN_FAMILY, COUNTER_COLUMN_NAME));
        }
        return rowCount;
    }

    /**
     * This method scans the original table, as HBase only scans from a row forward the pagination must be
     * done using different tables. The row key is stored in ascending order.
     *
     * @param fromKey The last key found. This record is excluded from the extracted page.
     * @param pageSize The size of the page to retrieve.
     * @return The found records, empty list if no more records or maximum the page size.
     * @throws IOException In case of errors while querying the HBase database.
     */
    public final List<V> scanForward(final K fromKey, final long pageSize) throws IOException {
        final List<V> page = new ArrayList<>();
        final Filter filters;
        final Scan scan;
        final Filter pageFilter = new PageFilter(pageSize);
        if (fromKey == null) {
            scan = new Scan();
            filters = new FilterList(pageFilter);
        } else {
            final Filter prefixFilter = createPrefixFilter(fromKey);
            final byte[] startRow = Bytes.add(createRowKey(fromKey), CERO_BYTE_ARRAY); // Used to skip the last record.
            scan = new Scan(startRow);
            if (prefixFilter != null) {
                filters = new FilterList(prefixFilter, pageFilter);
            } else {
                filters = new FilterList(pageFilter);
            }
        }
        scan.setFilter(filters);
        try (final ResultScanner scanner = table.getScanner(scan)) {
            final Iterator<Result> resultIterator = scanner.iterator();
            while (resultIterator.hasNext()) {
                page.add(createObject(resultIterator.next()));
            }
        }
        return page;
    }

    /**
     * This method scans the reverse index table, as HBase only scans from a row forward the pagination must be
     * done using different tables. The row key is stored in ascending order.
     *
     * @param fromKey The last key found. This record is excluded from the new page.
     * @param pageSize The size of the page to retrieve.
     * @return The found records, empty list if no more records or maximum the page size.
     * @throws IOException In case of errors while querying the HBase database.
     */
    public final List<V> scanBackward(final K fromKey, final long pageSize) throws IOException {
        if (useReverseIndex) {
            final List<V> page = new ArrayList<>();
            final Filter filters;
            final Scan scan;
            final Filter pageFilter = new PageFilter(pageSize);
            if (fromKey == null) {
                scan = new Scan();
                filters = new FilterList(pageFilter);
            } else {
                final Filter prefixFilter = createPrefixFilter(fromKey);
                // Used to skip the last record.
                final byte[] startRow = Bytes.add(createReverseRowKey(fromKey), CERO_BYTE_ARRAY);
                scan = new Scan(startRow);
                if (prefixFilter != null) {
                    filters = new FilterList(prefixFilter, pageFilter);
                } else {
                    filters = new FilterList(pageFilter);
                }
            }
            scan.setFilter(filters);
            try (final ResultScanner scanner = reverseIndex.getScanner(scan)) {
                final Iterator<Result> resultIterator = scanner.iterator();
                final byte[] lastRowKey;
                Result result = null;
                if (resultIterator.hasNext()) {
                    // As the order is reversed the first result is the last on the original table.
                    result = resultIterator.next();
                    lastRowKey = result.getRow();
                } else {
                    lastRowKey = null;
                }
                // Iterate until last record. If no other record, then the result will be still the first.
                while (resultIterator.hasNext()) {
                    result = resultIterator.next();
                }
                if (result != null && lastRowKey != null) {
                    final Scan rangeScan = new Scan(result.getRow(), lastRowKey);
                    try (final ResultScanner resultScanner = table.getScanner(rangeScan)) {
                        final Iterator<Result> iterator = resultScanner.iterator();
                        page.add(createObject(iterator.next()));
                    }
                }
            }
            return page;
        } else {
            throw new IllegalStateException("No reverse index provided, cannot perform backward scan.");
        }
    }

    /**
     * Retrieves the next identifier for the given current table. This works as an Oracle or PostgreSQL sequence.
     *
     * @return The next identifier.
     * @throws IOException In case of errors while incrementing the value.
     */
    protected long nextIdentifier() throws IOException {
        return countersTable.incrementColumnValue(table.getName().toBytes(),
                COUNTER_COLUMN_FAMILY, IDENTIFIER_COLUMN_NAME, 1);
    }

    /**
     * This method scans the table for records matching the different filters. This method has a limit of 1,000
     * records to prevent memory exhaustion. Also, the caller must add to the filters the prefix filter if needed.
     * This method also throws an IllegalStateException if no filter is provided.
     *
     * @param filters The filters to apply for the scan.
     * @return The found records, empty list if no records found matching the filter criteria.
     * @throws IOException In case of errors while querying the HBase database.
     */
    protected final List<V> scan(final Filter...filters) throws IOException {
        if (filters == null || filters.length == 0) {
            throw new IllegalArgumentException("Must provided at least one filter");
        } else {
            final List<V> foundRecords = new ArrayList<>();
            final Scan scan = new Scan();
            // Fail safe to prevent memory exhaustion.
            final Filter pageFilter = new PageFilter(1_000);
            final Filter[] allFilters = new Filter[filters.length + 1];
            System.arraycopy(filters, 0, allFilters, 0, filters.length);
            allFilters[filters.length] = pageFilter;
            final Filter filterList = new FilterList(allFilters);
            scan.setFilter(filterList);
            try (final ResultScanner scanner = table.getScanner(scan)) {
                final Iterator<Result> resultIterator = scanner.iterator();
                while (resultIterator.hasNext()) {
                    foundRecords.add(createObject(resultIterator.next()));
                }
            }
            return foundRecords;
        }
    }

    /**
     * This method creates a <code>Put</code> used as reverse index for backward scans.
     *
     * @param reverseRowKey The key for the reverse index record.
     * @param rowKey The original row key.
     * @return The Put to update the reverse index.
     */
    private Put createReversePut(final byte[] reverseRowKey, final byte[] rowKey) {
        final Put put = new Put(reverseRowKey);
        put.addColumn(ROWKEY_COLUMN_FAMILY, ROWKEY_COLUMN_NAME, rowKey);
        return put;
    }

    /**
     * This method must take the object <code>V</code> and create a new <code>Put</code> object. All the conversions
     * between Objects and Bytes must be done with the {@link Bytes} utility.
     *
     * @param object The object to serialize.
     * @return The Put to serialize the object.
     * @throws IOException May throw an exception if an error is encountered while generating the identifier for the
     *      record.
     */
    protected abstract Put createPut(V object) throws IOException;

    /**
     * This method must take the key object and generate the row key in a byte array.
     *
     * @param rowKey The record row key.
     * @return The key byte array.
     */
    protected abstract byte[] createRowKey(K rowKey);

    /**
     * This method must take the object and generate the row key in a byte array.
     *
     * @param object The record object.
     * @return The key byte array.
     */
    protected abstract byte[] createRowKeyFromObject(V object);

    /**
     * Creates a reverse key used for the reverse table. This implementation is needed for all the DAOs that require
     * paging.
     *
     * @param rowKey The row key.
     * @return The reverse row key.
     */
    protected abstract byte[] createReverseRowKey(K rowKey);

    /**
     * Creates a reverse key used for the reverse table. This implementation is needed for all the DAOs that require
     * paging.
     *
     * @param object The row object.
     * @return The reverse row key.
     */
    protected abstract byte[] createReverseRowKeyFromObject(V object);

    /**
     * This method must generate an object from a <code>Row</code> object.
     *
     * @param row The HBase row object.
     * @return The fully initialize object.
     */
    protected abstract V createObject(Result row);

    /**
     * The implementation of this method must create a Prefix Filter for composite keys. Otherwise must return null.
     *
     * @param rowKey The row key prefix.
     * @return A new prefix filter.
     */
    protected abstract PrefixFilter createPrefixFilter(K rowKey);

}
