/*
 * Copyright (c) 2015. OSR Data Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.osrdata.etltoolbox.fileloader;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class both encapsulates the source-to-target mappings and the processing of a source file to a database
 * targetTable.
 */
public class FileSpecification {
    private static final Logger log = LogManager.getLogger(FileSpecification.class);
    private Pattern sourcePattern;
    private Integer dateGroup;
    private String dateFormat;
    private Integer typeGroup;
    private Integer sourceId;
    private File sourceFile;
    private List<String> targetColumns = null;
    private String targetTable;
    private String targetSql;
    private DataSource targetDs = null;
    private JdbcTemplate targetTemplate = null;
    private DataSource auditDs = null;
    private int parserLine = 0;
    private char parserSeparator = CSVParser.DEFAULT_SEPARATOR;
    private char parserQuotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
    private char parserEscape = CSVParser.DEFAULT_ESCAPE_CHARACTER;
    private boolean parserStrictQuotes = CSVParser.DEFAULT_STRICT_QUOTES;
    private boolean parserIgnoreLeadingWhiteSpace = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
    private int batchThreshold;
    private boolean replaceExisting;
    private Date etlDate;
    private String etlType;
    private BigDecimal recordId;
    private int numColumns = 0;
    private long numRecords = 0l;
    private ArrayList<Object[]> records = new ArrayList<Object[]>();
    private long startTime = 0l;
    private long trace = 0l;

    /**
     * Constructs and initializes this object using source to target specifications contained in specification file.
     * @param spec file name of JSON formatted file that contains source to target specifications
     */
    public FileSpecification(Map<String, Object> spec, DataSource auditDs, DataSource targetDs, int batchThreshold,
                             boolean replaceExisting, long trace) {
        String stringProperty = (String) spec.get("sourcePattern");

        sourcePattern = Pattern.compile(stringProperty, Pattern.CASE_INSENSITIVE);

        if (spec.containsKey("parserLine")) {
            parserLine = (Integer) spec.get("parserLine");
        }

        if (spec.containsKey("parserSeparator")) {
            stringProperty = (String) spec.get("parserSeparator");
            parserSeparator = stringProperty.charAt(0);
        }

        if (spec.containsKey("parserQuotechar")) {
            stringProperty = (String) spec.get("parserQuotechar");
            parserQuotechar = stringProperty.charAt(0);
        }

        if (spec.containsKey("parserEscape")) {
            stringProperty = (String) spec.get("parserEscape");
            parserEscape = stringProperty.charAt(0);
        }

        dateGroup = (Integer) spec.get("dateGroup");
        dateFormat = (String) spec.get("dateFormat");
        typeGroup = (Integer) spec.get("typeGroup");
        sourceId = (Integer) spec.get("sourceId");
        targetTable = (String) spec.get("targetTable");
        targetColumns = (List<String>) spec.get("targetColumns");

        this.auditDs = auditDs;
        this.targetDs = targetDs;
        this.batchThreshold = batchThreshold;
        this.replaceExisting = replaceExisting;
        this.trace = trace;

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ").append(targetTable).append(" (");
        boolean firstColumn = true;
        for (int i = 0; i < targetColumns.size(); i++) {
            // Columns defined as empty string indicate that they should be skipped
            if (!targetColumns.get(i).equals("")) {
                if (firstColumn) {
                    sb.append(targetColumns.get(i));
                    firstColumn = false;
                } else {
                    sb.append(", ").append(targetColumns.get(i));
                }
            }
        }
        if (sourceId != null) {
            sb.append(", source_id, file_id, record_id)");
        } else {
            sb.append(")");
        }
        sb.append(" values (");
        firstColumn = true;
        for (int i = 0; i < targetColumns.size(); i++) {
            // Columns defined as empty string indicate that they should be skipped
            if (!targetColumns.get(i).equals("")) {
                if (firstColumn) {
                    sb.append("?");
                    firstColumn = false;
                } else {
                    sb.append(", ?");
                }
                numColumns++;
            }
        }

        // If source ID is not defined, then do not include source ID, file ID, and record ID fields.
        if (sourceId != null) {
            sb.append(", ?, ?, ?)");
            numColumns += 3;
        } else {
            sb.append(")");
        }
        targetSql = sb.toString();
    }

    /**
     * Determines if specified source file's name matches sourcePattern defined for this source to target specification.
     * @param sourceFile source file to match against
     * @return true, if file's name matches source sourcePattern; otherwise, false
     */
    public boolean match(File sourceFile) {
        Matcher matcher = sourcePattern.matcher(sourceFile.getName());
        return (matcher.matches());
    }

    /**
     * Loads specified file into target targetTable. This operation transactional and will rollback any database operations if
     * there are any errors processing the data.
     *
     * @param sourceFile source file to be loaded
     * @throws IOException on error reading file
     * @throws ParseException on error parsing fields from file
     */
    public void load(final File sourceFile) throws IOException, ParseException {
        this.sourceFile = sourceFile;
        Matcher matcher = sourcePattern.matcher(sourceFile.getName());
        etlDate = new Date();
        etlType = "I";
        if (matcher.find()) {
            if (dateGroup != null && dateGroup.intValue() <= matcher.groupCount()) {
                etlDate = new SimpleDateFormat(dateFormat).parse(matcher.group(dateGroup.intValue()));
            }
            if (typeGroup != null && typeGroup.intValue() <= matcher.groupCount()) {
                etlType = matcher.group(typeGroup.intValue()).substring(0,1).toUpperCase();
            }
        }
        recordId = new BigDecimal(new SimpleDateFormat("yyyyMMdd").format(etlDate) + "0000000000");

        DataSourceTransactionManager txManager = new DataSourceTransactionManager(targetDs);
        TransactionTemplate txTemplate = new TransactionTemplate(txManager);
        targetTemplate = new JdbcTemplate(targetDs);

        log.info("Processing source file " + sourceFile.getName());
        numRecords = 0l;
        try {
            txTemplate.execute(new TransactionCallbackWithoutResult() {
                public void doInTransactionWithoutResult(TransactionStatus status) {
                    try {
                        boolean loadFlag = false;
                        Integer fileId = selectAuditFile();
                        if (fileId != null && replaceExisting) {
                            deleteExisting(fileId);
                            updateAuditFile(fileId);
                            loadFlag = true;
                        } else if (fileId == null) {
                            fileId = insertAuditFile();
                            loadFlag = true;
                        }

                        if (loadFlag) {
                            CSVReader reader = new CSVReader(new FileReader(sourceFile), parserSeparator, parserQuotechar, parserEscape,
                                    parserLine, parserStrictQuotes, parserIgnoreLeadingWhiteSpace);
                            String[] values;
                            startTime = System.currentTimeMillis();
                            while ((values = reader.readNext()) != null) {
                                add(values, fileId);
                                numRecords++;
                                if (trace > 0l && numRecords % trace == 0l) {
                                    log.info("\tProcessed " + getCount(numRecords) + " records in " + getDuration() + " (" + getRecordsPerSecond() + " rps)");
                                }
                            }
                            reader.close();
                            insertTarget();
                        } else {
                            log.info("\tSkipping previously loaded file" + sourceFile.getName());
                        }
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Throwable e) {
                        log.error("\tError at record " + numRecords + " in " + sourceFile.getName());
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (RuntimeException e) {
            log.error("\tAn exception occurred while processing record " + numRecords + " in " + sourceFile.getName() + ". All transactions for this file have been rolled back.", e);
        }
        log.info("\tCompleted processing of " + getCount(numRecords) + " records in " + getDuration() + " (" + getRecordsPerSecond() + " rps)");
    }

    /**
     * Inserts record for source file into the audit_file table.
     */
    private Integer insertAuditFile() {
        JdbcTemplate template = new JdbcTemplate(auditDs);
        // TODO: Add support for other database types.
        Integer fileId = template.queryForObject("select next value for seq_audit", Integer.class);
        int result =  template.update("insert into audit_file values (?, ?, ?, ?, ?, ?, ?)",
                new Object[]{fileId, sourceId, sourceFile.getName(), targetTable, etlType, etlDate, "N" });
        log.debug("\tInsert into audit_file returned " + result + ", fileId " + fileId);
        return fileId;
    }

    /*
     * Selects file_id of record from audit_file table that matches criteria of file to be loaded, if record exists.
     */
    private Integer selectAuditFile() {
        JdbcTemplate template = new JdbcTemplate(auditDs);
        Integer fileId;
        try {
            fileId = template.queryForObject(
                    "select file_id from audit_file where source_id = ? and file_name = ? and table_name = ? and etl_type = ? and etl_date = ?",
                    new Object[]{sourceId, sourceFile.getName(), targetTable, etlType, etlDate},
                    Integer.class);
        } catch (EmptyResultDataAccessException e) {
            fileId = null;
        }
        log.debug("\tSelect audit_file returned " + fileId);
        return fileId;
    }

    /*
     * Updates existing audit_file record to reset its processed_flag to "N".
     */
    private void updateAuditFile(Integer fileId) {
        JdbcTemplate template = new JdbcTemplate(auditDs);
        int result = template.update("update audit_file set processed_flag = ? where file_id = ?", new Object[] { "N", fileId });
        log.debug("\tUpdate audit_file returned " + result);
    }

    /**
     * Deletes existing records from target targetTable with file ID. If source ID is not defined then deletes all
     * records from target targetTable.
     */
    private void deleteExisting(Integer fileId) {
        JdbcTemplate template = new JdbcTemplate(targetDs);
        int count;
        StringBuilder sql = new StringBuilder();
        sql.append("delete from ").append(targetTable);
        if (sourceId != null ) {
            sql.append(" where file_id = ?");
            count = template.update(sql.toString(), new Object[]{ fileId });
        } else {
            count = template.update(sql.toString());
        }
        log.info("\tDeleted " + getCount(count) + " existing records from " + targetTable);
    }

    /**
     * Adds record to batch insertTarget cache.
     */
    private void add(String[] values, Integer fileId) {
        Object[] record = new Object[numColumns];
        int j = 0;
        for (int i = 0; i < targetColumns.size(); i++) {
            if (!targetColumns.get(i).equals("")) {
                if (i < values.length) {
                    record[j] = values[i];
                } else {
                    record[j] = new String();
                }
                j++;
            }
        }
        if (sourceId != null) {
            record[numColumns - 3] = sourceId;
            record[numColumns - 2] = fileId;
            recordId = recordId.add(new BigDecimal(1));
            record[numColumns - 1] = recordId;
        }
        records.add(record);
        if (records.size() >= batchThreshold) {
            insertTarget();
        }
    }

    /**
     * Inserts cached records into database targetTable using batch update.
     */
    private void insertTarget() {
        if (records.size() > 0) {
            targetTemplate.batchUpdate(targetSql, records);
            log.debug("\tInserted " + records.size() + " records into " + targetTable);
            records.clear();
        }
    }

    /**
     * Gets formatted duration of current load.
     * @return formatted duration
     */
    private String getDuration() {
        long duration = System.currentTimeMillis() - startTime;
        long milliseconds = duration % 1000;
        long seconds = (duration / 1000) % 60;
        long minutes = (duration / (1000 * 60)) % 60;
        long hours = (duration / (1000 * 60 * 60)) % 24;
        return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds);
    }

    /**
     * Gets formatted count.
     * @param count value to format
     * @return formatted value.
     */
    private String getCount(long count) {
        return String.format("%,d", count);
    }

    /**
     * Gets formatted records per second value.
     * @return
     */
    private String getRecordsPerSecond() {
        long duration = System.currentTimeMillis() - startTime;
        double recordsPerSecond = (double) numRecords / (duration / 1000.0d);
        return String.format("%.2f", recordsPerSecond);
    }
}
