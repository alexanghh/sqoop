/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.sqoop.cli.RelatedOptions;
import org.apache.sqoop.mapreduce.ExportInputFormat;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.mapreduce.postgresql.PostgreSQLCopyExportJob;
import org.apache.sqoop.util.SubstitutionUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.CopyOut;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.io.SplittableBufferedWriter;
import com.cloudera.sqoop.manager.ExportJobContext;
import com.cloudera.sqoop.util.DirectImportUtils;
import com.cloudera.sqoop.util.ExportException;
import com.cloudera.sqoop.util.ImportException;
import com.cloudera.sqoop.util.JdbcUrl;
import com.cloudera.sqoop.util.PerfCounters;

/**
 * Manages direct dumps from Postgresql databases via psql COPY TO STDOUT
 * commands.
 */
public class DirectPostgresqlManager extends com.cloudera.sqoop.manager.PostgresqlManager {

    public static final Log LOG = LogFactory.getLog(DirectPostgresqlManager.class.getName());

    public static final String BOOLEAN_TRUE_STRING = "boolean-true-string";
    public static final String DEFAULT_BOOLEAN_TRUE_STRING = "TRUE";

    public static final String BOOLEAN_FALSE_STRING = "boolean-false-string";
    public static final String DEFAULT_BOOLEAN_FALSE_STRING = "FALSE";

    public DirectPostgresqlManager(final SqoopOptions opts) {
        super(opts);

        if (this.booleanFalseString == null) {
            this.booleanFalseString = DEFAULT_BOOLEAN_FALSE_STRING;
        }
        if (booleanTrueString == null) {
            this.booleanTrueString = DEFAULT_BOOLEAN_TRUE_STRING;
        }
    }

    private String booleanTrueString;

    private String booleanFalseString;

    private Configuration conf;
    private DBConfiguration dbConf;
    private Connection conn = null;
    private CopyOut copyout = null;

    /**
     * Takes a list of columns and turns them into a string like "col1, col2,
     * col3...".
     */
    private String getColumnListStr(String[] cols) {
        if (null == cols) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String col : cols) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(escapeColName(col));
            first = false;
        }

        return sb.toString();
    }

    /**
     * We need to modify boolean results to 'TRUE' and 'FALSE' instead of
     * postgres t/f syntax. Otherwise hive will ignore them and import nulls
     *
     * https://issues.cloudera.org/browse/SQOOP-43
     *
     *
     * @param cols
     * @return
     */
    private String getSelectListColumnsStr(String[] cols, String tableName) {
        if (null == cols || tableName == null) {
            return null;
        }
        Map<String, String> columnTypes = getColumnTypeNamesForTable(tableName);

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String col : cols) {
            String colEscaped = escapeColName(col);
            if (!first) {
                sb.append(", ");
            }
            if (columnTypes.get(col) == null) {
                LOG.error("can not find " + col + " in type medatadata");
                sb.append(col);
            } else {
                if ("bool".equalsIgnoreCase(columnTypes.get(col))) {
                    sb.append(
                            String.format(
                                    "case when %s=true then " + "'" + booleanTrueString + "' " + "when %s=false then "
                                            + "'" + booleanFalseString + "'" + " end as %s",
                                    colEscaped, colEscaped, colEscaped));
                } else if ("bit".equalsIgnoreCase(columnTypes.get(col))) {
                    sb.append(
                            String.format(
                                    "case when %s=B'1' then " + "'" + booleanTrueString + "' " + "when %s=B'0' then "
                                            + "'" + booleanFalseString + "'" + " end as %s",
                                    colEscaped, colEscaped, colEscaped));
                } else {
                    sb.append(colEscaped);
                }
            }
            first = false;
        }

        return sb.toString();
    }

    /**
     * @return the Postgresql-specific SQL command to copy the table ("COPY ....
     *         TO STDOUT").
     */
    private String getCopyCommand(String tableName) {

        // Format of this command is:
        //
        // COPY table(col, col....) TO STDOUT
        // or COPY ( query ) TO STDOUT
        // WITH DELIMITER 'fieldsep'
        // CSV
        // QUOTE 'quotechar'
        // ESCAPE 'escapechar'
        // FORCE QUOTE col, col, col....

        StringBuilder sb = new StringBuilder();
        String[] cols = getColumnNames(tableName);

        String escapedTableName = escapeTableName(tableName);

        sb.append("COPY ");
        String whereClause = this.options.getWhereClause();
        if (whereClause == null || whereClause.isEmpty()) {
            whereClause = "1=1";
        }

        // Import from a SELECT QUERY
        sb.append("(");
        sb.append("SELECT ");
        if (null != cols) {
            sb.append(getSelectListColumnsStr(cols, tableName));
        } else {
            sb.append("*");
        }
        sb.append(" FROM ");
        sb.append(escapedTableName);
        sb.append(" WHERE ");
        sb.append(whereClause);
        sb.append(")");

        // Translate delimiter characters to '\ooo' octal representation.
        sb.append(" TO STDOUT WITH DELIMITER E'\\");
        sb.append(Integer.toString((int) this.options.getOutputFieldDelim(), 8));
        sb.append("' CSV ");

        if (this.options.getNullStringValue() != null) {
            sb.append("NULL AS E'");
            sb.append(SubstitutionUtils.removeEscapeCharacters(this.options.getNullStringValue()));
            sb.append("' ");
        }

        if (this.options.getOutputEnclosedBy() != '\0') {
            sb.append("QUOTE E'\\");
            sb.append(Integer.toString((int) this.options.getOutputEnclosedBy(), 8));
            sb.append("' ");
        }
        if (this.options.getOutputEscapedBy() != '\0') {
            sb.append("ESCAPE E'\\");
            sb.append(Integer.toString((int) this.options.getOutputEscapedBy(), 8));
            sb.append("' ");
        }

        // add the "FORCE QUOTE col, col, col..." clause if quotes are required.
        if (null != cols && this.options.isOutputEncloseRequired()) {
            sb.append("FORCE QUOTE ");
            sb.append(getColumnListStr(cols));
        }

        sb.append(";");

        String copyCmd = sb.toString();
        LOG.info("Copy command is " + copyCmd);
        return copyCmd;
    }

    // TODO(aaron): Refactor this method to be much shorter.
    // CHECKSTYLE:OFF
    @Override
    /**
     * Import the table into HDFS by using psql to pull the data out of the db
     * via COPY FILE TO STDOUT.
     */
    public void importTable(com.cloudera.sqoop.manager.ImportJobContext context) throws IOException, ImportException {

        context.setConnManager(this);

        String tableName = context.getTableName();
        SqoopOptions options = context.getOptions();

        LOG.info("Beginning psql fast path import");

        if (options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
            // TODO(aaron): Support SequenceFile-based load-in
            LOG.warn("File import layout" + options.getFileLayout() + " is not supported by");
            LOG.warn("Postgresql direct import; import will proceed as text files.");
        }

        if (!StringUtils.equals(options.getNullStringValue(), options.getNullNonStringValue())) {
            throw new ImportException("Detected different values of --input-string and --input-non-string "
                    + "parameters. PostgreSQL direct manager do not support that. Please "
                    + "either use the same values or omit the --direct parameter.");
        }

        String connectString = options.getConnectString();
        String databaseName = JdbcUrl.getDatabaseName(connectString);

        if (null == databaseName) {
            throw new ImportException("Could not determine database name");
        }

        LOG.info("Performing import of table " + tableName + " from database " + databaseName);

        PerfCounters counters = new PerfCounters();

        conf = options.getConf();
        conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "org.postgresql.Driver");
        conf.set(DBConfiguration.URL_PROPERTY, options.getConnectString());
        conf.set(DBConfiguration.USERNAME_PROPERTY, options.getUsername());

        JobConf jobConf = new JobConf(conf);
        dbConf = new DBConfiguration(jobConf);
        CopyManager cm = null;
        try {
            conn = dbConf.getConnection();
            cm = ((PGConnection) conn).getCopyAPI();
        } catch (ClassNotFoundException ex) {
            LOG.error("Unable to load JDBC driver class", ex);
            throw new IOException(ex);
        } catch (SQLException ex) {
            LOG.error("Unable to get CopyOut", ex);
            throw new IOException(ex);
        }

        SplittableBufferedWriter w = DirectImportUtils.createHdfsSink(options.getConf(), options, context);
        try {
            String copyCmd = getCopyCommand(tableName);

            // begin the import using PSQL java copy API.
            LOG.debug("Starting psql with arguments:");
            LOG.debug(copyCmd);

            counters.startClock();

            copyout = cm.copyOut(copyCmd);
            try {
                byte[] lineBytes;
                while ((lineBytes = copyout.readFromCopy()) != null) {
                    w.write(new String(lineBytes));
                    w.allowSplit();
                    counters.addBytes(lineBytes.length);
                }
                LOG.info("Rows copied: " + copyout.getHandledRowCount());
            } finally { // see to it that we do not leave the connection locked
                try {
                    if (copyout.isActive())
                        copyout.cancelCopy();
                } catch (SQLException sqle) {
                    LOG.info("Error cancelling copyout: " + sqle.toString());
                }
            }
            if (null != w) {
                try {
                    w.close();
                } catch (IOException ioe) {
                    LOG.info("Error closing HDFS stream: " + ioe.toString());
                }
            }
            counters.stopClock();
            LOG.info("Transferred " + counters.toString());

        } catch (SQLException ex) {
            LOG.error("Unable to get CopyOut", ex);
            w.close();
            throw new IOException(ex);
        } finally {
            w.close();
        }
    }

    @Override
    public boolean supportsStagingForExport() {
        return true;
    }
    // CHECKSTYLE:ON

    /** {@inheritDoc}. */
    @Override
    protected void applyExtraArguments(CommandLine cmdLine) {
        super.applyExtraArguments(cmdLine);

        if (cmdLine.hasOption(BOOLEAN_TRUE_STRING)) {
            String arg = cmdLine.getOptionValue(BOOLEAN_TRUE_STRING);
            LOG.info("Loaded TRUE encoding string " + arg);
            this.booleanTrueString = arg;
        }
        if (cmdLine.hasOption(BOOLEAN_FALSE_STRING)) {
            String arg = cmdLine.getOptionValue(BOOLEAN_FALSE_STRING);
            LOG.info("Loaded FALSE encoding string " + arg);
            this.booleanFalseString = arg;
        }
    }

    /** {@inheritDoc}. */
    @Override
    @SuppressWarnings("static-access")
    protected RelatedOptions getExtraOptions() {
        RelatedOptions extraOptions = super.getExtraOptions();

        extraOptions.addOption(OptionBuilder.withArgName("string").hasArg()
                .withDescription("String to encode TRUE value").withLongOpt(BOOLEAN_TRUE_STRING).create());

        extraOptions.addOption(OptionBuilder.withArgName("string").hasArg()
                .withDescription("String to encode FALSE value").withLongOpt(BOOLEAN_FALSE_STRING).create());

        return extraOptions;
    }

    public void exportTable(ExportJobContext context) throws IOException, ExportException {
        context.setConnManager(this);
        PostgreSQLCopyExportJob job = new PostgreSQLCopyExportJob(context, null, ExportInputFormat.class,
                NullOutputFormat.class);
        job.runExport();
    }
}
