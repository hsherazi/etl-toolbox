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

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;


/**
 * This is the main class for the fileloader application and it manages parsing command line arguments, configuring
 * the FileLoader, and calling the load method for the files to be processed.
 * <p/>
 * The specification format contains properties that define the audit and target database parameters and then an array
 * of mappings for each source file and its target.
 * <p/>
 * {
 *   "auditUrl": "jdbc:sqlserver://localhost:1433;databaseName=audit",
 *   "auditUser": "audit",
 *   "auditPassword": "demouser",
 *   "targetUrl": "jdbc:sqlserver://localhost:133;databaseName=source",
 *   "targetUser": "source",
 *   "targetPassword": "password",
 *   "batchThreshold": 1000,
 *   "mappings": [
 *     {
 *       "sourcePattern": "TEST_([0-9]+)_(Initial|New|Deleted|Update)\\.txt",
 *       "sourceId": 1,
 *       "dateGroup": 1,
 *       "dateFormat": "MMddyyyy",
 *       "typeGroup": 2,
 *       "parserSeparator": ",",
 *       "parserQuotechar": "\"",
 *       "parserEscape": "\\",
 *       "parserLine": 1,
 *       "parserStrictQuotes": false,
 *       "parserIgnoreLeadingWhiteSpace": true,
 *       "targetTable": "src_test",
 *       "targetColumns": [
 *         "test_id",
 *         "test_value"
 *       ]
 *     }
 *   ]
 * }
 */
public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);

    /**
     * Main entry point into the application.
     * @param args command line argunemtns
     */
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(new Option("s", "spec", true, "Source-to-target specification file"));
        options.addOption(new Option("d", "directory", true, "Source directory to load"));
        options.addOption(new Option("f", "file", true, "File to perform operation on"));
        options.addOption(new Option("r", "replace", false, "Replace previously loaded data"));
        options.addOption(new Option("t", "trace", true, "Trace records processed at specified interval"));

        CommandLineParser parser = new BasicParser();

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("spec") && (line.hasOption("directory") || line.hasOption("file"))) {
                FileLoader loader = new FileLoader(line);

                loader.init();

                if (line.hasOption("file")) {
                    loader.load(new File(line.getOptionValue("file")));
                } else if (line.hasOption("directory")) {
                    File directory = new File(line.getOptionValue("directory"));

                    if (directory.isDirectory()) {
                        File[] files = directory.listFiles();

                        for (File file : files) {
                            loader.load(file);
                        }
                    } else {
                        log.fatal(directory.getAbsolutePath() + " does not appear to be a directory.");
                    }
                }
            } else {
                usage();
            }
        } catch (ParseException e) {
            usage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Prints usage message to STDOUT.
     */
    private static void usage() {
        System.out.println("Usage: java fileloader.jar -s <specfile>.json (-d <directory> | -f file) [-r] [-t <interval>]");
        System.out.println("where options include:");
        System.out.println("    -s <specfile>   specification file in JSON format");
        System.out.println("    -d <directory>  directory containing data files to be loaded");
        System.out.println("    -f <file>       individual file to be loaded");
        System.out.println("    -r              replace data previously loaded from file with the same name");
        System.out.println("    -t <interval>   print trace output of records processed at specified interval");
    }

}
