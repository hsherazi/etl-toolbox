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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class is used to manage loading of delimited source files into target database tables as specified in
 * the JSON formatted file specification file.
 */
public class FileLoader {
    private static final Logger log = LogManager.getLogger(FileLoader.class);
    private CommandLine commandLine;
    private ArrayList<FileSpecification> specs = new ArrayList<FileSpecification>();
    private boolean replaceExisting;
    private long trace = 0l;

    public FileLoader(CommandLine commandLine) {
        this.commandLine = commandLine;
    }

    /**
     * Initializes this class, parsing command line arguments and specification file.
     *
     * @throws ParseException
     * @throws IOException
     */
    public void init() throws ParseException, IOException {
        if (commandLine.hasOption("trace")) {
            trace = Long.parseLong(commandLine.getOptionValue("trace"));
        }
        replaceExisting = commandLine.hasOption("replace");

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> root = mapper.readValue(new File(commandLine.getOptionValue("spec")), Map.class);

        String auditUrl = (String) root.get("auditUrl");
        String auditUser = (String) root.get("auditUser");
        String auditPassword = (String) root.get("auditPassword");
        DataSource auditDs = new DriverManagerDataSource(auditUrl, auditUser, auditPassword);

        String targetUrl = (String) root.get("targetUrl");
        String targetUser = (String) root.get("targetUser");
        String targetPassword = (String) root.get("targetPassword");
        DataSource targetDs = new DriverManagerDataSource(targetUrl, targetUser, targetPassword);

        int batchThreshold = (Integer) root.get("batchThreshold");
        List<Object> mappings = (List<Object>) root.get("mappings");
        for (Object mapping : mappings) {
            specs.add(new FileSpecification((Map<String, Object>) mapping, auditDs, targetDs, batchThreshold,
                    replaceExisting, trace));
        }
    }

    /**
     * Attempts to load specified file, if it matches file name pattern from one or more file specifications.
     *
     * @param file file to load
     * @throws IOException              on error reading file
     * @throws java.text.ParseException on error parsing line in file
     */
    public void load(File file) {
        for (FileSpecification spec : specs) {
            try {
                if (spec.match(file)) {
                    spec.load(file);
                }
            } catch (java.text.ParseException e) {
                log.error("\tThe following parsing error occurred while attempting to load " + file.getName(), e);
            } catch (IOException e) {
                log.error("\tThe following IO error occurred while attempting to load " + file.getName(), e);
            } catch (RuntimeException e) {
                log.error("\tThe following error occurred while attempting to load " + file.getName(), e);
            }
        }
    }
}
