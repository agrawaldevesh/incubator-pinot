/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.readers;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.core.data.GenericRow;


/**
 * Record reader for CSV file.
 */
public class CSVRecordReader implements RecordReader {
  private final File _dataFile;
  private final Schema _schema;
  private final CSVFormat _format;
  private final char _multiValueDelimiter;

  private CSVParser _parser;
  private Iterator<CSVRecord> _iterator;

  public CSVRecordReader(File dataFile, Schema schema, CSVRecordReaderConfig config)
      throws IOException {
    _dataFile = dataFile;
    _schema = schema;

    if (config == null) {
      _format = CSVFormat.DEFAULT.withDelimiter(CSVRecordReaderConfig.DEFAULT_DELIMITER).withHeader();
      _multiValueDelimiter = CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER;
    } else {
      CSVFormat format;
      String formatString = config.getFileFormat();
      if (formatString == null) {
        format = CSVFormat.DEFAULT;
      } else {
        switch (formatString.toUpperCase()) {
          case "EXCEL":
            format = CSVFormat.EXCEL;
            break;
          case "MYSQL":
            format = CSVFormat.MYSQL;
            break;
          case "RFC4180":
            format = CSVFormat.RFC4180;
            break;
          case "TDF":
            format = CSVFormat.TDF;
            break;
          default:
            format = CSVFormat.DEFAULT;
            break;
        }
      }
      char delimiter = config.getDelimiter();
      format = format.withDelimiter(delimiter);
      String csvHeader = config.getHeader();
      if (csvHeader == null) {
        format = format.withHeader();
      } else {
        format = format.withHeader(StringUtils.split(csvHeader, delimiter));
      }
      _format = format;
      _multiValueDelimiter = config.getMultiValueDelimiter();
    }

    init();
  }

  private void init()
      throws IOException {
    _parser = _format.parse(RecordReaderUtils.getFileReader(_dataFile));
    _iterator = _parser.iterator();
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    CSVRecord record = _iterator.next();

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {
        // For TIME field spec, if incoming and outgoing time column name are the same, use incoming field spec to fill
        // the row; otherwise, fill both incoming and outgoing time column
        TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
        String incomingTimeColumnName = timeFieldSpec.getIncomingTimeColumnName();
        String incomingTimeToken = record.isSet(incomingTimeColumnName) ? record.get(incomingTimeColumnName) : null;
        if (incomingTimeToken != null) {
          // Treat incoming time column as a single-valued dimension column
          TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
          reuse.putField(incomingTimeColumnName, RecordReaderUtils.convertToDataType(incomingTimeToken,
              new DimensionFieldSpec(incomingTimeColumnName, incomingGranularitySpec.getDataType(), true)));
        }
        // Fill outgoing time column if its name is not the same as incoming time column
        String outgoingTimeColumnName = timeFieldSpec.getOutgoingTimeColumnName();
        if (!outgoingTimeColumnName.equals(incomingTimeColumnName)) {
          String outgoingTimeToken = record.isSet(outgoingTimeColumnName) ? record.get(outgoingTimeColumnName) : null;
          if (outgoingTimeToken != null) {
            reuse.putField(outgoingTimeColumnName,
                RecordReaderUtils.convertToDataType(outgoingTimeToken, timeFieldSpec));
          }
        }
      } else {
        String fieldName = fieldSpec.getName();
        String token = record.isSet(fieldName) ? record.get(fieldName) : null;
        if (fieldSpec.isSingleValueField()) {
          reuse.putField(fieldName, RecordReaderUtils.convertToDataType(token, fieldSpec));
        } else {
          String[] tokens = token != null ? StringUtils.split(token, _multiValueDelimiter) : null;
          reuse.putField(fieldName, RecordReaderUtils.convertToDataTypeArray(tokens, fieldSpec));
        }
      }
    }

    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _parser.close();
    init();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
    _parser.close();
  }
}
