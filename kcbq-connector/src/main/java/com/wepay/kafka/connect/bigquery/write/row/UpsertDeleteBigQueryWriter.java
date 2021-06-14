/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.BigQuerySinkConnector;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class UpsertDeleteBigQueryWriter extends AdaptiveBigQueryWriter {

  private final SchemaManager schemaManager;
  private static final Logger logger = LoggerFactory.getLogger(UpsertDeleteBigQueryWriter.class);
  private final boolean autoCreateTables;
  private final Map<TableId, TableId> intermediateToDestinationTables;

  /**
   * @param bigQuery Used to send write requests to BigQuery.
   * @param schemaManager Used to update BigQuery tables.
   * @param retry How many retries to make in the event of a 500/503 error.
   * @param retryWait How long to wait in between retries.
   * @param autoCreateTables Whether destination tables should be automatically created
   * @param intermediateToDestinationTables A mapping used to determine the destination table for
   *                                        given intermediate tables; used for create/update
   *                                        operations in order to propagate them to the destination
   *                                        table
   */
  public UpsertDeleteBigQueryWriter(BigQuery bigQuery,
                                    SchemaManager schemaManager,
                                    int retry,
                                    long retryWait,
                                    boolean autoCreateTables,
                                    Map<TableId, TableId> intermediateToDestinationTables) {
    // Hardcode autoCreateTables to true in the superclass so that intermediate tables will be
    // automatically created
    // The super class will handle all of the logic for writing to, creating, and updating
    // intermediate tables; this class will handle logic for creating/updating the destination table
    super(bigQuery, schemaManager.forIntermediateTables(), retry, retryWait, true);
    this.schemaManager = schemaManager;
    this.autoCreateTables = autoCreateTables;
    this.intermediateToDestinationTables = intermediateToDestinationTables;
  }

  @Override
  protected void attemptSchemaUpdate(PartitionedTableId tableId, List<SinkRecord> records) {
    // Update the intermediate table here...
    super.attemptSchemaUpdate(tableId, records);
    try {
      // ... and update the destination table here
      schemaManager.updateSchema(intermediateToDestinationTables.get(tableId.getBaseTableId()), records);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
          "Failed to update destination table schema for: " + tableId.getBaseTableId(), exception);
    }
  }

  @Override
  protected void attemptTableCreate(TableId tableId, List<SinkRecord> records) {
    // Create the intermediate table here...
    super.attemptTableCreate(tableId, records);
    if (autoCreateTables) {
      try {
        // ... and create or update the destination table here, if it doesn't already exist and auto
        // table creation is enabled
        logger.info("Destination Tables {}",intermediateToDestinationTables.get(tableId));
        logger.info("Table id {}",tableId);
        if(BigQuerySinkConnector.computeTableId.isEmpty()==true)
        {
          schemaManager.createOrUpdateTable(intermediateToDestinationTables.get(tableId), records);
        }
        else
        {

          TableId tb=TableId.of("wmt-edw-dev","US_SUPPLY_CHAIN_WTMS_NONCAT_TABLES",intermediateToDestinationTables.get(tableId).getTable());
          logger.info("vignesh intermediateTable from  attemptTableCreate {} {} {}",tb.getTable(),tb.getDataset(),tb.getProject());
          logger.info("vignesh from attemptTableCreate2 intermediateToDestinationTables keyset from {} destinationTableFor {}",tb);
          schemaManager.createOrUpdateTable(tb,records);
        }

      } catch (BigQueryException exception) {
        throw new BigQueryConnectException(
            "Failed to create table " + tableId, exception);
      }
    }
  }
}
