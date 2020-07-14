package com.datalake.pipeline;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

public class PubsubToBQStreamPipeline {

  protected static final BigQuery bigquery = getBigqueryCredentialsObject();
  protected static final Logger logger = LoggerFactory.getLogger(PubsubToBQStreamPipeline.class);
  protected static final Map<String, String> bqdataset = loadDatasetMapFromBQ();
  protected static final Map<String, Map<String, Map<String, String>>> globalbqschemadata = loadSchemaMapFromBQ();

  public static void main(String[] args) {

    try {
      
      /*
       * Initialize Pipeline Configurations
       */
      DataflowPipelineOptions options = PipelineOptionsFactory
          .as(DataflowPipelineOptions.class);
      options.setRunner(DirectRunner.class);
      options.setProject("");
      options.setStreaming(true);
      options.setTempLocation(""); 
      options.setStagingLocation("");
      options.setRegion("");
      options.setMaxNumWorkers(1);
      options.setWorkerMachineType("n1-standard-1");

      Pipeline pipeline = Pipeline.create(options);
      
      /*
       * Read Messages from PubSub Subscription
       */
      PCollection<PubsubMessage> pubsubMessage = pipeline.apply(
          "ReadMessages",
          PubsubIO.readMessagesWithAttributes().fromSubscription(""));

      /*
       * Convert JSON String to BQ Table Row Objects.
       * Pubsub messages also have attributes containing table name of BQ.
       * 
       * Read those attributes values and convert the json string to table row accordingly.
       * objects based out of their schema in BQ.
       * 
       * Make sure tables and their schemas are already present in BQ
       */
      
      WriteResult writeResult;
      writeResult = pubsubMessage.apply(
          "WriteToBigQuery",
          BigQueryIO.<PubsubMessage>write()
          .withExtendedErrorInfo()
          .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
          .to(
              input
              -> getTableDestination(
                  input,
                  "PROJECT_NAME", globalbqschemadata, bqdataset
                  ))
          .withFormatFunction(
              (PubsubMessage msg) -> DynamicTableRow(new String(msg.getPayload()),
                  msg.getAttribute("tablename"), globalbqschemadata, msg.getAttribute("clientkey"), 
                  bqdataset, msg.getAttribute("blankschemaname")))
          .withCreateDisposition(CreateDisposition.CREATE_NEVER)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND)
          .withFailedInsertRetryPolicy(InsertRetryPolicy.neverRetry())
          );

      /*
       * Handle all the failed records and push it to BQ failed table.
       */
      writeResult
      .getFailedInsertsWithErr()
      .apply("FormatFailedInserts", ParDo.of(new PubsubToBQStreamPipeline.FailedInserts()))
      .apply(
          "WriteFailedInsertsToDeadletter",
          BigQueryIO.writeTableRows()
          .to("")
          .withTimePartitioning(new TimePartitioning().setType("DAY"))
          .withCreateDisposition(CreateDisposition.CREATE_NEVER)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND));

      /*
       * Push converted table row objects to BQ tables.
       */
      pubsubMessage.apply(ParDo.of(new PubsubMsgToTableRow()))
      .apply(
          "Dump PubsubMessages",
          BigQueryIO.writeTableRows()
          .to("")
          .withTimePartitioning(new TimePartitioning().setType("DAY"))
          .withCreateDisposition(CreateDisposition.CREATE_NEVER)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND));

      pipeline.run().waitUntilFinish();
    } catch (Exception e) {
      logger.error(e.toString());
    }
  }

  
  /*
   * Fetch BQ table schema and convert them to BQ table row objects.
   */
  static TableRow DynamicTableRow(String json, String tablename, Map<String, Map<String, Map<String, String>>> globalschemadata, String clientkey, Map<String, String> datasetnames, String blankschemaname) {
    TableRow row = new TableRow();
    try {
      JSONObject json1 = new JSONObject(json);
      if (globalschemadata.containsKey(blankschemaname)) {
        Map<String, Map<String, String>> schemadata = globalschemadata.get(blankschemaname);

        if (schemadata.containsKey(tablename) && datasetnames.containsKey(clientkey)) {
          logger.info("schema found -" + tablename);
          row = mapToTableRow(schemadata.get(tablename), json1);
        } else {
          row.set("message", json1.toString());
          String bqTime = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS").print(new DateTime());
          row.set("time", bqTime);
          row.set("tablename", tablename);
          row.set("datasetname", clientkey);
          logger.info("schema not found for table : {}", tablename);
        }
      } else {
        row.set("message", json1.toString());
        String bqTime = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS").print(new DateTime());
        row.set("time", bqTime);
        row.set("tablename", tablename);
        row.set("datasetname", clientkey);
        logger.info("Blank schema not found for table : {}", tablename);
      }
    } catch (Exception e) { 
      logger.error("Exception : {}", e);
    }
    return row;
  }

  /*
   * Convert JSON to Table Row
   */
  static TableRow convertJsonToTableRow(String json, String dataset, String tablename) {
    TableRow row = new TableRow();
    Map<String, String> newshema = new HashMap<>();
    try {
      JSONObject json1 = new JSONObject(json);
      logger.info(tablename);
      logger.info(json);
      TableId tableId = TableId.of("", dataset, tablename.toLowerCase());
      Table table = bigquery.getTable(tableId);
      FieldList newfieldList = table.getDefinition().getSchema().getFields();
      for (Field field : newfieldList) {
        newshema.put(field.getName().trim(), field.getType().name());
      }
      logger.info("schema" + newshema.toString());
      row = mapToTableRow(newshema, json1);
      logger.info("converted row" + row.toString());
    } catch (Exception e) {
      logger.error("Exception : {}", e);
    }
    return row;
  }

  private static class PubsubMsgToTableRow extends DoFn<PubsubMessage, TableRow> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      PubsubMessage msg = c.element();
      String payload = new String(msg.getPayload());

      TableRow row = new TableRow();
      row.set("payload", payload);
      String bqTime = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS").print(new DateTime());
      row.set("time", bqTime);
      row.set("tablename", msg.getAttribute("tablename"));
      row.set("datasetname", msg.getAttribute("clientkey"));
      c.output(row);
    }
  }

  /*
   * Failed inserts records handling
   */
  private static class FailedInserts extends DoFn<BigQueryInsertError, TableRow> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      BigQueryInsertError input = c.element();
      try {
        String insertError = input.getError().toPrettyString();
        String tableRowStr = input.getRow().toString();
        String tablename = input.getTable().getTableId();
        String datasetname = input.getTable().getDatasetId();

        TableRow resp = new TableRow();

        resp.put("tablename", tablename);
        resp.put("errorstr", insertError);
        resp.put("rowdata", tableRowStr);
        resp.put("datasetname", datasetname);
        c.output(resp);
      } catch (Exception e) {
        logger.error(e.toString());
      }
    }
  }

  /*
   * Create Table Destination object by reading attributes from pubsub
   */
  static TableDestination getTableDestination(
      ValueInSingleWindow<PubsubMessage> value,
      String outputProject, Map<String, Map<String, Map<String, String>>> globalschemadata, Map<String, String> datasetnames) {
    try {
      PubsubMessage message = value.getValue();
      TableDestination destination;
      if (globalschemadata.containsKey(message.getAttribute("blankschemaname"))) {

        Map<String, Map<String, String>> schemadata = globalschemadata.get(message.getAttribute("blankschemaname"));
        if (message != null) {
          logger.info("getTableDestination -Table Name-" + message.getAttribute("tablename"));

          if (schemadata.containsKey(message.getAttribute("tablename")) && datasetnames.containsKey(message.getAttribute("clientkey"))) {
            destination
            = new TableDestination(
                String.format(
                    "%s:%s.%s",
                    outputProject, message.getAttribute("clientkey"), message.getAttribute("tablename")), //getTableName(new String(message.getPayload()))
                null);
          } else {
            destination
            = new TableDestination(
                String.format(
                    "%s:%s.%s",
                    outputProject, "", ""), //getTableName(new String(message.getPayload()))
                null);
          }
        } else {
          logger.info("getTableDestination -Cannot retrieve the dynamic table destination of an null message!");
          throw new RuntimeException(
              "Cannot retrieve the dynamic table destination of an null message!");
        }

      } else {
        destination
        = new TableDestination(
            String.format(
                "%s:%s.%s",
                outputProject, "", ""), //getTableName(new String(message.getPayload()))
            null);
      }

      return destination;
    } catch (Exception e) {
      logger.info("getTableDestination Error-" + e.toString());
      logger.error(e.toString());
      return null;
    }

  }
  static Table table;
  static Map<String, String> newshema = new HashMap<String, String>();

  public static BigQuery getBigqueryCredentialsObject() {
    GoogleCredentials credentials;
    BigQuery bigqueryObj = null;
    try ( InputStream serviceAccountStream = PubsubToBQStreamPipeline.class.getResourceAsStream("")) {
      credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);

      bigqueryObj
      = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

    } catch (IOException e) {
      logger.error("Exception : {}", e);
    }
    return bigqueryObj;
  }

  public static Map<String, Map<String, Map<String, String>>> loadSchemaMapFromBQ() {
    Map<String, Map<String, Map<String, String>>> globalmap = new HashMap<String, Map<String, Map<String, String>>>();

    try {
      String[] datasetlist = {"BlankDataset", "BANQBlankDataset"};
      for (int i = 0; i < datasetlist.length; i++) {
        Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
        Page<Table> tables = bigquery.listTables(datasetlist[i], BigQuery.TableListOption.pageSize(999));
        for (Table table1 : tables.iterateAll()) {
          Table table2 = bigquery.getTable(table1.getTableId());
          Map<String, String> obj = new HashMap<String, String>();
          FieldList newfieldList;
          newfieldList = table2.getDefinition().getSchema().getFields();
          for (Field field : newfieldList) {
            obj.put(field.getName().trim(), field.getType().name());
          }
          map.put(table1.getTableId().getTable(), obj);
        }
        globalmap.put(datasetlist[i], map);
      }
    } catch (Exception e) {
      logger.info("convertJsonToTableRow-" + e.toString());
    }
    return globalmap;
  }

  public static Map<String, String> loadDatasetMapFromBQ() {
    Map<String, String> map = new HashMap<String, String>();
    try {

      Page<Dataset> datasets = bigquery.listDatasets("", BigQuery.DatasetListOption.pageSize(999));
      for (Dataset dataset : datasets.iterateAll()) {
        map.put(dataset.getDatasetId().getDataset().toString(), dataset.getDatasetId().toString());
      }
      logger.info("Dataset read from bigquery-" + map.toString());
    } catch (Exception e) {
      logger.info("Dataset name read error-" + e.toString());
    }
    return map;
  }

  public static Map<String, String> getSchemaFromBQ(String tablename) {
    Map<String, String> obj = new HashMap<String, String>();

    TableId tableId = TableId.of("", "BlankDataset", tablename.toLowerCase());
    Table table = bigquery.getTable(tableId);

    FieldList newfieldList = table.getDefinition().getSchema().getFields();
    for (Field field : newfieldList) {
      obj.put(field.getName().trim(), field.getType().name());
    }
    return obj;
  }

  static TableRow convertJsonToTableRowold(String json, String tablename) {
    TableRow row = new TableRow();
    try {
      JSONObject json1 = new JSONObject(json);

      TableId tableId = TableId.of("", "", tablename);
      if (table == null) {
        table = bigquery.getTable(tableId);
        newshema = new HashMap<String, String>();
        FieldList newfieldList = table.getDefinition().getSchema().getFields();
        for (Field field : newfieldList) {
          newshema.put(field.getName().trim(), field.getType().name());
        }
      } else {
        if (!table.getTableId().equals(tableId)) {
          table = bigquery.getTable(tableId);
          newshema = new HashMap<String, String>();
          FieldList newfieldList = table.getDefinition().getSchema().getFields();
          for (Field field : newfieldList) {
            newshema.put(field.getName().trim(), field.getType().name());
          }
        }
      }
      row = mapToTableRow(newshema, json1);
      return row;
    } catch (Exception e) {
      logger.info("convertJsonToTableRow-" + e.toString());
      return row;
    }
  }

  static TableRow convertJsonToTableRow(String json) {
    TableRow row;
    try ( InputStream inputStream
        = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }
    return row;
  }

  public static TableRow mapToTableRow(Map<String, String> map, JSONObject jsonObject) {
    TableRow row = new TableRow();
    try {

      for (Map.Entry<String, String> entry : map.entrySet()) {

        if (!jsonObject.isNull(entry.getKey())) {
          if (entry.getValue().equals("STRING")) {
            if (jsonObject.get(entry.getKey()) != null) {
              row.put(entry.getKey(), String.valueOf(jsonObject.get(entry.getKey())));
            } else {
              row.put(entry.getKey(), "");
            }
          } else if (entry.getValue().equals("TIMESTAMP")) {
            if (jsonObject.get(entry.getKey()) != null) {
              String str = (jsonObject != null && !jsonObject.isNull(entry.getKey())) ? (String.valueOf(jsonObject.get(entry.getKey()))) : "";
              DateTime jodatime = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(str);
              DateTime time = jodatime.withZone(DateTimeZone.forID("UTC"));
              String bqTime = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS").print(time);
              row.put(entry.getKey(), bqTime);
            } else {
              row.put(entry.getKey(), null);
            }
          } else if (entry.getValue().equals("DATETIME")) {
            if ("datenull".equals(jsonObject.get(entry.getKey()).toString())) {
              row.put(entry.getKey(), null);
            } else {
              if (jsonObject.get(entry.getKey()) != null) {
                String str = (jsonObject != null && !jsonObject.isNull(entry.getKey())) ? (String.valueOf(jsonObject.get(entry.getKey()))) : "";
                DateTime jodatime = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(str);
                DateTime time = jodatime.withZone(DateTimeZone.forID("UTC"));
                String bqTime = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:ss.SSS").print(time);
                row.put(entry.getKey(), bqTime);
              } else {
                row.put(entry.getKey(), null);
              }
            }
          } else if (entry.getValue().equals("INTEGER")) {
            if (jsonObject.get(entry.getKey()) != null) {
              row.put(entry.getKey(), jsonObject.get(entry.getKey()));
            } else {
              row.put(entry.getKey(), null);
            }
          } else if (entry.getValue().equals("BOOLEAN")) {
            if (jsonObject.get(entry.getKey()) != null) {
              switch (jsonObject.get(entry.getKey()).toString()) {
              case "1":
                row.put(entry.getKey(), true);
                break;
              case "0":
                row.put(entry.getKey(), false);
                break;
              default:
                row.put(entry.getKey(), jsonObject.get(entry.getKey()));
                break;
              }
            } else {
              row.put(entry.getKey(), false);
            }
          } else if (entry.getValue().equals("DATE")) {
            if ("datenull".equals(jsonObject.get(entry.getKey()).toString())) {
              row.put(entry.getKey(), null);
            } else {
              if (jsonObject.get(entry.getKey()) != null) {
                row.put(entry.getKey(), jsonObject.get(entry.getKey()).toString().replaceAll("/", "-"));
              } else {
                row.put(entry.getKey(), null);
              }
            }
          } else if (entry.getValue().equals("FLOAT")) {
            if (jsonObject.get(entry.getKey()) != null) {
              row.put(entry.getKey(), jsonObject.get(entry.getKey()));
            } else {
              row.put(entry.getKey(), null);
            }
          } else {
            row.put(entry.getKey(), jsonObject.get(entry.getKey()));
          }
        } else {
          row.put(entry.getKey(), null);
        }
      }
      logger.info("mapToTableRow-row converted");
      return row;
    } catch (Exception e) {
      logger.info("mapToTableRow-got error" + e.toString());
      return row;
    }
  }
}
