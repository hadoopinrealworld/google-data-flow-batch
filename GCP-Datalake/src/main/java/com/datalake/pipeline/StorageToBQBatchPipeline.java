package com.datalake.pipeline;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.api.services.bigquery.model.TableRow;

public class StorageToBQBatchPipeline {

  public static void main(String[] args) {

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
     * Read files from GCS buckets
     */
    PCollection<ReadableFile> data = pipeline
        .apply(FileIO.match().filepattern("gs://dump_*"))
        .apply(FileIO.readMatches());

    /*
     * Create Tuple Tag to process passed as well as failed records while parsing in ParDo functions
     */
    final TupleTag<KV<String, String>> mapSuccessTag = new TupleTag<KV<String, String>>() {
      private static final long serialVersionUID = 1L;
    };
    final TupleTag<KV<String, String>> mapFailedTag = new TupleTag<KV<String, String>>() {
      private static final long serialVersionUID = 1L;
    };

    PCollectionTuple mapTupleObj = data.apply(ParDo.of(new MapTransformation(mapSuccessTag, mapFailedTag))
        .withOutputTags(mapSuccessTag, TupleTagList.of(mapFailedTag)));

    PCollection<KV<String, String>> map = mapTupleObj.get(mapSuccessTag).setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    
    /*
     * Create Tuple Tag to process passed as well as failed records while parsing in ParDo functions.
     * Failed Tags can be pushed in failed table.
     */
    final TupleTag<TableRow> tableRowSuccessTag = new TupleTag<TableRow>() {
      private static final long serialVersionUID = 1L;
    };
    final TupleTag<TableRow> tableRowFailedTag = new TupleTag<TableRow>() {
      private static final long serialVersionUID = 1L;
    };

    PCollectionTuple tableRowTupleObj = map.apply(ParDo.of(new TableRowTransformation(tableRowSuccessTag, tableRowFailedTag))
        .withOutputTags(tableRowSuccessTag, TupleTagList.of(tableRowFailedTag)));

    PCollection<TableRow> rowObj = tableRowTupleObj.get(tableRowSuccessTag).setCoder(NullableCoder.of(TableRowJsonCoder.of()));

    /*
     * Push records to BQ.
     */
    rowObj.apply(BigQueryIO.writeTableRows()
        .to("options.getOutputTable()")
        .ignoreUnknownValues() 
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND) 
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
       
    pipeline.run().waitUntilFinish();
  }

  /*
   * Convert File to KV Object
   */
  private static class MapTransformation extends DoFn<FileIO.ReadableFile,KV<String, String>> {
    static final long serialVersionUID = 1L;
    TupleTag<KV<String, String>> successTag;
    TupleTag<KV<String, String>> failedTag;

    public MapTransformation(TupleTag<KV<String, String>> successTag, TupleTag<KV<String, String>> failedTag) {
      this.successTag = successTag;
      this.failedTag = failedTag;
    }
    @ProcessElement
    public void processElement(ProcessContext c) {
      FileIO.ReadableFile f = c.element();
      String fileName = f.getMetadata().resourceId().toString();
      String fileData = null;
      try {
        fileData = f.readFullyAsUTF8String();
        c.output(successTag,KV.of(fileName, fileData));
      } catch (Exception e) {
        c.output(failedTag, KV.of(e.getMessage(), fileName));
      }
    }
  }

  /*
   * Convert KV to Table Row
   */
  private static class TableRowTransformation extends DoFn<KV<String, String>, TableRow> {
    static final long serialVersionUID = 1L;
    TupleTag<TableRow> successTag;
    TupleTag<TableRow> failedTag;

    public TableRowTransformation(TupleTag<TableRow> successTag, TupleTag<TableRow> failedTag) {
      this.successTag = successTag;
      this.failedTag = failedTag;
    }
    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        KV<String, String> kvObj = c.element();
        TableRow tableRow = new TableRow();
        tableRow.set(kvObj.getKey(), kvObj.getValue());

        c.output(successTag,tableRow);
      } catch (Exception e) {
        TableRow tableRow = new TableRow();
        c.output(failedTag, tableRow);
      }
    }
  }
}
