package net.deelam.vertx.rpc;

public final class OperationConsts {

  public static final String OPERATION_TYPE = "operationType";

  /// operation types values:  
  public static final String TYPE_INGESTER = "ingester";
  public static final String TYPE_POSTINGEST = "post-ingest";
  
  public static final String TYPE_PREREQUEST = "pre-request";
  public static final String TYPE_POSTREQUEST = "post-request";

  /// operation/job parameter keys:
  public static final String INPUT_URI = "inputUri";
  public static final String DATA_FORMAT = "dataFormat";
  public static final String OUTPUT_URI = "outputUri";
  public static final String PREV_JOBID = "prevJobId";
  
  /// DATA_FORMAT values:
  public static final String DATA_FORMAT_PARQUET = "parquet";
  public static final String DATA_FORMAT_LUCENE = "lucene";

  public static final String INGESTER_WORKER = "ingesterWorker";



}
