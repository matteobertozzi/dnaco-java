package tech.dnaco.storage.net;

import tech.dnaco.net.rpc.DnacoRpcHandler;
import tech.dnaco.storage.net.models.CountRequest;
import tech.dnaco.storage.net.models.CountResult;
import tech.dnaco.storage.net.models.ModificationRequest;
import tech.dnaco.storage.net.models.ModificationWithFilterRequest;
import tech.dnaco.storage.net.models.ScanNextRequest;
import tech.dnaco.storage.net.models.ScanRequest;
import tech.dnaco.storage.net.models.ScanResult;
import tech.dnaco.storage.net.models.Scanner;
import tech.dnaco.storage.net.models.TransactionCommitRequest;
import tech.dnaco.storage.net.models.TransactionStatusResponse;

public class EntityStorageRpcHandler implements DnacoRpcHandler {
// ================================================================================
  //  Modification Handlers
  // ================================================================================
  @RpcRequest("/v0/entity/insert")
  public TransactionStatusResponse insertEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.insertEntity(request);
  }

  @RpcRequest("/v0/entity/upsert")
  public TransactionStatusResponse upsertEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.upsertEntity(request);
  }

  @RpcRequest("/v0/entity/update")
  public TransactionStatusResponse updateEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.updateEntity(request);
  }

  @RpcRequest("/v0/entity/update-filtered")
  public TransactionStatusResponse updateEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    return EntityStorage.INSTANCE.updateEntityWithFilter(request);
  }

  @RpcRequest("/v0/entity/delete")
  public TransactionStatusResponse deleteEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.deleteEntity(request);
  }

  @RpcRequest("/v0/entity/delete-filtered")
  public TransactionStatusResponse deleteEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    return EntityStorage.INSTANCE.deleteEntityWithFilter(request);
  }

  @RpcRequest("/v0/commit")
  public TransactionStatusResponse commit(final TransactionCommitRequest request) throws Exception {
    return EntityStorage.INSTANCE.commit(request);
  }

  // ================================================================================
  //  Get/Scan Handlers
  // ================================================================================
  @RpcRequest("/v0/entity/get")
  public Scanner getEntity(final ScanRequest request) throws Exception {
    return EntityStorage.INSTANCE.getEntity(request);
  }

  @RpcRequest("/v0/entity/scan")
  public Scanner scanEntity(final ScanRequest request) throws Exception {
    return EntityStorage.INSTANCE.scanEntity(request);
  }

  @RpcRequest("/v0/entity/scan-all")
  public Scanner scanAll(final ScanRequest request) throws Exception {
    return EntityStorage.INSTANCE.scanAll(request);
  }

  @RpcRequest("/v0/entity/scan-next")
  public ScanResult scanNext(final ScanNextRequest request) throws Exception {
    return EntityStorage.INSTANCE.scanNext(request);
  }

  @RpcRequest("/v0/entity/count")
  public CountResult countEntity(final CountRequest request) throws Exception {
    return EntityStorage.INSTANCE.countEntity(request);
  }
}
