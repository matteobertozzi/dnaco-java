package tech.dnaco.storage.net;

import com.gullivernet.server.http.HttpMethod;
import com.gullivernet.server.http.rest.RestRequestHandler;

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

public class EntityStorageHttpHandler implements RestRequestHandler {
  // ================================================================================
  //  Modification Handlers
  // ================================================================================
  @UriMapping(uri = "/v0/entity/insert", method = HttpMethod.POST)
  public TransactionStatusResponse insertEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.insertEntity(request);
  }

  @UriMapping(uri = "/v0/entity/upsert", method = HttpMethod.POST)
  public TransactionStatusResponse upsertEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.upsertEntity(request);
  }

  @UriMapping(uri = "/v0/entity/update", method = HttpMethod.POST)
  public TransactionStatusResponse updateEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.updateEntity(request);
  }

  @UriMapping(uri = "/v0/entity/update-filtered", method = HttpMethod.POST)
  public TransactionStatusResponse updateEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    return EntityStorage.INSTANCE.updateEntityWithFilter(request);
  }

  @UriMapping(uri = "/v0/entity/delete", method = HttpMethod.POST)
  public TransactionStatusResponse deleteEntity(final ModificationRequest request) throws Exception {
    return EntityStorage.INSTANCE.deleteEntity(request);
  }

  @UriMapping(uri = "/v0/entity/delete-filtered", method = HttpMethod.POST)
  public TransactionStatusResponse deleteEntityWithFilter(final ModificationWithFilterRequest request) throws Exception {
    return EntityStorage.INSTANCE.deleteEntityWithFilter(request);
  }

  @UriMapping(uri = "/v0/commit", method = HttpMethod.POST)
  public TransactionStatusResponse commit(final TransactionCommitRequest request) throws Exception {
    return EntityStorage.INSTANCE.commit(request);
  }

  // ================================================================================
  //  Get/Scan Handlers
  // ================================================================================
  @UriMapping(uri = "/v0/entity/get", method = HttpMethod.POST)
  public Scanner getEntity(final ScanRequest request) throws Exception {
    return EntityStorage.INSTANCE.getEntity(request);
  }

  @UriMapping(uri = "/v0/entity/scan", method = HttpMethod.POST)
  public Scanner scanEntity(final ScanRequest request) throws Exception {
    return EntityStorage.INSTANCE.scanEntity(request);
  }

  @UriMapping(uri = "/v0/entity/scan-all", method = HttpMethod.POST)
  public Scanner scanAll(final ScanRequest request) throws Exception {
    return EntityStorage.INSTANCE.scanAll(request);
  }

  @UriMapping(uri = "/v0/entity/scan-next", method = HttpMethod.POST)
  public ScanResult scanNext(final ScanNextRequest request) {
    return EntityStorage.INSTANCE.scanNext(request);
  }

  @UriMapping(uri = "/v0/entity/count", method = HttpMethod.POST)
  public CountResult countEntity(final CountRequest request) throws Exception {
    return EntityStorage.INSTANCE.countEntity(request);
  }
}
