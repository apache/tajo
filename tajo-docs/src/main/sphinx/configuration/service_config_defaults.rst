**************************************
Cluster Service Configuration Defaults 
**************************************

Tajo Master Configuration Defaults
====================================

============================  ====================================  ===============  =================================================================
  Service Name                Config Property Name                  default address  Description
============================  ====================================  ===============  =================================================================
Tajo Master Umbilical Rpc     tajo.master.umbilical-rpc.address     localhost:26001  TajoMaster binding address between master and workers.
Tajo Master Client Rpc        tajo.master.client-rpc.address        localhost:26002  TajoMaster binding address between master and remote clients.
Tajo Master Info Http         tajo.master.info-http.address         0.0.0.0:26080    TajoMaster binding address between master and web browser.
Tajo Resource Tracker Rpc     tajo.resource-tracker.rpc.address     0.0.0.0:26003    TajoMaster binding address between master and workers.
Tajo Catalog Client Rpc       tajo.catalog.client-rpc.address       0.0.0.0:26005    CatalogServer binding address between catalog server and workers.
============================  ====================================  ===============  =================================================================

====================================
Tajo Worker Configuration Defaults
====================================

============================  ====================================  ===============  =================================================================
  Service Name                Config Property Name                  default address  Description
============================  ====================================  ===============  =================================================================
Tajo Worker Peer Rpc          tajo.worker.peer-rpc.address          0.0.0.0:28091    TajoWorker binding address between worker and master.
Tajo Worker Client Rpc        tajo.worker.client-rpc.address        0.0.0.0:28092    TajoWorker binding address between worker and remote clients.
Tajo Worker Info Http         tajo.worker.info-http.address         0.0.0.0:28080    TajoWorker binding address between master and web browser.
============================  ====================================  ===============  =================================================================