**********************
Configuration Defaults
**********************

Tajo Master Configuration Defaults
====================================

============================  ==============================================================  ===========   ===============  
  Service Name                Config Property Name                                            Description   default address 
============================  ==============================================================  ===========   ===============  
Tajo Master Umbilical Rpc     tajo.master.umbilical-rpc.address                                             localhost:26001 
Tajo Master Client Rpc        tajo.master.client-rpc.address                                                localhost:26002 
Tajo Master Info Http         tajo.master.info-http.address                                                 0.0.0.0:26080
Tajo Resource Tracker Rpc     tajo.resource-tracker.rpc.address                                             localhost:26003
Tajo Catalog Client Rpc       tajo.catalog.client-rpc.address                                               localhost:26005
============================  ==============================================================  ===========   ===============  

====================================
Tajo Worker Configuration Defaults
====================================

============================  ==============================================================  ===========   ===============  
  Service Name                Config Property Name                                            Description   default address 
============================  ==============================================================  ===========   ===============  
Tajo Worker Peer Rpc          tajo.worker.peer-rpc.address                                                  0.0.0.0:28091   
Tajo Worker Client Rpc        tajo.worker.client-rpc.address                                                0.0.0.0:28092   
Tajo Worker Info Http         tajo.worker.info-http.address                                                 0.0.0.0:28080   
============================  ==============================================================  ===========   ===============  