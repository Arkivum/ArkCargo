ArkCargo
========

Is a collection of tools to make transfering large volumes of data 
into the Arkivum service simplier and with consistency checking 
throughout the process.

This is a collection of scripts that support the bulk ingest and 
'loose' structuring of datasets from unstructured storage. To 
make transfering large volumes of data into the Arkivum service 
simplier and with consistency checking throughout the process. 

Written in python and compatible with older deployments such as 
v2.6 which is found on EMC Isilon appliances.

The toolkit is intended to have multiple scripts that allow for 
the creation of customisable pipeline.

The project includes unittesting (based on unittest), to ensure the
intended behaviour is maintained.


mkcargo.py
=========

primary modes
--full    (as per md5deep),
--incr    compares two snapshots and creates a cargo for any 
          new/modified files
--files   processes a file(s) that contain explicit paths to files or 
          directories, where a directory is supplied then all its 
          child objects will be included. 

qualifiers
--rework  accepts file with explicit absolute paths that need to have 
          a cargo created for, probably due to a previous failure
--stats   stats are gathered but no cargos produced, useful if you 
          created cargos with md5deep.py (equivolent to a 'full') 
          and now want to use it as the basis of incr runs.

It also produces a lot more information to support the creation of 
the snapshot views (synthetic fulls) and also produces file count 
and size stats. 
 
