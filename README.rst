pyrm
--------
``v0.1``

Package contains all the relevant scripts to pull manufacturing and QC data from all the Reagent Manufacturing process areas. 
The flagship functions of this package are the ``run_pipeline`` scripts that will read the files from Box or relevant locations, transform them into dataframes,
and push the dataframes to the CPPDA postgres database.

The scripts are structured in the following way