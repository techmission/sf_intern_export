<?php

/**
 * Export the intern records into the social graph denormalized table.
 */
 
/* Includes */
require_once 'sf_intern_export_lib.inc';

/* Main script. */

// Go down a directory to include Drupal framework, for DB layer functions.
chdir('..');
require_once './includes/bootstrap.inc'; // Drupal (for database & SF functions)

// Drupal bootstrap - to use database functions and Salesforce module 
drupal_bootstrap(DRUPAL_BOOTSTRAP_FULL);

// Switch to the social graph database.
db_set_active('techmi5_socgraph');

// Execute the intern sync cron.
run_cron();

/* Function definitions. */

// Main cron function
function run_cron() {
  $sf = salesforce_api_connect();

  if(!is_object($sf)) {
    exit_connection_failed();
  }
  
  $cv_objects = query_sf_objects($sf);
  print_r($cv_objects['Attachment']);
  
  if((isset($cv_objects[OBJECT_TYPE_LEAD]) && count($cv_objects[OBJECT_TYPE_LEAD]) > 0) || (isset($cv_objects[OBJECT_TYPE_CONTACT]) && count($cv_objects[OBJECT_TYPE_CONTACT]) > 0)) {
    //$result = write_cv_objects($cv_objects);
  }
}