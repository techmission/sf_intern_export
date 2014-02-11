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

// Execute the intern sync tests.
run_tests();

function run_tests() {
  $picklist_field_info = get_picklist_field_info();
  foreach($fields as $tid_field_name => $field_info) {
    foreach($field_values as $field_value) {
      $field_values[$key] = get_picklist_name_by_synonym($field_value);
      $tid_field_value = get_picklist_tid_by_name($field_value[$key], $field_info['vid']);
    }
  }
}