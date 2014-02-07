<?php

/**
 * Export the intern records into the social graph denormalized table.
 */

// Constants
define('OBJECT_TYPE_LEAD', 1);
define('OBJECT_TYPE_CONTACT', 2);
 
// Go down a directory to include Drupal framework, for DB layer functions.
chdir('..');
require_once './includes/bootstrap.inc'; // Drupal (for database & SF functions)

// Drupal bootstrap - to use database functions and Salesforce module 
drupal_bootstrap(DRUPAL_BOOTSTRAP_FULL);

// Switch to the social graph database.
db_set_active('techmi5_socgraph');

// Execute the intern sync cron.
run_cron();

// Main cron function
function run_cron() {
  $sf = salesforce_api_connect();

  if(!is_object($sf)) {
    exit_connection_failed();
  }
  
  $leads = query_sf_leads($sf);
  print_r($leads);
  
  if(isset($leads['leads']) && count($leads['leads']) > 0) {
    $result = write_sf_leads($leads);
  }
}

// Queries for the Salesforce intern leads to be imported
function query_sf_leads($sf) {
  // Function requires an active Salesforce connection.
  if(!is_object($sf)) {
    return;
  }

  $last_cron_run_date = get_last_cron_run_date();

  $soql_fields = get_soql_fields(OBJECT_TYPE_LEAD);
  $soql_fields_text = implode(", ", $soql_fields); 

  $soql = "SELECT " . $soql_fields_text . " FROM Lead";
  $soql .= " WHERE Status = '3. Eligible Applicant' AND LastModifiedDate > " . $last_cron_run_date;

  try {
    $result = $sf->client->query($soql);
  }
  catch (Exception $e) {
    salesforce_api_log(SALESFORCE_LOG_SOME, 'Exception in sf_intern_sync lead queries: ' . $e->getMessage(), array(), WATCHDOG_ALERT);
  }

  if(isset($result->records)) {
    $lead_ids = array();
    if($result->size >= 1) {
      foreach($result->records as $index => $lead) {
        $lead_ids[] = $lead->Id;
      }
      $lead_ids_text = implode_quoted($lead_ids);
      $soql = "SELECT Id, ParentId, Name, Body FROM Attachment WHERE ParentId IN (" . $lead_ids_text . ")"; 
      try {
        $result_attachments = $sf->client->query($soql);
      }
      catch (Exception $e) {
        salesforce_api_log(SALESFORCE_LOG_SOME, 'Exception in sf_intern_sync attachment queries: ' . $e->getMessage(), array(), WATCHDOG_ALERT);
      }
    }
    $leads_records = array();
    foreach($result->records as $key => $lead) {
      $lead = normalize_lead_record($lead);
      $leads_records[$lead->Id] = $lead;
    }
    unset($result);
    $query_results = array('leads' => $leads_records);
    if(isset($result_attachments->records)) { 
      $attachment_records = array();
      foreach($result_attachments->records as $key => $attachment) {
        $attachment_records[$attachment->ParentId] = $attachment;
      }
      unset($result_attachments);
      $query_results['attachments'] = $attachment_records;
    }
    return $query_results;
  }
  else {
    return array();
  }
}

// Write the SF Leads to the database table.
function write_sf_leads($leads) {
  // @todo: Add db code in here.
}

// Normalize a lead record to have all the fields.
function normalize_lead_record($lead) {
  // Exit early if the "bare minimum" of fields is not present.
  if(!is_object($lead) || empty($lead->Id)) {
    return NULL;
  }
  $lead->FirstName = !empty($lead->FirstName) ? $lead->FirstName : '';
  $lead->LastName = !empty($lead->LastName) ? $lead->LastName : '';
  $lead->Email = !empty($lead->Email) ? $lead->Email : '';
  $lead->Phone = !empty($lead->Phone) ? $lead->Phone : '';
  $lead->How_Heard_About_City_Vision__c = !empty($lead->How_Heard_About_City_Vision__c) ? $lead->How_Heard_About_City_Vision__c : '';
  $lead->Internship_Length__c = !empty($lead->Internship_Length__c) ? $lead->Internship_Length__c : '';
  $lead->Internship_Type__c = !empty($lead->Internship_Type__c) ? $lead->Internship_Type__c : '';
  $lead->Bachelors_Program_Fits_with_Career_Goals__c = !empty($lead->Bachelors_Program_Fits_with_Career_Goals__c) ? $lead->Bachelors_Program_Fits_with_Career_Goals__c : '';
  $lead->Is_a_Christian__c = !empty($lead->Is_a_Christian__c) ? $lead->Is_a_Christian__c : '';
  $lead->Age_Qualification__c = !empty($lead->Age_Qualification__c) ? $lead->Age_Qualification__c : '';
  $lead->Secondary_Education__c = !empty($lead->Secondary_Education__c) ? $lead->Secondary_Education__c = '';
  $
}

// Get the last time the cron was run.
// @todo Return the date from the table, 
// or a date 3 years in the past if the table is empty.
function get_last_cron_run_date() {
  // Currently just have this be the present minus 3 years.
  $last_cron_run_date = time() - (60 * 60 * 24 * 365 * 3);
  return gmdate(DATE_ATOM, $last_cron_run_date);
}

// Exports the information on a connection failure.
function exit_connection_failed() {
  // Put code in here to write to the database table that logs the cron runs
}

// Utility function: implodes the array with quotes around it.
function implode_quoted($arr) {
  $imploded_str = '';
  foreach($arr as $key => $value) {
    $arr[$key] = "'" . $value . "'";
  }
  $imploded_str = implode(", ", $arr);
  return $imploded_str;
}

/* 
$result = db_query('SELECT bn, legal_name, account_name FROM canada_charities LIMIT 10');
while($row = db_fetch_array($result)) {
  print_r($row);
}
*/
