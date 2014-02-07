<?php

/**
 * Export the intern records into the social graph denormalized table.
 */

/* Constants */

// Object types.
define('OBJECT_TYPE_LEAD', 'Lead');
define('OBJECT_TYPE_CONTACT', 'Contact');

// Setting for function get_picklist_field_info().
define('GET_TID_FIELD_NAMES', TRUE);

// Vocabulary id's for the picklists.
define('SOURCE_VID', 361);
define('INTERN_LENGTH_VID', 362);
define('INTERN_TYPE_VID', 363);
define('CITIZEN_VID', 364);
define('POS_PREF_VID', 365);
define('SPECIAL_SKILLS_VID', 366);
define('WORK_ENVIRON_VID', 367);
define('WORK_POP_PREF_VID', 368);
define('CVC_DEGREE_PROG_VID', 369);

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
  print_r($cv_objects);
  
  if((isset($objects[OBJECT_TYPE_LEAD]) && count($objects[OBJECT_TYPE_LEAD]) > 0) || (isset($objects[OBJECT_TYPE_CONTACT]) && count($objects[OBJECT_TYPE_CONTACT]) > 0)) {
    $result = write_cv_objects($cv_objects);
  }
}

// Queries for the Salesforce intern leads to be imported
function query_sf_objects($sf) {
  // Function requires an active Salesforce connection.
  if(!is_object($sf)) {
    return NULL;
  }  
  $query_results = array();
  $sf_object_ids = array();
  $object_types = get_object_types();
  
  foreach($object_types as $object_type) {
    $object_fields = get_object_fields($object_type);
    $soql_fields = implode(", ", $object_fields);

    $soql = build_soql_query($soql_fields, $object_type); 	
    try {
      $result = $sf->client->query($soql);
    }
    catch (Exception $e) {
      salesforce_api_log(SALESFORCE_LOG_SOME, 'Exception in sf_intern_sync ' . $object_type . ' queries: ' . $e->getMessage(), array(), WATCHDOG_ALERT);
    }

    if(isset($result->records)) {
      if($result->size >= 1) {
        foreach($result->records as $index => $sf_object) {
          $sf_object_ids[] = $sf_object->Id;
        }
      }
      $cv_object_records = array();
      foreach($result->records as $key => $sf_object) {
        $cv_object = normalize_record($sf_object, $object_type);
        $cv_object = populate_record($cv_object, $object_type);
        $cv_object_records[$cv_object->Id] = $cv_object;
      }
      // Free up some memory by unsetting the result array.
      unset($result);
    }
    $query_results += array($object_type => $cv_object_records);
  }
  if(count($sf_object_ids) > 0) {
    $sf_object_ids_text = implode_quoted($sf_object_ids);
    $soql = "SELECT Id, ParentId, Name, Body FROM Attachment WHERE ParentId IN (" . $sf_object_ids_text . ")"; 
    try {
      $result_attachments = $sf->client->query($soql);
    }
    catch (Exception $e) {
      salesforce_api_log(SALESFORCE_LOG_SOME, 'Exception in sf_intern_sync attachment queries: ' . $e->getMessage(), array(), WATCHDOG_ALERT);
    }
    if(isset($result_attachments->records)) { 
      $attachment_records = array();
      foreach($result_attachments->records as $key => $attachment) {
        $attachment_records[$attachment->ParentId] = $attachment;
      }
      // Free up some memory by unsetting the result array.
      unset($result_attachments);
      $query_results['Attachment'] = $attachment_records;
    }
  }
  return $query_results;
}

// Normalize a lead record to have all the fields.
function normalize_record($sf_object, $object_type) {
  // Exit early if the "bare minimum" of fields is not present.
  if(!is_object($sf_object) || empty($sf_object->Id)) {
    return NULL;
  }
  
  $object_fields = get_object_fields($object_type);
  foreach($object_fields as $cv_fieldname => $sf_fieldname) {
    $sf_object->{$cv_fieldname} = !empty($sf_object->{$sf_fieldname}) ? $sf_object->{$sf_fieldname} : '';
    // Remove the duplicate fields.
    if($sf_fieldname != 'Id') {
      unset($sf_object->{$sf_fieldname});
    }
  }
  
  return $sf_object;
}

// Populate the fields that are not directly in the SF object.
function populate_record($cv_object, $object_type) {
  // Exit early if the "bare minimum" of fields is not present.
  if(!is_object($cv_object) || empty($cv_object->Id)) {
    return NULL;
  }
  
  // Set the title field.
  $cv_object->title = $cv_object->applic_first_name . ' ' . $cv_object->applic_last_name;
  
  // Populate the picklist fields by tid.
  $picklist_field_info = get_picklist_field_info();
  foreach($picklist_field_info as $tid_field => $field_info) {
    if($field_info['name'] == 'intern_type') {
      $cv_object->{$field_info['name']} = explode(';', $cv_object->{$field_info['name']});
    }
    else {
      $field_values = array($cv_object->{$field_info['name']});
    }
    foreach($field_values as $key => $field_value) {	
      $field_values[$key] = get_picklist_name_by_synonym($field_value);
      $tid_field_values[] = get_picklist_tid_by_name($field_value, $field_info['vid']);
    }
    $cv_object->{$field_info['name'] = implode(';', $field_values);
    $cv_object->{$tid_field} = implode(';', $tid_field_values);
  }
  return $cv_object;
}

// Build the SOQL query for fetching Leads or Contacts.
function build_soql_query($soql_fields, $object_type) {
  $last_cron_run_date = get_last_cron_run_date();
  $soql = '';
  if($object_type == OBJECT_TYPE_LEAD) {
    $soql = "SELECT " . $soql_fields . " FROM Lead";
    $soql .= " WHERE Status = '3. Eligible Applicant' AND LastModifiedDate > " . $last_cron_run_date;
  }
  else if($object_type == OBJECT_TYPE_CONTACT) {
    $soql = "SELECT " . $soql_fields . " FROM Contact";
    $soql .= " WHERE CVC_Intern_Status__c = '4. Screened Applicant' AND LastModifiedDate > " . $last_cron_run_date;
  }
  return $soql;
}

// Write the objects queried from Salesforce to the database table.
function write_cv_objects($objects) {
  // @todo: Add db code in here.
}

// Exports the information on a connection failure.
function exit_connection_failed() {
  // @todo: Put code in here to write to the database table that logs the cron runs
}

/* Functions that define data structures. */
function get_object_types() {
  return array(OBJECT_TYPE_LEAD, OBJECT_TYPE_CONTACT);
}

function get_object_fields($object_type) {
  $object_fields = array(
    'sfid' => 'Id',
    'applic_first_name' => 'FirstName',
    'applic_last_name' => 'LastName',
    'email' => 'Email',
    'phone' => 'Phone',
    'source' = > 'How_Heard_About_City_Vision__c',
    'intern_length' => 'Internship_Length__c',
    'intern_type' => 'Internship_Type__c',
    'degree_career_goals' => 'Bachelors_Program_Fits_with_Career_Goals__c',
    'is_christian' => 'Is_a_Christian__c',
    'age_requirement' => 'Age_Qualification__c', 
    'diploma_status' => 'Secondary_Education__c', 
    'citizen' => 'Citizenship_Status__c', 
    'commitment_length' => 'Able_to_make_one_year_commitment__c',
    'attending_bachelors' => 'Actively_Pursuing_College__c', 
    'last_active_school' => 'Last_Year_Active_in_College__c', 
    'gender' => 'Gender__c', 
    'applic_loc_street' => 'Street', 
    'applic_loc_city' => 'City', 
    'applic_loc_province' => 'State', 
    'applic_loc_postal_code' => 'PostalCode', 
    'applic_loc_country' => 'Country', 
    'testimony' => 'Personal_Testimony__c', 
    'geo_pref' => 'Interns_Geographic_Preference__c', 
    'pos_pref' => 'Type_of_Position_Preferred__c', 
    'special_skills' => 'Special_Experience__c', 
    'interest_reason' => 'Reason_for_Applying__c', 
    'work_environ' => 'Type_of_Work_Preferred__c',
    'work_pop_pref' => 'Preference_of_Group_Served__c', 
    'language' => 'Languages_Spoken__c',
    'attends_church' => 'Attend_Church_Weekly__c',
    'cvc_degree_prog' => 'City_Vision_Major_CVC_Intern__c', 
    'major' => 'Degree_Preference_if_Non_Intern__c', 
    'has_bachelors' => 'Bachelors_Degree__c', 
    'credits' => 'Estimated_College_Credits_Earned__c', 
    'career_goals' => 'Career_Life_Goals_Next_10_Years__c', 
    'hrly_commit' => 'Time_Commitment__c', 
    'livable_stipend' => 'Living_Stipend_Sufficient__c', 
    'livable_stiplend_expl' => 'Potential_Financial_Hardship__c', 
    'crim_record' => 'Convicted_of_a_felony__c', 
    'crim_desc' => 'Circumstances_of_Crime__c', 
    'dob' => 'Birthdate__c', 
    'housing' => 'In_need_of_housing__c', 
    'sites_req' => 'Site_Requested__c', 
    'loc_pref' => 'S2_Location_Notes__c', 
    'start_time' => 'Preferred_Start_Date__c', 
    'forward_resume' => 'Permission_to_Recommend__c', 
    'webcam' => 'Webcam_Access__c', 
    'skype' => 'Skype_Name__c', 
    'pastoral_ref' => 'Pastoral_Reference_Name__c', 
    'pastoral_ref_church' => 'Pastoral_Reference_Organization__c', 
    'pastoral_ref_phone' => 'Pastoral_Reference_Phone__c', 
    'pastoral_ref_email' => 'Pastoral_Reference_Email__c', 
    'prof_ref' => 'Professional_Reference_Name__c', 
    'prof_ref_org' => 'Professional_Reference_Organization__c', 
    'prof_ref_phone' => 'Professional_Reference_Phone__c', 
    'prof_ref_email' => 'Professional_Reference_Email__c', 
    'ad_source' => 'Ad_Source__c', 
    'ad_campaign' => 'Adwords_Campaign__c', 
    'ad_keywords' => 'Adwords_Keyword__c', 
    'referer_url' => 'Referer_URL__c'
  );
  if($object_type = OBJECT_TYPE_CONTACT) {
    $object_fields['degree_career_goals'] = 'Bachelors_Program_Fits_with__c';
    $object_fields['commitment_length'] = 'Year_Long_Commitment__c';
    $object_fields['applic_loc_street'] = 'MailingStreet';
    $object_fields['applic_loc_city'] = 'MailingCity';
    $object_fields['applic_loc_province'] = 'MailingState';
    $object_fields['applic_loc_postal_code'] = 'MailingPostalCode';
    $object_fields['applic_loc_country'] = 'MailingCountry';
    $object_fields['attends_church'] = 'Attends_church_weekly__c';
    $object_fields['cvc_degree_prog'] = 'City_Vision_Program_Track__c';
    $object_fields['career_goals'] = 'CVC_I5_Career_Life_Goals__c';
    $object_fields['crim_record'] = 'Previous_Criminal_Convictions__c';
    $object_fields['dob'] = 'Birthdate';
    $object_fields['loc_pref'] = 'CVC_S2_Location_Notes__c';
    $object_fields['start_time'] = 'Preferred_Start_Date_CVC__c';
  }
  return $object_fields;
}

function get_picklist_field_info($reverse = FALSE) {
  $picklist_field_info = array(
    'source_tids' => array('name' => 'source', 'vid' => SOURCE_VID),
    'intern_length_tid' => array('name' => 'intern_length', 'vid' => INTERN_LENGTH_VID),
    'intern_type_tid' => array('name' => 'intern_type', 'vid' => INTERN_TYPE_VID),
    'citizen_tid' => array('name' => 'citizen', 'vid' => CITIZEN_VID),
    'pos_pref_tid' => array('name' => 'pos_pref', 'vid' => POS_PREF_VID),
    'special_skills_tid' => array('name' => 'special_skills', 'vid' => SPECIAL_SKILLS_VID),
    'work_environ_tid' => array('name' => 'work_environ', 'vid' => WORK_ENVIRON_VID),
    'work_pop_pref_tid' => array('name' => 'work_pop_pref', 'vid' => WORK_POP_PREF_VID),
    'cvc_degree_prog_tid' => array('name' => 'cvc_degree_prog', 'vid' => CVC_DEGREE_PROG_VID),
  );
  if($reverse == TRUE) {
    $picklist_field_info = array_keys($picklist_field_info);
  }
  return $picklist_field_info;
}

/* Utility functions, used elsewhere. */

// Load the correct picklist name by synonym name.
// Note: Synonyms in the {term_synonym} table are used to indicate how the values for the picklists
// are actually stored in Salesforce, and will need to be updated if something changes in SF.
function get_picklist_name_by_synonym($synonym) {
  db_set_active('urbmi5');
  $name = db_result(db_query("SELECT td.name FROM {term_data} td JOIN {term_synonym} ts ON ts.tid = td.tid WHERE ts.name = '%s'", $synonym);
  db_set_active('techmi5_socgraph');
  if(empty($name)) {
    $name = $synonym;
  }
  return $name;
}

// Load the correct tid for population into the CV.org record.
function get_picklist_tid_by_name($name, $vid) {
  db_set_active('urbmi5');
  $tid = db_result(db_query("SELECT tid FROM {term_data} WHERE name = '%s' AND vid = %d", $name, $vid));
  db_set_active('techmi5_socgraph');
  return $tid;
}

// Get the last time the cron was run.
// @todo Return the date from the table, 
// or a date 3 years in the past if the table is empty.
function get_last_cron_run_date() {
  // Currently just have this be the present minus 3 years.
  $last_cron_run_date = time() - (60 * 60 * 24 * 365 * 3);
  return gmdate(DATE_ATOM, $last_cron_run_date);
}

// Implodes the array with quotes around it.
function implode_quoted($arr) {
  $imploded_str = '';
  foreach($arr as $key => $value) {
    $arr[$key] = "'" . $value . "'";
  }
  $imploded_str = implode(", ", $arr);
  return $imploded_str;
}
