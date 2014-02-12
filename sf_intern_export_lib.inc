<?php

/* Constants */

// Object types.
define('OBJECT_TYPE_LEAD', 'Lead');
define('OBJECT_TYPE_CONTACT', 'Contact');
define('OBJECT_TYPE_ATTACHMENT', 'Attachment');
define('OBJECT_TYPE_CV', 'CVOrg');

// Setting for function get_cv_object_fields().
define('GET_CV_FIELD_FIELDS', TRUE);
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

// Directory for storing attachment files.
define('ATTACHMENT_DIRECTORY', '/home/chrisvol/files/resumes');

// SF connection failed.
define('CONNECTION_FAILED', 0);
define('CONNECTION_SUCCESS', 1);

/* Workhorse functions */

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
    $object_fields = get_sf_object_fields($object_type);
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
  // Fetch the attachments linked to the Leads & Contacts.
  if(count($sf_object_ids) > 0) {
    $attachment_records = query_sf_attachments($sf, $sf_object_ids);
	if(count($attachment_records) > 0) {
	  $query_results[OBJECT_TYPE_ATTACHMENT] = $attachment_records;
	}
  }
  return $query_results;
}

// Query for the Salesforce attachments
function query_sf_attachments($sf, $sf_object_ids) {
  $attachment_records = array();
  $queried_all = FALSE;
  // Function requires an active Salesforce connection.
  if(!is_object($sf)) {
    return NULL;
  }  
  // If no ParentIds, return the empty array.
  if(!is_array($sf_object_ids) || count($sf_object_ids) < 1) {
	return $attachment_records;
  }
  // Implode & quote the array of Ids for use in SOQL statement.
  $sf_object_ids_text = implode_quoted($sf_object_ids);
  // Query for the attachments.
  $soql = "SELECT Id, ParentId, Name, Body FROM Attachment WHERE ParentId IN (" . $sf_object_ids_text . ")"; 
  try {
    $result_attachments = $sf->client->query($soql);
  }
  catch (Exception $e) {
    salesforce_api_log(SALESFORCE_LOG_SOME, 'Exception in sf_intern_sync attachment queries: ' . $e->getMessage(), array(), WATCHDOG_ALERT);
  }
  // Use queryMore to get the rest of the attachments.
  if($result_attachments->size > 0) {
	while($queried_all == FALSE) {
	  foreach($result_attachments->records as $key => $attachment) {
	    $attachment_filename = write_attachment_to_file($attachment);
		unset($attachment);
		if(!empty($attachment_filename)) {
          $attachment_records[$attachment->ParentId] = $attachment_filename;
		}
      }
	  if($result_attachments->done != TRUE) {
	    try {
		  $result_attachments = $sf->client->queryMore($result_attachments->queryLocator);
		}
		catch (Exception $e) {
		  salesforce_api_log(SALESFORCE_LOG_SOME, 'Exception in sf_intern_sync attachment queryMore: ' . $e->getMessage(), array(), WATCHDOG_ALERT);
		}
	  }
	  else {
		$queried_all = TRUE;
	  }
	} 
  }
  return $attachment_records;
}

function write_attachment_to_file($attachment) {
  // Check for required properties.
  if(!is_object($attachment) || empty($attachment->ParentId) || empty($attachment->Name) || empty($attachment->Body)) {
    return NULL;
  }
  // Get the filename parts.
  $base_filename = pathinfo($attachment->Name, PATHINFO_FILENAME);
  $base_filename = preg_replace('/\s+/', '', $base_filename);
  $ext = pathinfo($attachment->Name, PATHINFO_EXTENSION);
  // Check if the file extension is allowed.
  if(!file_extension_is_allowed($ext)) {
    return NULL;
  }
  // Write the file to the filesystem.
  $filename = ATTACHMENT_DIRECTORY . '/SFID=' .  $attachment->Id . '_Filename=' . $base_filename . '.' . $ext;
  $result = file_put_contents($filename, $attachment->Body);
  // @todo: Add db logging here.
  if($result > 0) {
    return $filename;
  }
  else {
    return NULL;
  }
}
  
// Normalize a lead record to have all the fields.
function normalize_record($sf_object, $object_type) {
  // Exit early if the "bare minimum" of fields is not present.
  if(!is_object($sf_object) || empty($sf_object->Id)) {
    return NULL;
  }
  
  // Set all the missing fields to have values, with the CV.org fieldnames.
  $object_fields = get_cv_object_fields();
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
  
  // Format the birthdate field.
  $dob = strtotime($cv_object->dob);
  $cv_object->dob = date('Y-m-d H:i:s', $dob);
  
  // Normalize the applic_loc_country field
  $cv_object->applic_loc_country = get_country_code($cv_object->applic_loc_country);
  
  // Populate the picklist fields by tid.
  $picklist_field_info = get_picklist_field_info();
  foreach($picklist_field_info as $tid_field => $field_info) {
    $field_values = array();
    $tid_field_values = array();
    if($field_info['name'] == 'intern_type') {
      $field_values = explode(';', $cv_object->{$field_info['name']});
    }
    else {
      $field_values = array($cv_object->{$field_info['name']});
    }
    foreach($field_values as $key => $field_value) {
	  $field_values[$key] = get_picklist_name_by_synonym($field_value);
      $tid_field_values[] = get_picklist_tid_by_name($field_values[$key], $field_info['vid']);
    }
    if(count($field_values) > 0) {
      $cv_object->{$field_info['name']} = implode(';', $field_values);
    }
    if(count($tid_field_values) > 0) {
      $cv_object->{$tid_field} = implode(';', $tid_field_values);
    }
  }
  
  //@todo: Set the correct value for cvintern_screened.
  if($object_type == OBJECT_TYPE_LEAD) {
    $cv_object->cvintern_screened = 0;
  }
  else {
    $cv_object->cvintern_screened = 1;
  }
  
  $cv_object->cvcintern_applicant = "City Vision Intern Applicant";
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
function write_cv_objects($cv_objects) {
  $results = array('attempts' => array(OBJECT_TYPE_LEAD => 0, OBJECT_TYPE_CONTACT => 0, OBJECT_TYPE_ATTACHMENT => 0),
                   'successes' => array(OBJECT_TYPE_LEAD => 0, OBJECT_TYPE_CONTACT => 0, OBJECT_TYPE_ATTACHMENT => 0),
				   'failures' => array(OBJECT_TYPE_LEAD => 0, OBJECT_TYPE_CONTACT => 0, OBJECT_TYPE_ATTACHMENT => 0));
  $sql_fields = get_cv_object_fields(GET_CV_FIELD_NAMES);
  $sql_fields = implode(', ', $sql_fields);
  $placeholders = get_cv_object_fields();
  $placeholders = implode(', ', $placeholders);
  
  $object_types = get_object_types();
  foreach($object_types as $object_type) {
    foreach($cv_objects[$object_type] as $c) {
      $sql = "INSERT INTO tbl_applic_resume_joined (" . $sql_fields . ") VALUES";
      $sql .= " ( " . $placeholders . ")";
      $result = db_query($sql, $c->sfid, $c->sfid, $c->sfid, $c->sfid, $c->source_tids, 
        $c->intern_length_tid, $c->intern_type_tid, $c->citizen_tid, $c->pos_pref_tid, $c->special_skills_tid,
        $c->work_environ_tid, $c->work_pop_pref_tid, $c->cvc_degree_prog_tid, $c->cvintern_screened, $c->cvcintern_applicant,
        $c->sfid, $c->title, $c->applic_first_name, $c->applic_last_name, $c->email, $c->phone,
        $c->source, $c->intern_length, $c->intern_type, $c->degree_career_goals, $c->is_christian,
        $c->age_requirement, $c->diploma_status, $c->citizen, $c->commitment_length, $c->attending_bachelors,
        $c->last_active_school, $c->gender, $c->applic_loc_street, $c->applic_loc_city, $c->applic_loc_province,
        $c->applic_loc_postal_code, $c->applic_loc_country, $c->testimony, $c->geo_pref,
        $c->pos_pref, $c->special_skills, $c->interest_reason, $c->work_environ, $c->work_pop_pref,
        $c->language, $c->attends_church, $c->cvc_degree_prog, $c->major, $c->has_bachelors,
        $c->credits, $c->career_goals, $c->hrly_commit, $c->livable_stipend, $c->livable_stiplend_expl,
        $c->crim_record, $c->crim_desc, $c->dob, $c->housing, $c->sites_req, $c->loc_pref,
        $c->start_time, $c->forward_resume, $c->webcam, $c->skype, $c->pastoral_ref, $c->pastoral_ref_church, 
        $c->pastoral_ref_phone, $c->pastoral_ref_email, $c->prof_ref, $c->prof_ref_org, $c->prof_reg_phone,
        $c->prof_ref_email, $c->ad_source, $c->ad_campaign, $c->ad_keywords, $c->referer_url);
	  if($result != FALSE) {
	    $results['successes'][$object_type]++;
	  }
	  else {
	    $results['failures'][$object_type]++;
	  }
    }
  }
  if(isset($cv_objects[OBJECT_TYPE_ATTACHMENT]) && count($cv_objects[OBJECT_TYPE_ATTACHMENT]) > 0) {
    $results['attempts'][OBJECT_TYPE_ATTACHMENT] = count($cv_objects[OBJECT_TYPE_ATTACHMENT]);
    foreach($cv_objects[OBJECT_TYPE_ATTACHMENT] as $parent_sfid => $filename) {
      $sql = 'UPDATE tbl_applic_resume_joined SET res_file = "%s" WHERE sfid = "%s"';
	  $result = db_query($sql, $filename, $parent_sfid);
	  if($result != FALSE) {
	    $results['successes'][OBJECT_TYPE_ATTACHMENT]++;
	  }
	  else {
	    $results['failures'][OBJECT_TYPE_ATTACHMENT]++;
	  }
    }
  }	
  return $results;
}

// Log the results of the export from SF to CV
function write_export_results($results) {
  $last_cron_run_id = get_last_cron_run_id();
  $sql = "INSERT INTO tbl_sf_intern_export_cron (cron_id, run_date, success, 
    lead_attempts, lead_successes, lead_failures, 
	contact_attempts, contact_successes, contact_failures,
	attachment_attempts, attachment_successes, attachment_failures) VALUES
	(%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)";
  $result = db_query($sql, $last_cron_run_id, time(), CONNECTION_SUCCESS, 
    $results['attempts'][OBJECT_TYPE_LEAD], $results['successes'][OBJECT_TYPE_LEAD], $results['failures'][OBJECT_TYPE_LEAD],
	$results['attempts'][OBJECT_TYPE_CONTACT], $results['successes'][OBJECT_TYPE_CONTACT], $results['failures'][OBJECT_TYPE_CONTACT],
	$results['attempts'][OBJECT_TYPE_ATTACHMENT], $results['successes'][OBJECT_TYPE_ATTACHMENT], $results['failures'][OBJECT_TYPE_ATTACHMENT]);
  return $result;
}

// Writes to the cron log table information on a connection failure.
function exit_connection_failed() {
  $last_cron_run_id = get_last_cron_run_id();
  $sql = "INSERT INTO tbl_sf_intern_export_cron (cron_id, run_date, success) VALUES (%d, %d, %d)";
  $result = db_query($sql, $last_cron_run_id, time(), CONNECTION_FAILED);
  return $result;
}

/* Functions that define data structures. */

// Get the types of Salesforce objects for which to query.
function get_object_types() {
  return array(OBJECT_TYPE_LEAD, OBJECT_TYPE_CONTACT);
}

// Get the fields of the Salesforce object for leads and contacts.
function get_sf_object_fields($object_type) {
  $object_fields = array(
    'sfid' => 'Id',
    'applic_first_name' => 'FirstName',
    'applic_last_name' => 'LastName',
    'email' => 'Email',
    'phone' => 'Phone',
    'source' => 'How_Heard_About_City_Vision__c',
    'intern_length' => 'Internship_Length__c',
    'intern_type' => 'Internship_Type__c',
    'degree_career_goals' => 'Bachelors_Program_Fits_with_Career_Goals__c',
    'is_christian' => 'Is_a_Christian__c',
    'age_requirement' => 'Age_Qualification__c', 
    'diploma_status' => 'Secondary_Education__c', 
    'citizen' => 'Work_Qualification__c',
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
  if($object_type == OBJECT_TYPE_CONTACT) {
    $object_fields['top_candidate'] = 'Top_Candidate__c';
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

// Get the fields to write to in the social graph database, and their placeholder type.
function get_cv_object_fields($reverse = FALSE) {
  $object_fields = array(
    'nid' => "'%s'",
    'vid' => "'%s'",
    'applic_nid' => "'%s'",
    'applic_vid' => "'%s'",
    'source_tids' => "'%s'",
    'intern_length_tid' => '%d',
    'intern_type_tid' => '%d',
    'citizen_tid' => '%d',
    'pos_pref_tid' => '%d',
    'special_skills_tid' => '%d',
    'work_environ_tid' => '%d',
    'work_pop_pref_tid' => '%d',
    'cvc_degree_prog_tid' => '%d',
    'cvintern_screened' => '%d',
    'cvcintern_applicant' => "'%s'",
    'sfid' => "'%s'",
    'title' => "'%s'",
    'applic_first_name' => "'%s'",
    'applic_last_name' => "'%s'",
    'email' => "'%s'",
    'phone' => "'%s'",
    'source' => "'%s'",
    'intern_length' => "'%s'",
    'intern_type' => "'%s'",
    'degree_career_goals' => "'%s'",
    'is_christian' => "'%s'",
    'age_requirement' => "'%s'", 
    'diploma_status' => "'%s'",
    'citizen' => "'%s'",
    'commitment_length' => "'%s'",
    'attending_bachelors' => "'%s'", 
    'last_active_school' => "'%s'",
    'gender' => "'%s'", 
    'applic_loc_street' => "'%s'",
    'applic_loc_city' => "'%s'", 
    'applic_loc_province' => "'%s'", 
    'applic_loc_postal_code' => "'%s'", 
    'applic_loc_country' => "'%s'", 
    'testimony' => "'%s'", 
    'geo_pref' => "'%s'", 
    'pos_pref' => "'%s'", 
    'special_skills' => "'%s'",
    'interest_reason' => "'%s'", 
    'work_environ' => "'%s'",
    'work_pop_pref' => "'%s'", 
    'language' => "'%s'",
    'attends_church' => "'%s'",
    'cvc_degree_prog' => "'%s'",
    'major' => "'%s'",
    'has_bachelors' => "'%s'",
    'credits' => "'%s'",
    'career_goals' => "'%s'",
    'hrly_commit' => "'%s'",
    'livable_stipend' => "'%s'",
    'livable_stiplend_expl' => "'%s'",
    'crim_record' => "'%s'", 
    'crim_desc' => "'%s'",
    'dob' => "'%s'",
    'housing' => "'%s'",
    'sites_req' => "'%s'", 
    'loc_pref' => "'%s'",
    'start_time' => "'%s'",
    'forward_resume' => "'%s'",
    'webcam' => "'%s'",
    'skype' => "'%s'",
    'pastoral_ref' => "'%s'",
    'pastoral_ref_church' => "'%s'",
    'pastoral_ref_phone' => "'%s'", 
    'pastoral_ref_email' => "'%s'", 
    'prof_ref' => "'%s'",
    'prof_ref_org' => "'%s'", 
    'prof_ref_phone' => "'%s'", 
    'prof_ref_email' => "'%s'",
    'ad_source' => "'%s'",
    'ad_campaign' => "'%s'", 
    'ad_keywords' => "'%s'",
    'referer_url' => "'%s'",
  );
  if($reverse == TRUE) {
    $object_fields = array_keys($object_fields);
  }
  return $object_fields;
}

// Get information about the picklist fields.
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
  $name = db_result(db_query("SELECT td.name FROM um_term_data td JOIN um_term_synonym ts ON ts.tid = td.tid WHERE ts.name = '%s'", $synonym));
  db_set_active('techmi5_socgraph');
  if(empty($name)) {
    $name = $synonym;
  }
  return $name;
}

// Load the correct tid for population into the CV.org record.
function get_picklist_tid_by_name($name, $vid) {
  db_set_active('urbmi5');
  $tid = db_result(db_query("SELECT tid FROM um_term_data WHERE name = '%s' AND vid = %d", $name, $vid));
  db_set_active('techmi5_socgraph');
  return $tid;
}

// Load the ISO 2-character country code by country name.
function get_country_code($name) {
  db_set_active('default');
  $iso_code = db_result(db_query("SELECT country_iso_code_2 FROM uc_countries WHERE country_name = '%s'", $name));
  db_set_active('techmi5_socgraph');
  return $iso_code;
}

// Get last cron run id.
function get_last_cron_run_id() {
  // Default to 1 for cron run id.
  $last_cron_run_id = 1;
  $sql = 'SELECT MAX(cron_id) FROM tbl_sf_intern_export_cron';
  $result = db_result(db_query($sql));
  if($result != FALSE) {
    $last_cron_run_id = $result;
  }
  return $last_cron_run_id;
}

// Get the last time the cron was run.
// Returns the date from the table, 
// or a date 3 months in the past if the table is empty.
function get_last_cron_run_date() {
  // Default to the present minus 3 months.
  $last_cron_run_date = time() - (60 * 60 * 24 * 90);
  $sql = 'SELECT run_date FROM tbl_sf_intern_export_cron WHERE cron_id = MAX(cron_id)';
  $result = db_result(db_query($sql));
  // Set to  if the table couldn't be read or was empty.
  if($result != FALSE) {
    $last_cron_run_date = $result;
  }
  return gmdate(DATE_ATOM, $last_cron_run_date);
}

// Check if the file extension is allowed.
function file_extension_is_allowed($ext) {
  $is_allowed = FALSE;
  $allowed_exts = array('txt', 'doc', 'docx', 'rtf', 'htm', 'html', 'pdf', 'odt', 'wpd');
  if(in_array($ext, $allowed_exts)) {
    $is_allowed = TRUE;
  }
  return $is_allowed;
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
