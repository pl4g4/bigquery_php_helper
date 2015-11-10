<?php

/**
* @author        pl4g4
* @since         30/10/15 2:09 PM
*
* @description   Bigquery helper class
*
**/

namespace src\BigQueryLibrary;

class BigQueryLibrary {

	private $bigqueryService;
	private $email_address;
	private $key_file_location;
	private $client;

	/**
	 * @param string $key_location
	 * @param string $email_address
	 */
	function __construct($key_location, $email_address){

		$this->email_address = $email_address;
		$this->key_file_location = $key_location;

		$this->client = new Google_Client();
		$key = file_get_contents($this->key_file_location);

		$cred = new Google_Auth_AssertionCredentials(
			$this->email_address,
			Google_Service_Bigquery::BIGQUERY,
			$key
		);

		$this->client->setAssertionCredentials($cred);

		//setup proxy if neccesary
		/*$io = new Google_IO_Curl($this->client);
		$curlOptions = array();
		$curlOptions[CURLOPT_PROXY] = "http://proxy.local";
		$curlOptions[CURLOPT_PROXYPORT] = "8080";
		$io->setOptions($curlOptions);
		$this->client->setIo($io);*/

		if ($this->client->getAuth()->isAccessTokenExpired() || $this->client->getAccessToken() == NULL || $this->client->getAccessToken() == '') {
			$auth = new Google_Auth_OAuth2($this->client);
			$auth->refreshTokenWithAssertion($cred);
			$token = $auth->getAccessToken();
		    $this->client->setAccessToken($token);
		}

		// Instantiate a new BigQuery Client
		$this->bigqueryService = new Google_Service_Bigquery($this->client);

	}

/**
	 * @return \Exception|\Google_Service_Bigquery_ProjectList|mixed
	 */
	public function getProjectsList(){

		$projectList =  $this->bigqueryService->projects->listProjects();

		$count = 0;
		$projects = array();
		foreach($projectList as $project){
			$projects['project_'.$count]['friendlyName'] = $project->friendlyName;
			$projects['project_'.$count]['id'] = $project->id;
			$projects['project_'.$count]['kind'] = $project->kind;
			$count++;
		}

		return $projects;

	}

	/**
	 * @param $project_id
	 * @return \Exception|\Google_Service_Bigquery_DatasetList|mixed
	 */
	public function getDatasetList($project_id){

		$datasetList =  $this->bigqueryService->datasets->listDatasets($project_id)->getDatasets();

		$count = 0;
		$dataset = array();
		foreach($datasetList as $set){
			$dataset['dataset_'.$count]['friendlyName'] = $set->friendlyName;
			$dataset['dataset_'.$count]['id'] = $set->id;
			$dataset['dataset_'.$count]['kind'] = $set->kind;
			$count++;
		}


		return $dataset;

	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @return \Exception|\Google_Service_Bigquery_TableList|mixed
	 */
	public function bigQueryGetTables($project_id, $dataSet) {

		$tables = $this->bigqueryService->tables->listTables($project_id, $dataSet);

		$count = 0;
		$result = array();
		foreach($tables as $table){
			$result['table_'.$count]['friendlyName'] = $table->friendlyName;
			$result['table_'.$count]['id'] = $table->id;
			$result['table_'.$count]['kind'] = $table->kind;
			$result['table_'.$count]['type'] = $table->type;
			$count++;
		}

		return $result;

	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @param $tableId
	 * @return array|\Exception|\Google_Service_Bigquery_Table
	 */
	public function bigQueryGetTableDetails($project_id, $dataSet, $tableId) {

		$tableDetails = $this->bigqueryService->tables->get($project_id, $dataSet, $tableId);

		$fields = array();
		$count = 0;
		foreach($tableDetails->getSchema() as $field) {
			$fields['field_'.$count]['name'] = $field->name;
			$fields['field_'.$count]['type'] = $field->type;
			$count++;
		}

		return array(
			'schema' => $fields,
			'description' => $tableDetails->getDescription(),
			'creationTime' => $tableDetails->getCreationTime(),
			'friendlyName' => $tableDetails->getFriendlyName(),
			'NumRows' => $tableDetails->getNumRows(),
		);

	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @param $tableId
	 * @param array $params
	 * @return array|\Exception
	 */
	public function bigQueryGetTableData($project_id, $dataSet, $tableId, $params = array()) {

		$tableData = $this->bigqueryService->tabledata->listTabledata($project_id, $dataSet, $tableId, $params);

		$rows = $tableData->getRows();
		$newResults = array();
		$count = 0;
		foreach ($rows as $row) {
			$newRow = array();
			$fieldCount = 0;
			foreach ($row['f'] as $field) {
				$newRow['field_'.$fieldCount] = $field['v'];
				$fieldCount++;
			}

			$newResults['row_'.$count] = $newRow;
			$count++;
		}

		return array(
			'rows' => $newResults,
			'count' => $tableData->count(),
			'totalRows' => $tableData->getTotalRows()
		);

	}


	/**
	 * @param $project_id
	 * @param $sql
	 * @param array $params
	 * @return array|\Exception
	 */
	public function bigQuerySql($project_id, $sql, $params = array()) {

		$query = new \Google_Service_Bigquery_QueryRequest();
		$query->setQuery($sql);

		$result = $this->bigqueryService->jobs->query($project_id, $query, $params);
		$fieldsOject = $result->getSchema()->getFields();
		$rows = $result->getRows();
		$totalCount = $result->count();

		$fields = array();
		foreach ($fieldsOject as $field) {
			$fields[] = $field->name;
		}

		$count = 0;
		$newResults = array();

		foreach ($rows as $row) {
			$newRow = array();
			foreach ($row['f'] as $field) {
				array_push($newRow, $field['v']);
			}
			//making the result an associative array
			$newResults['row_'.$count] = array_combine($fields, $newRow);
			$count++;
		}

		return array('count' => $totalCount, 'results' => $newResults);

	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @param $tableId
	 * @param $csvPath
	 * @param $skipLeadingRows
	 * @return array|\Exception
	 */
	public function bigQueryLoadDataExistingTable($project_id, $dataSet, $tableId, $csvPath, $skipLeadingRows = 1){

		// Information about the destination table
		$destination_table = new \Google_Service_Bigquery_TableReference();
		$destination_table->setProjectId($project_id);
		$destination_table->setDatasetId($dataSet);
		$destination_table->setTableId($tableId);

		// Set the load configuration, including source file(s) and schema
		$load_configuration = new \Google_Service_Bigquery_JobConfigurationLoad();
		//'gs://YOUR_GOOGLE_CLOUD_STORAGE_BUCKET/file.csv'
		$load_configuration->setSourceUris(array($csvPath));
		$load_configuration->setDestinationTable($destination_table);
		$load_configuration->skipLeadingRows = $skipLeadingRows;
		$load_configuration->sourceFormat = 'CSV';
		$load_configuration->setwriteDisposition('WRITE_APPEND');

		$job_configuration = new \Google_Service_Bigquery_JobConfiguration();
		$job_configuration->setLoad($load_configuration);

		$job = new \Google_Service_Bigquery_Job();
		$job->setKind('load');
		$job->setConfiguration($job_configuration);

		$jobs = $this->bigqueryService->jobs;
		$response = $jobs->insert($project_id, $job);

		$jobStatus = new \Google_Service_Bigquery_JobStatus();
		$status = $response->getStatus();

		if ($jobStatus->count() != 0) {
			$err_res = $jobStatus->getErrorResult();
			return $err_res->getMessage();
		}

		$jr = $response->getJobReference();
		$jobId = $jr['jobId'];
		$state = $status['state'];

		return array(
			'JOBID' => $jobId,
			'STATUS' => $state
		);

	}

	/**
	 * @param $project_id
	 * @param array $params
	 * @return \Exception|mixed
	 */
	public function bigQueryGetJobsList($project_id, $params = array()){

		$jobsList = $this->bigqueryService->jobs->listJobs($project_id, $params)->getJobs();

		$count = 0;
		$jobs = array();
		foreach($jobsList as $job){
			$jobs['job_'.$count]['id'] = $job->id;
			$jobs['job_'.$count]['kind'] = $job->kind;
			$jobs['job_'.$count]['state'] = $job->state;
			$jobs['job_'.$count]['userEmail'] = $job->userEmail;
			$count++;
		}

		return $jobs;
	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @param $tableId
	 * @param array $payload
	 * @return \Exception|\Google_Service_Bigquery_TableDataInsertAllResponse
	 */
	public function bigQueryStreaming($project_id, $dataSet, $tableId, $payload = array()){

		//check this instructions
		//http://blog.shinetech.com/2014/08/25/put-on-your-streaming-shoes/

		$rows = array();

		foreach ($payload as $k => $v) {
			$row = new \Google_Service_Bigquery_TableDataInsertAllRequestRows;
			$row->setJson($payload);
			//$row->setInsertId(null);
			//$row->setInsertId( strtotime('now') );
			$rows[] = $row;
		}

		$request = new \Google_Service_Bigquery_TableDataInsertAllRequest;
		$request->setKind('bigquery#tableDataInsertAllRequest');
		$request->setRows($rows);
		$response = $this->bigqueryService->tabledata->insertAll($project_id, $dataSet, $tableId, $request);

		$result = $response->getInsertErrors();

		if(!empty($result)){
			$errors = array();
			$count = 0;
			foreach($result as $error){

				$vars = $this->get_object_vars_all($error);
				$errors['error_'.$count]['index'] = $error->index;
				$errors['error_'.$count]['error'] = $vars['modelData']['errors'];
				$count++;

			}

			$result = $errors;

		}else{

			$result = 'data was inserted';

		}

		return $result;

	}

	//get protected variables from class .... hacky way
	function get_object_vars_all($obj) {
		$objArr = substr(str_replace(get_class($obj)."::__set_state(","",var_export($obj,true)),0,-1);
		eval("\$values = $objArr;");
		return $values;
	}

} 
