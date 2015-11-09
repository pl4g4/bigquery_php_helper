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
	public function bigQueryGetProjectsList(){
		$projectList =  $this->bigqueryService->projects->listProjects();
		return $projectList->getProjects();
	}

	/**
	 * @param $project_id
	 * @return \Exception|\Google_Service_Bigquery_DatasetList|mixed
	 */
	public function bigQueryGetDatasetList($project_id){
		$datasetList =  $this->bigqueryService->datasets->listDatasets($project_id);
		return $datasetList->getDatasets();
	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @return \Exception|\Google_Service_Bigquery_TableList|mixed
	 */
	public function bigQueryGetTables($project_id, $dataSet) {
		$tables = $this->bigqueryService->tables->listTables($project_id, $dataSet);
		return $tables->getTables();
	}

	/**
	 * @param $project_id
	 * @param $dataSet
	 * @param $tableId
	 * @return array|\Exception|\Google_Service_Bigquery_Table
	 */
	public function bigQueryGetTableDetails($project_id, $dataSet, $tableId) {
		$tableDetails = $this->bigqueryService->tables->get($project_id, $dataSet, $tableId);
		return array(
			'schema' => $tableDetails->getSchema(),
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
		return array( 'rows' => $tableData->getRows(), 'count' => $tableData->count(), 'totalRows' => $tableData->getTotalRows());
	}


	/**
	 * @param $project_id
	 * @param $sql
	 * @param array $params
	 * @return array|\Exception
	 */
	public function bigQuerySql($project_id, $sql, $params = array()) {

		$query = new Google_Service_Bigquery_QueryRequest();
		$query->setQuery($sql);

		$result = $this->bigqueryService->jobs->query($project_id, $query, $params);
		$fieldsOject = $result->getSchema()->getFields();
		$rows = $result->getRows();
		$totalRows = $result->getTotalRows();
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

		return array('count' => $totalCount, 'totalRows' => $totalRows, 'results' => $newResults);

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
		$destination_table = new Google_Service_Bigquery_TableReference();
		$destination_table->setProjectId($project_id);
		$destination_table->setDatasetId($dataSet);
		$destination_table->setTableId($tableId);

		// Set the load configuration, including source file(s) and schema
		$load_configuration = new Google_Service_Bigquery_JobConfigurationLoad();
		//'gs://YOUR_GOOGLE_CLOUD_STORAGE_BUCKET/file.csv'
		$load_configuration->setSourceUris(array($csvPath));
		$load_configuration->setDestinationTable($destination_table);
		$load_configuration->skipLeadingRows = $skipLeadingRows;
		$load_configuration->sourceFormat = 'CSV';
		$load_configuration->setwriteDisposition('WRITE_APPEND');

		$job_configuration = new Google_Service_Bigquery_JobConfiguration();
		$job_configuration->setLoad($load_configuration);

		$job = new Google_Service_Bigquery_Job();
		$job->setKind('load');
		$job->setConfiguration($job_configuration);

		$jobs = $this->bigqueryService->jobs;
		$response = $jobs->insert($project_id, $job);

		$jobStatus = new Google_Service_Bigquery_JobStatus();
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
		$jobsList = $this->bigqueryService->jobs->listJobs($project_id, $params);
		return $jobsList->getJobs();
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

		$payload = json_decode(json_encode($payload));

		$rows = array();
		$row = new Google_Service_Bigquery_TableDataInsertAllRequestRows;
		$row->setJson($payload);
		$row->setInsertId(null);
		$rows[0] = $row;
		$request = new Google_Service_Bigquery_TableDataInsertAllRequest;
		$request->setKind('bigquery#tableDataInsertAllRequest');
		$request->setRows($rows);
		$response = $this->bigqueryService->tabledata->insertAll($project_id, $dataSet, $tableId, $request);

		return $response;

		//The URL which does the actual streaming to BigQuery
		/*$url = 'http://bigquery.streaming.ie/StreamToBigQuery.php';

		$post_params = array();+
		foreach ($payload as $key => &$val) {
			if (is_array($val)) $val = implode(',', $val);
			$post_params[] = $key . '=' . urlencode($val);
		}

		$post_string = implode('&', $post_params);
		$parts = parse_url($url);

		$fp = pfsockopen($parts['host'],
			isset($parts['port']) ? $parts['port'] : 8080, $errno, $errstr, 5);

		$parts['path'] .= '?' . $post_string;
		$out = "POST " . $parts['path'] . " HTTP/1.1\r\n";
		$out .= "Host: " . $parts['host'] . "\r\n";
		$out .= "Content-Type: application/x-www-form-urlencoded\r\n";
		$out .= "Content-Length: " . strlen($post_string) . "\r\n";
		$out .= "Connection: Close\r\n\r\n";
		if (isset($post_string)) $out .= $post_string;

		fwrite($fp, $out);
		fclose($fp);*/

	}

} 