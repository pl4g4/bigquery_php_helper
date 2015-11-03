# bigquery_php_helper
Small class to extend bigquery main functionality

###Examples

```
error_reporting(E_ALL);
ini_set('display_errors', 1);

$test = $this->bigQueryGetTables('projectid', 'dataset');

$test = $this->bigQueryGetTableDetails('projectid', 'dataset', 'table');

$test = $this->getProjectsList();

$test = $this->getDatasetList('projectid');

$test = $this->bigQueryGetTableData('projectid', 'dataset', 'table', array('maxResults' => 10));

$sql = 'SELECT * FROM [ppn_sales_test.sales] LIMIT 3';
$test = $this->bigQuerySql('projectid', $sql);

$test = $this->bigQueryLoadDataExistingTable('projectid', 'dataset', 'table', 'gs://bucket/ill.csv', 1);

$test = $this->bigQueryGetJobsList('projectid');

$payload = array(
	'name' => 'car',
	'lastname' => 'toyota',
);
$test = $this->bigQueryStreaming('projectid', 'dataset', 'table', $payload);
```

