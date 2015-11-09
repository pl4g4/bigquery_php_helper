# bigquery_php_helper
Small class to extend bigquery main functionality

###Install 

- Include the following code inside your composer.json

```
{
	"repositories": [{
	    "type": "package",
	    "package": {
	        "name": "pl4g4/bigquery_php_helper",
	        "version": "master",
	        "source": {
	            "url": "https://github.com/pl4g4/bigquery_php_helper.git",
	            "type": "git",
	            "reference": "master"
	        },
	        "autoload": {
                "psr-4" : {
                    "pl4g4\\bigquery_php_helper\\" : "src"
                }
            }
	    }
	}],
	"require": {
	  "google/apiclient": "1.0.*@beta",
	  "pl4g4/bigquery_php_helper": "dev-master"
	}
}
```

###Examples

```
error_reporting(E_ALL);
ini_set('display_errors', 1);

$test = $this->bigQueryGetTables('projectid', 'dataset');

$test = $this->bigQueryGetTableDetails('projectid', 'dataset', 'table');

$test = $this->bigQueryGetProjectsList();

$test = $this->bigQueryGetDatasetList('projectid');

$test = $this->bigQueryGetTableData('projectid', 'dataset', 'table', array('maxResults' => 10));

$sql = 'SELECT * FROM [sales] LIMIT 3';
$test = $this->bigQuerySql('projectid', $sql);

$test = $this->bigQueryLoadDataExistingTable('projectid', 'dataset', 'table', 'gs://bucket/ill.csv', 1);

$test = $this->bigQueryGetJobsList('projectid');

$payload = array(
	'name' => 'car',
	'lastname' => 'toyota',
);
$test = $this->bigQueryStreaming('projectid', 'dataset', 'table', $payload);
```
