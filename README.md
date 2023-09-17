# DataQualityCheck

 "Garbage in - Garbage out". The principle is quite universal, here are some examples:
* If incorrect data comes to the input of an analytical report, the report will show incorrect results.
* If a dataset with errors is used to train an ML model, the model's predictions will also have errors.
￼
￼![dg_garbage_in_out](https://github.com/apovalov/DataQualityCheck/assets/43651275/bafbeb3c-9e71-4293-b575-891884e26749)

## What data properties are important to us

* Completeness: presence of required and optional fields, gaps in the data.
* Consistency: absence of inconsistencies in the data, correctness of relationships.
* Availability: Data is readable.
* Veracity: Values are unambiguous and within acceptable limits.

  
![pipeline](https://github.com/apovalov/DataQualityCheck/assets/43651275/1f27ec30-0420-4fb6-a500-36235ef2fa3b)

* Data - data
* Checklist - list of metrics, checked values and allowed values
* DQ module - framework for data quality check and report building
* DQ report - final report of data quality assessment


* metrics.py - metrics
* checklist.py - list of checks and execution criteria
* report.py - DQ-Report module itself (able to work with both PySpark and Pandas).


checklist.py contains a structure like

A list that contains tuples (tuple). Each tuple contains:
* table name (str),
* check (callable, i.e. callable type),
* criterion (dict)

Criteria is a dictionary (dict):
* key - name of the check (str)
* tuple of two values: minimum and maximum allowable (float or int)

```
…
tables = {}
spark_tables = {}

sales = pd.read_csv('data/ke_daily_sales.csv')
sales_df = pd.DataFrame(sales)

tables['sales'] = sales_df


spark = SparkSession.builder.appName("dq_report").getOrCreate()
spark_tables['sales'] = spark.createDataFrame(sales_df, ['day', 'item_id', 'qty', 'price', 'revenue'])
checklist = checklist.CHECKLIST

report = Report(checklist, engine = "pyspark")
report.fit(spark_tables)

print(report.to_str())

```
![dq_report_HmVLe9g](https://github.com/apovalov/DataQualityCheck/assets/43651275/ed221830-fdc8-49f6-8da5-f3e726ea9561)

Description of columns:
* table_name (str type) - name of the table to be checked (the key of the tables dictionary passed to the Report class instance)
* metric (str type) - metric, example value: "CountDuplicates(columns=['day', 'item_id'])".
* limits (str type) - limits set in checklist.py file 
* values (dict type) - actual values of the metric 
* status (str type): '.' - if the check is successful; 'F' - if the check is not successful; 'E' - if an error occurred during execution (be sure to wrap the code calling the metric calculation in try... except...)
* error - error description (exeption from the except section)
