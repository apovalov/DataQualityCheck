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


## metrics.py
It has a base class Metric and metrics based on it with Pandas and Spark implementation

```
@dataclass
class Metric:
    """Base class for Metric"""

    def __call__(self, df: Union[pd.DataFrame, ps.DataFrame]) -> Dict[str, Any]:
        if isinstance(df, pd.DataFrame):
            return self._call_pandas(df)

        if isinstance(df, ps.DataFrame):
            return self._call_pyspark(df)

        msg = (
            f"Not supported type of arg 'df': {type(df)}. "
            "Supported types: pandas.DataFrame, "
            "pyspark.sql.dataframe.DataFrame"
        )
        raise NotImplementedError(msg)

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        return {}


@dataclass
class CountTotal(Metric):
    """Total number of rows in DataFrame"""

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {"total": len(df)}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        return {"total": df.count()}

...
```

## checklist.py contains a structure like

A list that contains tuples (tuple). Each tuple contains:
* table name (str),
* check (callable, i.e. callable type),
* criterion (dict)

Criteria is a dictionary (dict):
* key - name of the check (str)
* tuple of two values: minimum and maximum allowable (float or int)

```
CHECKLIST = [
    # Table with sales ["day", "item_id", "qty", "revenue", "price"]
    ("sales", CountTotal(), {"total": (1, 1e6)}),
    ("sales", CountLag("day"), {"lag": (0, 3)}),
    ("sales", CountDuplicates(["day", "item_id"]), {"total": (0, 0)}),
    ("sales", CountNull(["qty"]), {"total": (0, 0)}),
...
```

## report.py
Contains class Report which connects all the other classes and caches and outputs the result.
It is also the file that we will call from the command line as a script.

```
…
LimitType = Dict[str, Tuple[float, float]]
CheckType = Tuple[str, Metric, LimitType]

def memoize(f):
    cache = {}

    @wraps(f)
    def memoized_func(*args):
        if args in cache:
            return cache[args]
        result = f(*args)
        cache[args] = result
        return result

    return memoized_func


@dataclass
class Report:
    """DQ report class."""

    checklist: List[CheckType]
    engine: str = "pandas"

    def fit(self, tables: Dict[str, Union[pd.DataFrame, ps.DataFrame]]) -> Dict:
        """Calculate DQ metrics and build report."""
        if self.engine == "pandas":
            return self._fit_pandas(tables)

        if self.engine == "pyspark":
            return self._fit_pyspark(tables)

        raise NotImplementedError("Only pandas and pyspark APIs currently supported!")

    def _fit_pandas(self, tables: Dict[str, pd.DataFrame]) -> Dict:
        """Calculate DQ metrics and build report."""
        self.report_ = {}
        report = self.report_

        # # Check if engine supported
        for table_name, metric, limits in self.checklist:
            report_entry = {
                "table_name": table_name,
                "metric": str(metric),
                "limits": str(limits),
                "values": {},
                "status": "",
                "error": None
            }

            try:
                # Perform the metric calculation on the corresponding table
                metric_results = metric(tables[table_name])
                # Check if the metric_results contain the keys defined in limits
                if len(limits.items()) == 0:
                    report_entry["values"] = metric_results
                    report_entry["error"] = ""
                    report_entry["status"] += "."
                for metric_key, (min_limit, max_limit) in limits.items():
                    if metric_key in metric_results:
                        # value = metric_results[metric_key]
                        report_entry["values"] = metric_results #[metric_key] = value
                        report_entry["error"] = ""

                        # Compare the metric value with the provided limits
                        if min_limit <= metric_results[metric_key] <= max_limit:
                            report_entry["status"] += "."
                        else:
                            report_entry["status"] += "F"
                    else:
                        # Metric key not found in results, set status as "E" for error
                        report_entry["status"] += "E"

            except Exception as e:
                # Error occurred during metric calculation
                report_entry["status"] = "E"
                report_entry["error"] = str(e)

            # Add the entry to the report
            report.setdefault("result", []).append(report_entry)

        # Calculate overall DQ metrics
        passed_count = sum(entry["status"].count(".") for entry in report["result"])
        failed_count = sum(entry["status"].count("F") for entry in report["result"])
        error_count = sum(entry["status"].count("E") for entry in report["result"])
        total_checks = passed_count + failed_count + error_count

        report["passed"] = passed_count
        report["failed"] = failed_count
        report["errors"] = error_count
        report["total"] = total_checks

        if total_checks > 0:
            report["passed_pct"] = (passed_count / total_checks) * 100
            report["failed_pct"] = (failed_count / total_checks) * 100
            report["errors_pct"] = (error_count / total_checks) * 100
        else:
            report["passed_pct"] = 0
            report["failed_pct"] = 0
            report["errors_pct"] = 0
        report['result'] = pd.DataFrame(report['result'])

        table_names = list(tables.keys())
        table_names.sort()
        report["title"] = "DQ Report for tables {}".format(table_names)
        return report


    def _fit_pyspark(self, tables: Dict[str, Union[pd.DataFrame, ps.DataFrame]]) -> Dict:
            """Calculate DQ metrics and build report."""
            self.report_ = {}
            report = self.report_
            ...

            return report


    def to_str(self) -> None:
        """Convert report to string format."""
        report = self.report_

        msg = (
            "This Report instance is not fitted yet. "
            "Call 'fit' before usong this method."
        )

        assert isinstance(report, dict), msg

        pd.set_option("display.max_rows", 500)
        pd.set_option("display.max_columns", 500)
        pd.set_option("display.max_colwidth", 50)
        pd.set_option("display.width", 1000)

        return (
            f"{report['title']}\n\n"
            f"{report['result']}\n\n"
            f"Passed: {report['passed']} ({report['passed_pct']}%)\n"
            f"Failed: {report['failed']} ({report['failed_pct']}%)\n"
            f"Errors: {report['errors']} ({report['errors_pct']}%)\n"
            "\n"
            f"Total: {report['total']}"
        )


from pyspark.sql import SparkSession

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


## Hadoop

Hadoop is an open source software platform for storing and processing large amounts of data. It is designed for parallel processing of data distributed across multiple computers in a cluster. Hadoop is used to store and process large amounts of data.

Apache Spark is an open source distributed computing platform designed for fast and parallel data processing. The platform is built on the Hadoop ecosystem and can be used to process data stored in the Hadoop Distributed File System (HDFS) or other storage systems.

Spark can be used for a wide range of tasks such as data cleaning, machine learning, and real-time data processing. Spark is known for its fast processing speed and ease of use and has become a popular choice for many companies working with BigData.

PySpark is a Python API for Apache Spark. It allows you to write scripts to process data in a Spark cluster in Python. PySpark is often used by Data Science teams because it presents a simple and flexible way to write data processing pipelines for both analysis and model training.

The data itself can be huge in size and stored in the Hadoop Distributed File System (HDFS) on multiple computers. The data processing will be done by the Spark cluster. Pipelines for data processing can be written, for example, in Python using the PySpark library.

Why do I need Spark if there is a DBMS?
Why do you need Spark if you have a DBMS (Postgres, ClickHouse, etc.)?

Spark is a distributed computing platform designed for fast parallel data processing, while traditional DBMSs (database management systems) are designed to store and manage data in a structured format. While DBMSs can be used for some big data processing, they are not always well suited for handling very large volumes or for real-time distributed data processing.

Spark, on the other hand, is designed specifically for the task of distributed processing of large data sets. It is often used in combination with traditional DBMSs, which allows you to get a comprehensive solution and extract the advantages of each approach.


## Higher level of abstraction - lighter code

Such a framework is needed to build a certain data-driven product where the ETL process is quite complex. For example, it may include model training, data processing before training / before recording / before sending. And the whole process needs to be done on demand or on schedule.

PySpark code is much more readable as the complexity of the query or data processing algorithm grows. With Spark, abstractions are added that make the process of developing maintenance, adjusting business logic and interacting with tables much easier.

Translated with www.DeepL.com/Translator (free version)