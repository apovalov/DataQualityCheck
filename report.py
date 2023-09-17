"""DQ Report."""

from typing import Dict, List, Tuple, Union
from dataclasses import dataclass
from metrics import Metric

import pandas as pd
import pyspark.sql as ps
from functools import wraps
from pyspark.sql import DataFrame
import checklist

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
                    if isinstance(tables[table_name], ps.DataFrame):
                        print('_call_pyspark_1')
                        metric_results = metric._call_pyspark(tables[table_name])
                    else:
                        raise ValueError("Invalid type of dataframe")

                    # Check if the metric_results contain the keys defined in limits
                    if len(limits.items()) == 0:
                        report_entry["values"] = metric_results
                        report_entry["error"] = ""
                        report_entry["status"] += "."
                    for metric_key, (min_limit, max_limit) in limits.items():
                        if metric_key in metric_results:
                            report_entry["values"] = metric_results #metric_results[metric_key]
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

            table_names = list(tables.keys())
            table_names.sort()
            report["title"] = "DQ Report for tables {}".format(table_names)
            report['result'] = pd.DataFrame(report['result'])

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