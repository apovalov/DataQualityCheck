"""Metrics."""

from typing import Any, Dict, Union, List
from dataclasses import dataclass

import pandas as pd
import pyspark.sql as ps
import numpy as np

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


@dataclass
class CountZeros(Metric):
    """Number of zeros in choosen column"""

    column: str

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        n = len(df)
        k = sum(df[self.column] == 0)
        return {"total": n, "count": k, "delta": k / n}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col, count
        n = df.count()
        k = df.filter(col(self.column) == 0).count()
        return {"total": n, "count": k, "delta": k / n}


@dataclass
class CountNull(Metric):
    """Number of rows with null values in specified columns"""

    columns: List[str]
    aggregation: str = "any"  # "All" or "Any"

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, float]:
        n = len(df)
        if self.aggregation == "all":
            mask = df[self.columns].isna().all(axis=1)
        elif self.aggregation == "any":
            mask = df[self.columns].isna().any(axis=1)
        else:
            raise ValueError("Invalid variant. Choose 'all' or 'any'.")

        k = mask.sum()
        return {"total": n, "count": k, "delta": k / n}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, float]:
        from pyspark.sql.functions import col, isnan

        # Calculate the total number of rows in the DataFrame
        n = df.count()

        # Create a list of conditions for each column to check for null values
        conditions = [col(col_name).isNull() | isnan(col(col_name)) for col_name in self.columns]

        # Combine the conditions using OR or AND based on the aggregation parameter
        combined_condition = conditions[0]
        for condition in conditions[1:]:
            combined_condition = combined_condition | condition if self.aggregation == "any" else combined_condition & condition

        # Filter the DataFrame based on the combined condition and count the matching rows
        k = df.where(combined_condition).count()

        # Calculate the proportion of rows with null values in any or all of the specified columns
        delta = k / n

        return {"total": n, "count": k, "delta": delta}

@dataclass
class CountDuplicates(Metric):
    """Number of duplicates in chosen columns"""

    columns: List[str]

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        # Calculate the total number of rows in the DataFrame
        n = len(df)

        # Count the number of duplicate rows based on the mask
        k = n - len(df.drop_duplicates(subset=self.columns))

        # Calculate the proportion of duplicate rows
        delta = k / n

        return {"total": n, "count": k, "delta": delta}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col

        # Calculate the total number of rows in the DataFrame
        n = df.count()

        # Group by the chosen columns and count the occurrences of each group
        group_cols = [col(col_name) for col_name in self.columns]
        grouped_df = df.groupBy(*group_cols).count()

        # Filter the groups where count is greater than 1 (indicating duplicates) and count the number of duplicate rows
        k = grouped_df.where(col("count") > 1).count()

        # Calculate the proportion of duplicate rows
        delta = k / n

        return {"total": n, "count": k, "delta": delta}


@dataclass
class CountLag(Metric):
    """A lag between latest date and today"""

    column: str
    fmt: str = "%Y-%m-%d"

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        # Convert the column to datetime format
        df[self.column] = pd.to_datetime(df[self.column], format=self.fmt)

        # Find the latest date in the column
        last_day = df[self.column].max()

        # Calculate the lag between the latest date and today
        today = pd.to_datetime("today").normalize()
        lag = today - last_day

        return {"today": today.strftime(self.fmt), "last_day": last_day.strftime(self.fmt), "lag": lag.days}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col, max
        from datetime import datetime

        df = df.withColumn(self.column, col(self.column).cast("date"))

        # Находим последнюю дату в столбце
        last_day_row = df.select(max(self.column)).first()
        last_day = last_day_row[0]

        # Вычисляем лаг между последней датой и сегодняшним днем
        today = datetime.now().date()
        lag = today - last_day

        return {"today": today.strftime(self.fmt), "last_day": last_day.strftime(self.fmt), "lag": lag.days}

@dataclass
class CountValue(Metric):
    """Number of values in choosen column"""

    column: str
    value: Union[str, int, float]

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        n = len(df)
        k = (df[self.column] == self.value).sum()

        return {"total": n, "count": k, "delta": k / n}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col

        # Calculate the total number of rows in the DataFrame
        n = df.count()
        k = df.filter(col(self.column) == self.value).count()

        return {"total": n, "count": k, "delta": k / n}

@dataclass
class CountBelowValue(Metric):
    """Number of values below threshold"""

    column: str
    value: float
    strict: bool = False

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        n = len(df)

        if self.strict:
            below_trashhold_mask = df[self.column] < self.value
        else:
            below_trashhold_mask = df[self.column] <= self.value

        k = sum(below_trashhold_mask)
        return {"total": n, "count": k, "delta": k / n}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col

        # Calculate the total number of rows in the DataFrame
        n = df.count()

        if self.strict:
            condition = col(self.column) < self.value
        else:
            condition = col(self.column) <= self.value

        k = df.where(condition).count()

        return {"total": n, "count": k, "delta": k / n}

@dataclass
class CountBelowColumn(Metric):
    """Count how often column X below Y"""

    column_x: str
    column_y: str
    strict: bool = False

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        n = len(df)
        if self.strict:
            below_trashhold_mask = df[self.column_x] < df[self.column_y]
        else:
            below_trashhold_mask = df[self.column_x] <= df[self.column_y]

        k = sum(below_trashhold_mask)
        return {"total": n, "count": k, "delta": k / n}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col, isnan

        # Calculate the total number of rows in the DataFrame
        n = df.count()

        # Create conditions for each column to check for non-null and non-NaN values
        x_not_null_condition = col(self.column_x).isNotNull()
        y_not_null_condition = col(self.column_y).isNotNull()

        x_not_nan_condition = ~isnan(col(self.column_x))
        y_not_nan_condition = ~isnan(col(self.column_y))

        # Combine conditions to filter out rows where X or Y are null or NaN
        non_null_condition = x_not_null_condition & y_not_null_condition
        non_nan_condition = x_not_nan_condition & y_not_nan_condition

        # Combine the conditions using AND to check both ratio condition and non-null/non-NaN condition
        condition = non_null_condition & non_nan_condition

        if self.strict:
            below_condition = col(self.column_x) < col(self.column_y)
        else:
            below_condition = col(self.column_x) <= col(self.column_y)

        # Filter the DataFrame based on the combined condition and count the matching rows
        k = df.where(condition & below_condition).count()

        return {"total": n, "count": k, "delta": k / n}


@dataclass
class CountRatioBelow(Metric):
    """Count how often X / Y below Z"""

    column_x: str
    column_y: str
    column_z: str
    strict: bool = False

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        n = len(df)
        if self.strict:
            below_trashhold_mask = df[self.column_x] / df[self.column_y] < df[self.column_z]
        else:
            below_trashhold_mask = df[self.column_x] / df[self.column_y] <= df[self.column_z]

        k = sum(below_trashhold_mask)
        return {"total": n, "count": k, "delta": k / n}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import col, isnan
        # Calculate the total number of rows in the DataFrame
        n = df.count()

        # Create conditions for each column to check for non-null and non-NaN values
        x_not_null_condition = col(self.column_x).isNotNull()
        y_not_null_condition = col(self.column_y).isNotNull()
        z_not_null_condition = col(self.column_z).isNotNull()

        x_not_nan_condition = ~isnan(col(self.column_x))
        y_not_nan_condition = ~isnan(col(self.column_y))
        z_not_nan_condition = ~isnan(col(self.column_z))

        # Combine conditions to filter out rows where X, Y, or Z are null or NaN
        non_null_condition = x_not_null_condition & y_not_null_condition & z_not_null_condition
        non_nan_condition = x_not_nan_condition & y_not_nan_condition & z_not_nan_condition

        # Combine the conditions using AND to check both ratio condition and non-null/non-NaN condition
        condition = non_null_condition & non_nan_condition

        if self.strict:
            ratio_condition = col(self.column_x) / col(self.column_y) < col(self.column_z)
        else:
            ratio_condition = col(self.column_x) / col(self.column_y) <= col(self.column_z)

        # Combine the conditions using AND to check both ratio condition and non-null condition
        condition = ratio_condition & condition

        k = df.where(condition).count()
        # print('CountRatioBelow_spark')
        # print("Total", n, "count", k, "delta", k / n)

        return {"total": n, "count": k, "delta": k / n}

@dataclass
class CountCB(Metric):
    """Calculate lower/upper bounds for N%-confidence interval"""
    column: str
    conf: float = 0.95

    def _call_pandas(self, df: pd.DataFrame) -> Dict[str, Any]:
        delta = (1 - self.conf) / 2
        data = df[self.column].dropna()
        ucb = np.percentile(data, (1 - delta) * 100)
        lcb = np.percentile(data, (delta) * 100)
        return {"lcb": lcb, "ucb": ucb}

    def _call_pyspark(self, df: ps.DataFrame) -> Dict[str, Any]:
        from pyspark.sql.functions import expr
        delta = (1 - self.conf) / 2

        # Use PySpark's built-in functions to calculate percentiles
        expr_ucb = expr(f"percentile_approx({self.column}, {(1 - delta)})")
        expr_lcb = expr(f"percentile_approx({self.column}, {delta})")

        # Calculate upper and lower bounds directly in PySpark
        df = df.dropna()
        ucb = df.select(expr_ucb).collect()[0][0]
        lcb = df.select(expr_lcb).collect()[0][0]

        return {"lcb": lcb, "ucb": ucb}


# data = pd.read_csv('sales_origin.csv')

# df = pd.DataFrame(data)
# df.loc[0] = ['2022-10-23 ', 200, None, 100, None]
# df.loc[1] = ['2022-10-23 ', 200, 0, 100, None]
# # df.loc[2] = ['2022-10-23 ', 200, 6, 100, None]
# # df.loc[3] = ['2022-10-23 ', 200, 8, 0, None]
# # df.loc[4] = ['2022-10-23 ', 200, 8, 0, None]
# # df.loc[5] = ['2022-10-23 ', 200, 8, 0, None]
# # df.loc[6] = ['2022-10-23 ', 200, 8, 100, None]
# df.loc[8] = ['2022-10-23 ', 200, 8, 0, None]

# # print('\n')
# # print(df.head)
# # print('////////////////////////////////')

# from pyspark.sql import SparkSession
# # from user_input.metrics import CountZeros

# # # Initialize Spark session
# spark = SparkSession.builder.appName("dq_report").getOrCreate()
# print('////////// Spark df ///////////')
# df_spark = spark.createDataFrame(df, ['day', 'item_id', 'qty', 'price', 'revenue'])

# # print(df.show(10))


# # # columns = ['day', 'item_id', 'qty', 'price', 'revenue']
# columns = ['qty']
# column = 'day'
# # # mask = df[columns].isna().any(axis=0)
# # # print('Mask: \n', mask)

# count_lag = CountLag(column=column)
# print('Count_lag:', count_lag(df_spark))

# # columns = ['day', 'item_id']
# # count_dub = CountDuplicates(columns=columns)
# # print('Count_lag:', count_dub(df))

# # columns = ['qty', 'item_id']
# # count_null = CountNull(columns=columns)
# # print('Count_null:', count_null(df))


# columns = ['qty', 'item_id']
# count_null = CountCB(column='price')
# print('Count_cb:', count_null(df))


# columns = ['qty', 'item_id']


# count_null = CountNull(columns=['qty'])

# print('Count_null_spark:', count_null(df_spark))
# print('Count_null_pandas:', count_null(df))


# count_below = CountBelowColumn(column_x='item_id',
#                               column_y = 'price')

# print('Count_below_spark:', count_below(df_spark))
# print('Count_below_pandas:', count_below(df))




# count_zeros = CountZeros(column=column)
# print('count_zeros:', count_zeros(df))