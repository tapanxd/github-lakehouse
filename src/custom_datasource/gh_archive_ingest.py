# Databricks notebook source
# 💡 Refernece implementation for how to call APIs in a reliable manner, fully integrated with Spark Structured Streaming.
from datetime import datetime, date, timedelta
from typing import Dict, List, Iterator, Tuple, Any
import json, requests
from pyspark.sql.types import *
from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition


# 💡 A "partition" contains all information required for an API call from an executor.
# ⚠️ Partitions for API calls are how Spark Structured Streaming divides requests among executors.
# This is 🚨not🚨 the same as Delta table storage partitions.
class WeatherPartition(InputPartition):
    def __init__(self, name: str, lat: float, lon: float, start_date: str, end_date: str):
        self.name = name
        self.lat = lat
        self.lon = lon
        self.start_date = start_date
        self.end_date = end_date

class WeatherStreamReader(DataSourceStreamReader):
    def __init__(self, schema: StructType, options: Dict[str, Any]):
        self.schema = schema
        self.options = options or {}
        self.session = requests.Session()
        self.locations: List[Dict[str, float]] = json.loads(self.options.get("locations", "[]"))
        self.endpoint: Dict[str, str] = json.loads(self.options.get("endpoint", "{}"))
        default_minimum = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        self.initial_start_date: date = datetime.strptime(
            self.options.get("start_date", default_minimum), "%Y-%m-%d"
        ).date()

        self.max_retries: int = int(self.options.get("max_retries", 3))
        self.backoff_sec: float = float(self.options.get("backoff_sec", 1.0))

    # This is what the Data Source should use as an initial starting point.
    # Here we make it dynamic so that the Open Meteo API doesn't complain about us retrieving data from too long ago.
    def initialOffset(self) -> Dict[str, str]:
        initial_ts = (self.initial_start_date).isoformat()

        return {
            loc['name']: initial_ts
            for loc in self.locations
        }

    # ⚠️ Important ⚠️
    # This defines the latest possible offset available for the stream.
    # Spark Structured Streaming will catch up to the latest possible offset and divide that work into "partitions" as defined in `partitions`
    # If you want to limit the amount of content per call, you can persist additional state in Volumes using the `commit` statment (e.g. to only receive X records per request)
    # Generally, though, it is preferable to maintain less state and let Spark Structured Streaming handle the catch-up process using partitions.
    def latestOffset(self) -> Dict[str, str]:
        now_iso = datetime.now().isoformat()
        return {
            loc['name']: now_iso
            for loc in self.locations
        }

    # Offsets look like this so we need to parse them:
    # v1
    # {"batchWatermarkMs":0,"batchTimestampMs":1763669344627,"conf":{"spark.sql.streaming.stateStore.providerClass":"com.databricks.sql.streaming.state.RocksDBStateStoreProvider","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.stateStore.encodingFormat":"unsaferow","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.stateStore.rowChecksum.enabled":"false","spark.databricks.optimizer.joinOrToUnionAllExpansionWithEquiJoin.streaming":"false","spark.sql.streaming.dropDuplicates.stateFormatVersion":"1","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.databricks.optimizer.convertCollectSetToCollectlistDistinct.enableStreaming":"false","spark.sql.streaming.failStatefulQuerySchemaEvolution":"2","spark.sql.shuffle.partitions":"200","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.optimizer.pruneFiltersCanPruneStreamingSubplan":"false"}}
    # {"offset_43.70455_-79.404625": "2025-11-20T20:09:04.627434", "offset_49.2827_-123.1207": "2025-11-20T20:09:04.627434"}
    def _parse_offset(self, offset: Any) -> Dict[str, str]:
        if isinstance(offset, dict):
            return offset

        if isinstance(offset, str):
            lines = offset.strip().split('\n')
            if len(lines) >= 2:
                try:
                    return json.loads(lines[-1])
                except json.JSONDecodeError:
                    return {}
            elif len(lines) == 1:
                try:
                    return json.loads(lines[0]) if lines[0].strip() else {}
                except json.JSONDecodeError:
                    return {}

        return {}

    # ⚠️ Important ⚠️
    # Partitions for API calls are how Spark Structured Streaming divides requests among executors.
    # This is 🚨not🚨 the same as Delta table storage partitions.
    # Here we divide API calls by days and locations between the last offset and the latest offset.
    def partitions(self, start: Dict[str, str], end: Dict[str, str]) -> List[InputPartition]:
        start_offsets = self._parse_offset(start)
        end_offsets = self._parse_offset(end)
        partitions: List[InputPartition] = []

        for loc in self.locations:
            name = loc["name"]
            lat = loc["latitude"]
            lon = loc["longitude"]
            actual_start = datetime.fromisoformat(start_offsets.get(name))
            actual_end = datetime.fromisoformat(end_offsets.get(name))
            while actual_start < actual_end:
                next_hour = actual_start + timedelta(hours=1)
                if next_hour > actual_end:
                    break
                
                partition = WeatherPartition(
                    name,
                    lat, 
                    lon, 
                    actual_start.isoformat(), 
                    next_hour.isoformat()
                )
                partitions.append(partition)
                actual_start = next_hour
                print(f"No data to process for partition {name}: {actual_start} to {actual_end}")
        return partitions

    # This function is where the actual API calls occur.
    # All information needed for an API call should be included in the partition, since this runs on an executor.
    def read(self, partition: WeatherPartition) -> Iterator[Tuple]:
        start_date = datetime.fromisoformat(partition.start_date)
        end_date = datetime.fromisoformat(partition.end_date)
        duration_hours = (end_date - start_date).total_seconds() / 3600

        api_start_str = start_date.strftime("%Y-%m-%dT%H:00")

        if duration_hours >= 24:
            adjust_end = end_date - timedelta(hours=1)
            api_end_str = adjust_end.strftime("%Y-%m-%dT%H:00")
        else:
            api_end_str = api_start_str

        url = (
            f"{self.endpoint.get('url')}?"
            f"latitude={partition.lat}&longitude={partition.lon}"
            f"&start_hour={api_start_str}&end_hour={api_end_str}"
            f"&hourly=temperature_2m"
        )

        api_response = self.session.get(url, timeout=30).json()

        for idx, time in enumerate(api_response['hourly']['time']):
            yield (
                self.endpoint.get("source", ""),
                datetime.fromisoformat(time),
                api_response['hourly']['temperature_2m'][idx],
                api_response['hourly_units']['temperature_2m'],
                partition.name,
                api_response.get("latitude"),
                api_response.get("longitude"),
                api_response.get("elevation"),
                api_response.get("generationtime_ms"),
                api_response.get("utc_offset_seconds"),
                api_response.get("timezone"),
                api_response.get("timezone_abbreviation"),
            )

class WeatherDataSource(DataSource):
    @classmethod
    # This name is what's referenced in the `.format(...)` option on the spark.readStream command
    def name(cls) -> str:
        return "open_meteo"

    def schema(self) -> StructType:
        return StructType([
            StructField("source", StringType(), True),
            StructField("time", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("location_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("elevation", DoubleType(), True),
            StructField("generationtime_ms", DoubleType(), True),
            StructField("utc_offset_seconds", IntegerType(), True),
            StructField("timezone", StringType(), True),
            StructField("timezone_abbreviation", StringType(), True),
        ])

    # There are non-streaming versions of this as well.
    # See further documentation here: https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
    # And examples here: https://github.com/allisonwang-db/pyspark-data-sources
    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        return WeatherStreamReader(schema, self.options)

# COMMAND ----------
catalog_name = dbutils.widgets.get("catalog_name")
bronze_schema_name = dbutils.widgets.get("bronze_schema_name")
spark.dataSource.register(WeatherDataSource)

# COMMAND ----------
source = "open_meteo"
endpoint = {"url": "https://api.open-meteo.com/v1/forecast", "source": source}
table_name = f"{source}_api"
locations = [
    {"name": "Toronto", "latitude": 43.70455, "longitude": -79.404625},
    {"name": "Vancouver", "latitude": 49.2827, "longitude": -123.1207},
]
partition_cols = ["latitude", "longitude"]
checkpoint_location = f"/Volumes/{catalog_name}/{bronze_schema_name}/autoload_source/{table_name}/_checkpoint/"

query = (
    spark.readStream
        .format(source)
        .option("locations", json.dumps(locations))
        .option("endpoint", json.dumps(endpoint))
        .load()
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .partitionBy(partition_cols) 
        .toTable(f"{catalog_name}.{bronze_schema_name}.{table_name}")
)