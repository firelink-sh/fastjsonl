import json
import fastjsonl
import polars as pl
import pyarrow as pa


if __name__ == "__main__":
    json_schema = json.dumps(
        {
            "properties": {
                "a": {
                    "type": "number",
                },
                "b": {
                    "type": "string",
                },
            },
        }
    )

    arrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
        ]
    )

    obj = {"a": 12341, "b": "cool?"}

    rb = fastjsonl.test(json.dumps(obj).encode("utf-8"), json_schema, arrow_schema)
    df = pl.from_arrow(rb)
    print(df.head())
