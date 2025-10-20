import fastjsonl
import polars as pl
import pyarrow as pa


if __name__ == "__main__":
    with open("./example-schema2.json", "r") as f:
        json_schema = f.read()

    arrow_schema = pa.schema(
        [
            pa.field("a", pa.int64()),
            pa.field("b", pa.string()),
        ]
    )

    rb = fastjsonl.test(b"", json_schema, arrow_schema)
    df = pl.from_arrow(rb)
    print(df.head())
