import json
import io
import csv
import sys
import os
import time

import matplotlib.pyplot as plt
import pyarrow as pa
import polars as pl
import fastjsonl
import jsonschema


if __name__ == "__main__":
    with open("./example-schema2.json", "r") as f:
        json_schema_str = f.read()
        json_schema = json.loads(json_schema_str)

    columns = json_schema["properties"].keys()
    validator = jsonschema.Draft202012Validator(json_schema)

    arrow_schema = pa.schema(
        [
            pa.field("user_id", pa.int64()),
            pa.field("username", pa.string()),
            pa.field("last_login", pa.string()),
            pa.field("banned", pa.bool_()),
            pa.field("friends", pa.string()),
        ]
    )

    files = os.listdir(".")

    filesizes = []
    py_timings = []
    rs_timings = []

    for i, file in enumerate(files, start=1):
        if ".jsonl" in file:
            filesize = file.split("-")[2].split(".")[0]
            filesizes.append(int(filesize))

            with open(file, "rb") as f:
                data = f.read()

            pydata = data.decode("utf-8")
            sys.stdout.write(f"\n - [ {filesize:<9} ] PYTHON\t... ")
            sys.stdout.flush()

            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(col for col in columns)

            t_p_start = time.perf_counter()
            for j, line in enumerate(pydata.splitlines(), start=1):
                obj = json.loads(line)
                validator.validate(obj)
                _ = writer.writerow(obj.get(field, "") for field in columns)

            buffer.seek(0)
            df = pl.read_csv(source=buffer)
            t_p_elapsed = time.perf_counter() - t_p_start
            assert len(df) == int(filesize)
            sys.stdout.write(
                f"{t_p_elapsed:>8.4f} seconds ({int(filesize) / t_p_elapsed:>9.1f} rows / second)"
            )

            py_timings.append(t_p_elapsed)

            sys.stdout.write(f"\n - [ {filesize:<9} ] RUST\t... ")
            sys.stdout.flush()

            t_r_start = time.perf_counter()
            rb = fastjsonl.jsonl_to_arrow(data, json_schema_str, arrow_schema)
            df = pl.from_arrow(rb)
            t_r_elapsed = time.perf_counter() - t_r_start
            assert len(df) == int(filesize)
            sys.stdout.write(
                f"{t_r_elapsed:>8.4f} seconds ({int(filesize) / t_r_elapsed:>9.1f} rows / second)"
            )

            rs_timings.append(t_r_elapsed)

    plt.figure(figsize=(10, 6))
    plt.plot(
        filesizes,
        py_timings,
        marker="+",
        color="tab:blue",
        label="Python (stdlib json + jsonschema + stdlib csv)",
    )
    plt.plot(
        filesizes,
        rs_timings,
        marker="x",
        color="tab:orange",
        label="Rust (pyo3, serde_json + jsonschema-rs + arrow)",
    )
    plt.grid(True, which="both", ls="--", lw=0.5)
    plt.title(
        "jsonl (ndjson) parsing + validation => polars.DataFrame", fontweight="bold"
    )
    plt.xlabel("Number of rows")
    plt.ylabel("Time [s]")
    plt.legend()
    plt.tight_layout()
    plt.savefig("benchmark_full.png")
