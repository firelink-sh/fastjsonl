import json
import sys
import os
import time

import matplotlib.pyplot as plt
import fastjsonl
import jsonschema

if __name__ == "__main__":
    with open("./example-schema2.json", "r") as f:
        schema = f.read()

    validator = jsonschema.Draft202012Validator(json.loads(schema))

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
            t_p_start = time.perf_counter()
            for line in pydata.splitlines():
                obj = json.loads(line)
                validator.validate(obj)
            t_p_elapsed = time.perf_counter() - t_p_start
            sys.stdout.write(
                f"{t_p_elapsed:>8.4f} seconds ({int(filesize) / t_p_elapsed:>8.1f} rows / second)"
            )

            py_timings.append(t_p_elapsed)

            sys.stdout.write(f"\n - [ {filesize:<9} ] RUST\t... ")
            sys.stdout.flush()
            t_r_start = time.perf_counter()
            fastjsonl.validate_jsonl(data, schema)
            t_r_elapsed = time.perf_counter() - t_r_start
            sys.stdout.write(
                f"{t_r_elapsed:>8.4f} seconds ({int(filesize) / t_r_elapsed:>8.1f} rows / second)"
            )

            rs_timings.append(t_r_elapsed)

    plt.figure(figsize=(10, 6))
    plt.plot(
        filesizes,
        py_timings,
        marker="+",
        color="tab:blue",
        label="Python (stdlib json + jsonschema)",
    )
    plt.plot(
        filesizes,
        rs_timings,
        marker="x",
        color="tab:orange",
        label="Rust (pyo3, serde_json + jsonschema)",
    )
    plt.grid(True, which="both", ls="--", lw=0.5)
    plt.title("jsonl (ndjson) parsing + validation", fontweight="bold")
    plt.xlabel("Number of rows")
    plt.ylabel("Time [s]")
    plt.legend()
    plt.tight_layout()
    plt.savefig("benchmark.png")
