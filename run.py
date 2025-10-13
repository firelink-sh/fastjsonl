import fastjsonl

with open("./example-schema.json", "r") as f:
    schema = f.read()

with open("./example-data.jsonl", "rb") as f:
    data = f.read()

fastjsonl.validate_jsonl(data, schema)
