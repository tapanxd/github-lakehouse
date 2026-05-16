import os, yaml, functools, operator, re, sys, argparse
# 📜 Generates code for migrating Data Contract to Unity Catalog

repository = f"https://github.com/{sys.argv[1]}" # GitHub Actions repository
print(f"Repository: {repository}")
ref_name = sys.argv[2] # GitHub Actions ref_name
print(f"ref_name: {ref_name}")

parser = argparse.ArgumentParser()

parser.add_argument("--template_file", type=str, required=True, help="File including template for non-streaming comments")
parser.add_argument("--gold_dlt_file_name", type=str, required=True, help="DLT pipeline filename where gold layer tables are published")
parser.add_argument("--import_file_name", type=str, required=True, help="Filename used for imports to both DLT&non-DLT scripts")

# Parse arguments
args = parser.parse_args(sys.argv[3:]) 



def replace_streaming_comments(file_location, table_name, streaming_comments):
    try:
        with open(os.path.join(".","src",table_name,file_location), "r+") as importFile:
            dlt_content = importFile.read()
            for streaming_comment in streaming_comments:
                dlt_content = re.sub(r'{"comment": "' + streaming_comment["name"] +  r'"}',r'{"comment": "' + streaming_comment["description"] +  r'"}', dlt_content)
                print(f"💬 Writing DLT column streaming comment in {file_location} for field {streaming_comment["name"]} with description '{streaming_comment["description"]}'")
            importFile.seek(0)
            importFile.write(dlt_content)
            importFile.truncate()
    except:
        print(f"⚠️ Warning: no {file_location} file found for replacement under {table_name} directory")

data_contract_file_odcs = os.path.join(".", "contracts", "datacontract.odcs.yml")
file_from_template = args.template_file.replace("_template","")
print(f"💻 Using contract_to_unitycatalog_template to create {file_from_template} based on data contract")
print(f"💡 Please note that streaming comments do not support a direct COMMENT ON COLUMN statement and so are added as replacements to the {args.gold_dlt_file_name} and {args.import_file_name} files in the src/your-data-product directory")

contract = None
with open(data_contract_file_odcs, "r") as f:
    contract = yaml.safe_load(f)

lines = []
tables = []
for item in contract["schema"]:
    cur_table = {}
    cur_table["name"] = item["name"]
    table_name = cur_table["name"]
    cur_table["description"] = item["description"]
    try:
        cur_table["streaming"] = [x for x in item["customProperties"] if x["property"] == "streaming"][0]["value"]
    except:
        print("Warning: streaming is a mandatory field in Data Contract")
    cur_table["streaming_comments"] = []
    cur_table["nonstreaming_comments"] = []
    tables.append(cur_table)
    print(f"💻 Identified table {table_name} in data product")
    streaming_string = 'streaming' if cur_table["streaming"] else 'non-streaming'
    print(f"💡 {table_name} is {streaming_string}")
    for field in item["properties"]:
        field_name = field["name"]
        field_description = field["description"]
        print(f"📜 Identified column {field_name} in table {table_name}")
        if cur_table["streaming"]:
            cur_field = {}
            cur_field["name"] = field_name
            cur_field["description"] = field_description
            cur_table["streaming_comments"].append(cur_field)
        else:
            cur_table["nonstreaming_comments"].append("""spark.sql(f"COMMENT ON COLUMN {catalog_name}.{gold_schema_name}.""" + table_name + "." + field_name + " IS '" + field_description + "'\")")
        print(f"💬 Added {streaming_string} comment for {field_name}")

# Update DLT pipeline table description and streaming column comments
for tbl in tables:
    table_name = tbl["name"]
    print(f"📄 Writing DLT comment for {tbl["name"]} with description '{tbl["description"]}'")
    with open(os.path.join(".","src",tbl["name"], args.gold_dlt_file_name), "r+") as dltFile:
        dlt_content = dltFile.read().replace("DATA_PRODUCT_TABLE_COMMENT", tbl["description"])
        dltFile.seek(0)
        dltFile.write(dlt_content)
        dltFile.truncate()
    replace_streaming_comments(args.import_file_name, tbl["name"], tbl["streaming_comments"])
    replace_streaming_comments(args.gold_dlt_file_name, tbl["name"], tbl["streaming_comments"])

# Adding schema tags to indicate deployment version and source.
schema_tags = {}
schema_tags["nonstreaming_comments"] = []
schema_tags["nonstreaming_comments"].append("spark.sql(f\"ALTER SCHEMA {catalog_name}.{bronze_schema_name} SET TAGS ('repo'='" + repository + "', 'ref_name'='" + ref_name + "')\")")
schema_tags["nonstreaming_comments"].append("spark.sql(f\"ALTER SCHEMA {catalog_name}.{silver_schema_name} SET TAGS ('repo'='" + repository + "', 'ref_name'='" + ref_name + "')\")")
schema_tags["nonstreaming_comments"].append("spark.sql(f\"ALTER SCHEMA {catalog_name}.{gold_schema_name} SET TAGS ('repo'='" + repository + "', 'ref_name'='" + ref_name + "')\")")
tables.append(schema_tags)

# Update non-streaming DLT column comments
all_nonstreaming = []
for t in tables:
    all_nonstreaming.append(t["nonstreaming_comments"])
ns_final = functools.reduce(operator.iconcat, all_nonstreaming, [])
replace_data = "\n".join(ns_final)
replaced_text = None
with open(os.path.join(".","src",args.template_file), "r") as readFile:
    replaced_text = readFile.read().replace("{{UNITY_CATALOG_COLUMN_COMMENTS}}",replace_data)
with open(os.path.join(".","src",file_from_template), "w") as writeFile:
    writeFile.write(replaced_text)
    writeFile.truncate()