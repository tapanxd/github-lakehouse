import sys, os, re
from datacontract.data_contract import DataContract
contract_location = sys.argv[1]
contract_file = sys.argv[2]
ref_name = sys.argv[3]
bundle_name = sys.argv[4]

ref_name = re.sub(r'[^A-Za-z0-9._-]', '_', ref_name)
bundle_name = re.sub(r'[^A-Za-z0-9._-]', '', bundle_name)
html_filename = f"{bundle_name}_{ref_name}.html"

print("📄 Generating markdown and HTML from {0} {1}".format(contract_location,contract_file))
abs_path = os.path.abspath(".")
print("📁 path .: {0}".format(abs_path))
source_file = os.path.join(abs_path,contract_location,contract_file)
print("📁 source_file: {0}".format(source_file))

target_file_md = os.path.join(abs_path,contract_location,contract_file.replace("yml","md").replace("yaml","md"))
print("📁 target_file_md {0}".format(target_file_md))
target_file_html = os.path.join(abs_path, contract_location, html_filename)
print("📁 target_file_html {0}".format(target_file_html))

contract = DataContract(data_contract_file=source_file)
output_md = contract.export(export_format="markdown")
output_html = contract.export(export_format="html")

with open(target_file_md, "w") as mdf:
    mdf.write(output_md)
print("Wrote Markdown ✅")
with open(target_file_html, "w") as htmlf:
    htmlf.write(output_html)
print("Wrote HTML ✅")