# Langfuse-Trace-Importer

A Python script for importing exported Langfuse traces back into Langfuse with corrected json, preserving the complete trace structure, observations, and conversation history. This is especially useful for traces that include tool calls—without this importer, such traces cannot be used in the Langfuse Playground due to a json formatting bug (camelCase vs snake_case, ie. toolCall vs. tool_call). This script fixes the JSON so these traces become fully usable again.

Note: Langfuse can sometimes take up to a few minutes to process newly imported traces

## Instalation/Requirements
- Download trace_importer_fixed.py
- pip install requests python-dotenv
- Create a .env file in the same directory:
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...

## Usage
Download an existing trace from Langfuse using the button in Log View:

![Screenshot](DownloadButton.png)


#### Basic Usage
python trace_importer_fixed.py trace_export.json
#### With Command Line Credentials
python trace_importer_fixed.py trace_export.json \
  --public-key pk-lf-... \
  --secret-key sk-lf-... \
  --host https://us.cloud.langfuse.com
