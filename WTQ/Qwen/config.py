system_prompt = '''You are a Python expert specializing in pandas. You are given a question and a table. Your task is to translate the given natural language question into
a single-line pandas expression. This expression, which acts like a query, must
be valid and executable so that running the pandas expression will output the
answer to the question. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct
given answer, and strictly follows the Json format: {{"PANDA": "<your Pandas code>"}}

If column names have spaces or special characters, use df['column name']
Use pd.to_datetime() for date comparisons
Return the actual value (not index or position)

### Table Schema:
{table}

### Column Data Types:
{column_types}

### Query:
{query}
'''

logic_prompt = '''You are an expert in Python with a specialization in pandas. Your task is to verify and correct a given pandas code that translates a natural language statement into a pandas expression. The corrected pandas code must accurately evaluate the truth of the statement when applied to the given table. Requirements:

The table is represented as a pandas DataFrame named df.

The pandas code must evaluate to a value using the snippet: (eval(pandas_code)). The result can be boolean, number, string, date, or any type that matches the expected answer.

The corrected pandas code must match the value indicated by the provided "Label".

Ensure the output is concise, correct, and when run outputs the answer, and strictly in the following JSON format with a single key "PANDA": "CORRECT PANDA": "<your pandas code>"

### Table schema
{table}

### Pandas code
{pandas}

### Label
{label}
'''

correct_prompt = '''You are an expert in Python, specializing in pandas. Your task is to correct a pandas code that translates a given natural language statement into a pandas expression. The code, along with the specific error it contains, is provided. Your corrected pandas_code must be valid and executable when running the code snippet eval(pandas_code), ensuring it accurately evaluates the statement using the provided table with no errors.

The pandas_code can return any type (boolean, number, string, date, etc.) that matches the expected answer. Consider the following:

The table is represented as a pandas DataFrame named df.

Do not include explanations, comments, or multiline outputs.

Ensure the output is concise, correct, and when run outputs the answer, and strictly in the following JSON format with a single key "PANDA": "<your pandas code>"

### Table schema
{table}

### Pandas code
{pandas}

### Label
{label}
'''