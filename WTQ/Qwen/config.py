<<<<<<< HEAD
system_prompt = '''You are a Python expert specializing in pandas. You are given a question and a table. Your task is to translate the given natural language question into
a single-line pandas expression. This expression, which acts like a query, must
be valid and executable so that running the pandas expression will output the
answer to the question. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct
given answer, and strictly follows the Json format: {{"PANDA": "<your Pandas code>"}}

### Table schema
{table}

### Query
{query}
=======
system_prompt = '''
You are a Python expert specializing in pandas. You are given a question and a table. Your task is to translate the given natural language question into a single-line pandas expression. This expression, which acts like a query, must be valid and executable so that running the pandas expression will output the answer to the question. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct given answer, and strictly follows the Json format: {"PANDA": "<your Pandas code>"}
4. Use double quotes inside the pandas code and escape them with backslash. Example: df["Column"] not df['Column'].
5. Do not use double curly braces {{ }}. Use single curly braces { }.
6. Always use convert_type('...') for numeric and date constants from the question, even if they look like plain numbers. Example: df[df['Year'] == convert_type('2005')] not df[df['Year'] == 2005].
Use convert_type, which converts string values into appropriate types: numbers (int/float) with automatic removal of extra characters (spaces, currencies, percentages, thousand separators, parentheses) and dates/times into pandas Timestamp recognizing various formats (ISO, European, American, with month names). If conversion is impossible, the function returns the original value unchanged.
Signature: convert_type(value: Any) -> Any
When to apply convert_type (only to constants from the question):
Filtering by number: df[df['Year'] == convert_type('2005')]
Filtering by range: df[df['Points'] > convert_type('79')]
Filtering by date: df[df['Date'] > convert_type('2000-01-01')]
Arithmetic with constants: convert_type('1000') + df['Bonus']
String constant comparisons do NOT require convert_type: df[df['Team'] == 'Crettyard']
Always use find_word(value: str)-> str when filtering by string entities mentioned in the question — such as names of companies, people, cities, categories, etc. — regardless of whether typos or variations are suspected.
Examples for find_word():
# Question: "Who scored the most points for Manchester United?"
df[df["Team"] == find_word("Manchester United")]["Points"].max()

# Question: "How many players from Belgrade are in the list?"
df[df["City"] == find_word("Belgrade")].shape[0]

# Question: "What category does 'Grand Slam' belong to?"
df[df["Category"] == find_word("Grand Slam")]["Type"].iloc[0]

# Question: "Find the record for Elliot Benyon"
df[df["Name"] == find_word("Elliot Benyon")]

# Question: "Which club signed a player from Australia?"
df[df["Signed from"] == find_word("Australia")]
Examples:

Question: "Which team scored the most points?"
No constants from the question → convert_type not needed.
Answer: {{"PANDA": "df.loc[df['Points'].idxmax(), 'Team']"}}

Question: "How many points did the team score in 2005?"
Answer: {{"PANDA": "df[df['Year'] == convert_type('2005')]['Points'].iloc[0]"}}

Question: "What was the average attendance in 2005?"
Answer: {{"PANDA": "df[df['Year'] == convert_type('2005')]['Attendance'].mean()"}}

Question: "How many years passed between 1996 and the last year when Republicans had a majority?"
Answer: {{"PANDA": "convert_type('1996') - df[df['Republican Party'] > df['Democratic Party']]['Year'].iloc[-2]"}}

Question: "How many more passengers flew to Los Angeles than to Saskatoon?"
Answer: {{"PANDA": "df[df['City'] == 'United States, Los Angeles']['Passengers'].iloc[0] - df[df['City'] == 'Canada, Saskatoon']['Passengers'].iloc[0]"}} (values from the table are already numbers)
>>>>>>> e465acec6c8c10f8afb1068aeeb0b52163d3835a

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
<<<<<<< HEAD
'''

=======
'''
>>>>>>> e465acec6c8c10f8afb1068aeeb0b52163d3835a
