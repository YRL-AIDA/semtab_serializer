generate_prompt = '''
You are a Python expert specializing in pandas. You are given a question and a table. Your task is to translate the 
given natural language question into a single-line pandas expression. This expression, which acts like a query, 
must be valid and executable so that running the pandas expression will output the answer to the question. Consider the following:
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

'''

logic_prompt = '''
You are an expert in Python, specializing in pandas. Your task is to correct a pandas code that translates a given natural language question into a pandas expression. You are provided with: the question, the table (in serialized form), and an incorrect pandas code that contains a logical error and returns an incorrect result. Your corrected pandas_code must be valid and executable with eval(pandas_code), and must return the correct answer to the given question, using the provided table without any errors.

The pandas_code can return any type (boolean, number, string, date, etc.) that matches the expected answer. Consider the following:

1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct answer, and strictly follows the JSON format: {"PANDA": "<your Pandas code>"}
4. Use double quotes inside the pandas code and escape them with backslash. Example: df["Column"] not df['Column'].
5. Do not use double curly braces {{ }}. Use single curly braces { }.
6. Always use convert_type('...') for numeric and date constants from the question, even if they look like plain numbers. Example: df[df['Year'] == convert_type('2005')] not df[df['Year'] == 2005].
7. Always use find_word(value: str) -> str when filtering by string entities mentioned in the question — such as names of companies, people, cities, categories, etc. — regardless of whether typos or variations are suspected.

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
'''

correct_prompt = '''You are an expert in Python, specializing in pandas. Your task is to correct a pandas code that translates a given natural language statement into a pandas expression. The code, along with the specific error it contains, is provided. Your corrected pandas_code must be valid and executable when running the code snippet eval(pandas_code), ensuring it accurately evaluates the statement using the provided table with no errors.

The pandas_code can return any type (boolean, number, string, date, etc.) that matches the expected answer. Consider the following:

1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct given answer, and strictly follows the Json format: {"PANDA": "<your Pandas code>"}
4. Use double quotes inside the pandas code and escape them with backslash. Example: df["Column"] not df['Column'].
5. Do not use double curly braces {{ }}. Use single curly braces { }.
6. Always use convert_type('...') for numeric and date constants from the question, even if they look like plain numbers. Example: df[df['Year'] == convert_type('2005')] not df[df['Year'] == 2005].
Ensure the output is concise, correct, and when run outputs the answer, and strictly in the following JSON format with a single key "PANDA": "<your pandas code>"
You need to read the table, question, answer, and pandas code and return a corrected version of the pandas code
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


'''

sql_pandas_prompt = '''
You are a Python expert specializing in converting SQL queries to pandas code. You are given the original question, 
available columns, the table, and a SQL query. The SQL query produces the correct answer, and your primary task is to 
translate the SQL query into pandas, using the original question as self-validation. Your task is to translate the given 
SQL query into a single-line pandas expression. This expression, acting like a query, must be valid and executable so that 
running the pandas expression will produce the same result as the SQL query. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct given answer, and strictly follows the JSON format: {"PANDA": "<your Pandas code>"}
4. Use double quotes inside the pandas code and escape them with backslash. Example: df["Column"] not df['Column'].
5. Do not use double curly braces {{ }}. Use single curly braces { }.
6. Always use convert_type('...') for numeric and date constants from the SQL query, even if they look like plain numbers. Example: df[df['Year'] == convert_type('2005')] not df[df['Year'] == 2005].
Use convert_type, which converts string values into appropriate types: numbers (int/float) with automatic removal of extra characters (spaces, currencies, percentages, thousand separators, parentheses) and dates/times into pandas Timestamp recognizing various formats (ISO, European, American, with month names). If conversion is impossible, the function returns the original value unchanged.
Signature: convert_type(value: Any) -> Any
When to apply convert_type (only to constants from the SQL query):
- Filtering by number: df[df['Year'] == convert_type('2005')]
- Filtering by range: df[df['Points'] > convert_type('79')]
- Filtering by date: df[df['Date'] > convert_type('2000-01-01')]
- Arithmetic with constants: convert_type('1000') + df['Bonus']
String constant comparisons do NOT require convert_type: df[df['Team'] == 'Crettyard']
Always use find_word(value: str) -> str when filtering by string entities mentioned in the SQL query — such as names of companies, people, cities, categories, etc. — regardless of whether typos or variations are suspected.
Examples for find_word():
# SQL: SELECT * FROM df WHERE Team = 'Manchester United'
df[df["Team"] == find_word("Manchester United")]

# SQL: SELECT MAX(Points) FROM df WHERE Team = 'Manchester United'
df[df["Team"] == find_word("Manchester United")]["Points"].max()

# SQL: SELECT COUNT(*) FROM df WHERE City = 'Belgrade'
df[df["City"] == find_word("Belgrade")].shape[0]

# SQL: SELECT Type FROM df WHERE Category = 'Grand Slam' LIMIT 1
df[df["Category"] == find_word("Grand Slam")]["Type"].iloc[0]

SQL query: {sql_query}
Table schema: {table_schema}
Original question: {question}
'''

sql_pandas_correct_prompt = '''
You are a Python expert specializing in converting SQL queries to pandas code. You are given the original question, 
available columns, the table, a SQL query, a previous incorrect pandas code, and its error. The SQL query produces the 
correct answer. Your task is to translate the SQL query into a single-line pandas expression, using the previous 
incorrect code and error to avoid repeating mistakes. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Output must be valid JSON: {"PANDA": "<your Pandas code>"}
4. Use double quotes inside pandas code, escaped with backslash: df["Column"]
5. Use convert_type('...') for numeric/date constants from SQL.
6. Use find_word('...') for string entity filtering.
Use convert_type, which converts string values into appropriate types: numbers (int/float) with automatic removal of extra characters (spaces, currencies, percentages, thousand separators, parentheses) and dates/times into pandas Timestamp recognizing various formats (ISO, European, American, with month names). If conversion is impossible, the function returns the original value unchanged.
Signature: convert_type(value: Any) -> Any
When to apply convert_type (only to constants from the SQL query):
- Filtering by number: df[df['Year'] == convert_type('2005')]
- Filtering by range: df[df['Points'] > convert_type('79')]
- Filtering by date: df[df['Date'] > convert_type('2000-01-01')]
- Arithmetic with constants: convert_type('1000') + df['Bonus']
String constant comparisons do NOT require convert_type: df[df['Team'] == 'Crettyard']
Always use find_word(value: str) -> str when filtering by string entities mentioned in the SQL query — such as names of companies, people, cities, categories, etc. — regardless of whether typos or variations are suspected.
Examples for find_word():
# SQL: SELECT * FROM df WHERE Team = 'Manchester United'
df[df["Team"] == find_word("Manchester United")]

# SQL: SELECT MAX(Points) FROM df WHERE Team = 'Manchester United'
df[df["Team"] == find_word("Manchester United")]["Points"].max()

# SQL: SELECT COUNT(*) FROM df WHERE City = 'Belgrade'
df[df["City"] == find_word("Belgrade")].shape[0]

# SQL: SELECT Type FROM df WHERE Category = 'Grand Slam' LIMIT 1
df[df["Category"] == find_word("Grand Slam")]["Type"].iloc[0]

SQL query: {sql_query}
Table schema: {table_schema}
Original question: {question}
'''

sql_pandas_logic_prompt = '''
You are a Python expert specializing in converting SQL queries to pandas code. You are given the original question, 
available columns, the table, a SQL query, a previous incorrect pandas code, and the error it produced. 
The SQL query produces the correct answer. Your primary task is to translate the SQL query into a single-line pandas expression, 
using the previous incorrect code and its error to avoid repeating mistakes. Consider the following:
1. The table is represented as a pandas DataFrame named df.
2. Do not include explanations, comments, or multiline outputs.
3. Ensure the output is concise, correct, and when run, it outputs the correct given answer, and strictly follows the JSON format: {"PANDA": "<your Pandas code>"}
4. Use double quotes inside the pandas code and escape them with backslash. Example: df["Column"] not df['Column'].
5. Do not use double curly braces {{ }}. Use single curly braces { }.
6. Always use convert_type('...') for numeric and date constants from the SQL query, even if they look like plain numbers. Example: df[df['Year'] == convert_type('2005')] not df[df['Year'] == 2005].
Use convert_type, which converts string values into appropriate types: numbers (int/float) with automatic removal of extra characters (spaces, currencies, percentages, thousand separators, parentheses) and dates/times into pandas Timestamp recognizing various formats (ISO, European, American, with month names). If conversion is impossible, the function returns the original value unchanged.
Signature: convert_type(value: Any) -> Any
When to apply convert_type (only to constants from the SQL query):
- Filtering by number: df[df['Year'] == convert_type('2005')]
- Filtering by range: df[df['Points'] > convert_type('79')]
- Filtering by date: df[df['Date'] > convert_type('2000-01-01')]
- Arithmetic with constants: convert_type('1000') + df['Bonus']
String constant comparisons do NOT require convert_type: df[df['Team'] == 'Crettyard']
Always use find_word(value: str) -> str when filtering by string entities mentioned in the SQL query — such as names of companies, people, cities, categories, etc. — regardless of whether typos or variations are suspected.
Examples for find_word():
# SQL: SELECT * FROM df WHERE Team = 'Manchester United'
df[df["Team"] == find_word("Manchester United")]

# SQL: SELECT MAX(Points) FROM df WHERE Team = 'Manchester United'
df[df["Team"] == find_word("Manchester United")]["Points"].max()

# SQL: SELECT COUNT(*) FROM df WHERE City = 'Belgrade'
df[df["City"] == find_word("Belgrade")].shape[0]

# SQL: SELECT Type FROM df WHERE Category = 'Grand Slam' LIMIT 1
df[df["Category"] == find_word("Grand Slam")]["Type"].iloc[0]

SQL query: {sql_query}
Table schema: {table_schema}
Original question: {question}
'''