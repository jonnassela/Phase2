# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC
# MAGIC 1. First, upload the `movies_data_1.csv` file as a table into Databricks. To do that, click **New → Add or upload data → Create or modify a table**, then upload the CSV file.
# MAGIC 2. After the file is uploaded, set the **Table Name** and open **Advanced attributes**. Make sure the following options are checked: `First row contains the header`, `Automatically detect column types`, and `Rows span multiple lines` so that the file is properly parsed.
# MAGIC 3. Click the **Create table** button which will redirect you to a new page where you'll see the schema and sample data.
# MAGIC 4. Next, read the table as a pandas dataframe.

# COMMAND ----------

# Read table into spark dataframe and convert it to pandas
import pandas as pd
import numpy as np
import re


# Read table into spark dataframe and convert it to pandas
df = spark.read.table("workspace.default.movies_data_1")

display(df)  # explore in Spark

pdf = df.toPandas()  # convert only if needed
print(pdf.shape)
pdf.head()


# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS workspace.default.movies_data_1")

# COMMAND ----------

# Reload fresh to inspect raw YEAR values
df_raw = spark.read.table("workspace.default.movies_data_1").toPandas()

# Check unique YEAR values
print(df_raw['YEAR'].value_counts().head(30))
print("---")
print(df_raw['YEAR'].unique()[:50])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movies dataset description
# MAGIC
# MAGIC *  **Movie:** movie name
# MAGIC *  **Year:** release year
# MAGIC *  **Genre:** movie genre
# MAGIC *  **Rating:** audience rating
# MAGIC *  **One-Line:** short description about the movie
# MAGIC *  **Stars:** the casting art. Contains the director and star actors
# MAGIC *  **Votes:** audience votes
# MAGIC *  **Runtime:** Duration of movie
# MAGIC *  **Gross:** total amount earned worldwide
# MAGIC *  **Extract_date:** datetime of extraction
# MAGIC *  **owner_company:** Owner company of the movie/tvshow
# MAGIC *  **nr_of_episodes:** number of episodes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Data cleaning
# MAGIC
# MAGIC Apply the following steps for data cleaning:
# MAGIC 1. Clean dataset by removing empty rows. An empty row is considered when column Movies,Year, Genre, Rating, One-Line,Stars, Votes, RunTime and Gross are all null.
# MAGIC 2. Clean whitespaces, remove the '\n' from columns Genre, One-Line and Stars and '\t' from owner_company
# MAGIC

# COMMAND ----------

import pandas as pd
# Data cleaning requirements

# write your code here
import pandas as pd

# convert FIRST
df = spark.read.table("workspace.default.movies_data_1").toPandas()

# remove empty rows
cols_to_check = ['MOVIES', 'YEAR', 'GENRE', 'RATING', 'ONE-LINE', 'STARS', 'VOTES', 'RunTime', 'Gross']
df = df.dropna(subset=cols_to_check, how='all')

# reset index
df = df.reset_index(drop=True)

# clean text
#regex=false for exact text matching, =true for patter matching
df['GENRE'] = df['GENRE'].str.replace('\n', ' ', regex=False).str.strip().str.replace(' +', ' ', regex=True)
df['STARS'] = df['STARS'].str.replace('\n', ' ', regex=False).str.strip().str.replace(' +', ' ', regex=True)
df['ONE-LINE'] = df['ONE-LINE'].str.replace('\n', ' ', regex=False).str.strip().str.replace(' +', ' ', regex=True)
df['owner_company'] = df['owner_company'].str.replace('\t', '', regex=False).str.strip().str.replace(' +', ' ', regex=True)

 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Column splitting
# MAGIC
# MAGIC Apply the following steps for extracting data into separate columns:
# MAGIC
# MAGIC 3. Create separate columns _"Director"_ and _"Stars"_ from column "STARS". Extract the director name and store it into the newly created column _"Director"_. Do the same thing for stars. In the end, drop the original "STARS" column.
# MAGIC
# MAGIC 4. Store separately date and timestamp from Extract_date column as new columns <code>extraction_date</code> and <code>extraction_time</code>. Drop the original "Extract_date" column in the end.
# MAGIC
# MAGIC

# COMMAND ----------

# Column splitting requirements

# 1. Split STARS into Director and Stars columns
def extract_director(stars_str):
    if pd.isna(stars_str):
        return None
    match = re.search(r'Director[s]?:\s*(.*?)(?:\s*\||\s*Star[s]?:|$)', stars_str, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def extract_stars(stars_str):
    if pd.isna(stars_str):
        return None
    match = re.search(r'Star[s]?:\s*(.*?)$', stars_str, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


df['Director'] = df['STARS'].apply(extract_director)
df['Stars']    = df['STARS'].apply(extract_stars)

# Drop original STARS column
# df = df.drop(columns=['STARS'])

# Verify
print(df.iloc[0]['Director'])  # Should print: Peter Thorwarth

# 2. Split Extract_date into extraction_date and extraction_time
df['Extract_date']    = pd.to_datetime(df['Extract_date'])
df['extraction_date'] = df['Extract_date'].dt.date
df['extraction_time'] = df['Extract_date'].dt.time

# Drop original Extract_date column
# df = df.drop(columns=['Extract_date'])

print(df.shape)  # Should be (9999, 14)-- it is 16 bc i drop the columns afterwards 
# display(df)

# COMMAND ----------

#drop column
df = df.drop(columns=['Extract_date'])

# COMMAND ----------

# drop column
df = df.drop(columns=['STARS'])


# COMMAND ----------

# testing purposes, now its 14 as expected
print(df.columns.tolist())
print(df.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Year logic
# MAGIC
# MAGIC Apply the following steps for extracting data into separate columns:
# MAGIC
# MAGIC
# MAGIC 5. Calculate how long did the movie/tv show last and store the value in a separate column **"lasted"**. Also, store in new columns the **start_year** and **end_year** values of the movie when applicable. If the movie is still in production, then fill end_year with value <code>'present'</code>. In the end, drop the original "YEAR" column from the dataset.
# MAGIC

# COMMAND ----------

def parse_year(year_str):
    if pd.isna(year_str):
        return None, None, ''
    
    # Remove parentheses, roman numeral prefixes like (I), (II), strip spaces
    year_str = re.sub(r'^\([IVX]+\)\s*', '', year_str.strip())
    year_str = year_str.replace('(', '').replace(')', '').strip()
    
    # Range with end year like "2019–2023" or "2019-2023"
    range_match = re.match(r'(\d{4})\s*[–\-—]\s*(\d{4})', year_str)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        return start, end, end - start
    
    # Still running like "2020– " or "2020–"
    open_match = re.match(r'(\d{4})\s*[–\-—]\s*$', year_str)
    if open_match:
        start = int(open_match.group(1))
        return start, 'present', ''
    
    # Single year like "2021" or "2017 TV Special"
    single_match = re.match(r'^(\d{4})', year_str)
    if single_match:
        start = int(single_match.group(1))
        return start, start, ''

    return None, None, ''

results = df['YEAR'].apply(lambda y: pd.Series(parse_year(y), index=['start_year', 'end_year', 'lasted']))
df[['start_year', 'end_year', 'lasted']] = results
df['lasted'] = df['lasted'].fillna('')


# Verify
print("Shape:", df.shape)                                                    # Should be (9999, 16)
print("end_year='present':", df.loc[df['end_year'] == 'present'].shape[0]) # Should be 3180
print("lasted=='':", df.loc[df['lasted'] == ''].shape[0])                  # Should be 8611
print("lasted at index 11:", df.iloc[11]['lasted']) 

# COMMAND ----------

#drop column
df = df.drop(columns=['YEAR'])

# COMMAND ----------

# print(df['YEAR'].value_counts().head(20))
# print(df['YEAR'].unique()[:30])

print(df['end_year'].value_counts().head(20))
print("---")
print(df['lasted'].value_counts().head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Requirement: Dimension dataframes
# MAGIC
# MAGIC Apply the following steps for extracting data into separate columns:
# MAGIC
# MAGIC
# MAGIC 6. Extract unique values from **owner_company** column and store them as separate pandas Dataframe called <code>DimCompany</code>
# MAGIC 7. Similarly, extract unique values from **director column** and store it as separate pandas Dataframe called <code>DimDirector</code>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Write your code here
DimCompany = pd.DataFrame(df['owner_company'].dropna().unique(), columns=['owner_company']).reset_index(drop=True)

DimDirector=pd.DataFrame(df['Director'].dropna().unique(),columns=['Director']).reset_index(drop=True)

print("DimCompany shape:", DimCompany.shape)
print("DimDirector shape:", DimDirector.shape)

print(DimCompany)
print(DimDirector.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions
# MAGIC
# MAGIC The cells below are assertions for checking  whether the requirements were fulfilled on the movies dataframe. You can execute the cell to check your work. If everything passes, then you have successfully implemented the requirements.

# COMMAND ----------

import numpy as np

def assert_df(df):

    # Checks for the first csv file
    # the end dataset should be of shape (9999,16)
    assert df.shape[0] == 9999
    assert df.shape[1] == 16

    # The Lasted column should have 3180 rows with 'present' value
    assert df.loc[df["end_year"]=='present'].shape[0] == 3180
    assert df.loc[df["lasted"]==''].shape[0] == 8611 


    # random checks of data
    assert df.iloc[0]['Director'] == 'Peter Thorwarth'
    assert df.iloc[11]['MOVIES'] == 'Lucifer'
    assert df.iloc[11]['lasted'] == 5

    # there should be 10 unique values for companies
    assert len(df.owner_company.unique()) == 10

    # check if the unique values for companies are the same
    np.testing.assert_array_equal(df.owner_company.unique(),['Columbia Pictures', 'Legendary Entertainment',
        'Universal Pictures', 'Paramount Pictures', 'Walt Disney Pictures',
        'Marvel Studios', '20th Century Fox', 'Relativity Media',
        'RatPac-Dune Entertainment', 'Warner Bros.'])

    # there should be 3709 unique values for directors
    assert len(df.Director.unique()) == 3709
    print("Assertion of dataframe is complete!")


# COMMAND ----------

# Pass in the name of your movies dataframe
assert_df(df)

# COMMAND ----------

print(df.shape)
# Show first 5 rows
df.head(1000)

# COMMAND ----------

print(df.shape)
# Add lasted as string type explicitly
df['lasted'] = df['lasted'].fillna('')
print(df.loc[df['lasted'] == ''].shape[0])  # Should be 8611
print(df.loc[df['end_year'] == 'present'].shape[0])  # Should be 3180

# COMMAND ----------

# Check if maybe we need a separate 'duration' column from RunTime
print(df['RunTime'].head(10))

# COMMAND ----------


df['lasted'] = df['lasted'].fillna('')

# Check - maybe RunTime needs a separate 'duration' column
# df['duration'] = df['RunTime']

print(df.shape)  # Should be (9999, 16)
assert_df(df)
