import sys
import pandas as pd
from sqlalchemy import create_engine


def merge_data(messages, categories):
    '''
    Merges the messages and categories dataframes into a single dataframe
    
    Parameters: 
        messages dataframe
        categories dataframe

    Returns: 
        merged dataframe

    '''
    print("\tstart merge_data")
    
    df = pd.merge(messages, categories, on='id', how='inner')
    
    print("\tend merge_data")    
    
    return df

def create_categories(df):
    '''
    Creates a separate categories dataframe from the original categories column
    that contains a separate column for each category
    
    Parameters: 
        df - merged dataframe

    Returns: 
        categories dataframe

    '''
    print("\tstart create_categories")

    # create a dataframe of the 36 individual category columns
    categories = df["categories"].str.split(";",expand=True)

    # select the first row of the categories dataframe
    row = categories.iloc[0]

    # Use this row to extract a list of new column names for categories.
    # Each category has the form {category_name}-n where the -n is 1 when
    # this category applies to the message and 0 when it does not
    # we can extract the category name by slicing and removing the last 2 chars
    category_colnames = row.str.slice(0, -2)    
    
    # rename the columns to the extracted categories
    categories.columns = category_colnames    
    
    #convert the category column values to be the last char of the category
    #converted to a number
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1]
    
        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column])    
    
    print("\tend create_categories")
        
    return categories

def append_categories(df, categories):
    '''
    Removes the original categories column and concatenates the categories dataframe
    onto the merged dataframe
    
    Parameters: 
        df - merged dataframe
        categories - categories dataframe

    Returns: 
        None

    '''
    print("\tstart append_categories")

    # drop the original categories column from `df`
    df.drop(columns="categories",inplace=True)
    
    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)    
    
    # drop duplicates
    df.drop_duplicates(keep="first", inplace=True)    

    print("\tend append_categories")
    
    return df






def save_data(df, database_filename):
    '''
    Loads the merged messages table into a sqllite database
    
    Parameters: 
        df - merged dataframe

    Returns: 
        None
    '''

    print("\tstart save_data")
    
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('disaster_response', engine, index=False, if_exists='replace')    
    
    print("\tend save_data")    

def clean_data(df):
    '''
    Creates a datafrane wtih a column for each category
   
    
    Parameters: 
        df - merged dataframe

    Returns: 
        reformatted dataframe with a column for each category
    '''
	
    print("\tstart clean_data")
    
    categories_df = create_categories(df)
    df = append_categories(df, categories_df)
		    
    print("\tend clean_data")
	  		    
    return df

def load_data(messages_filepath, categories_filepath):
    '''
    Loads messages and categories data from csv files
    
    Parameters: 
        messages_filepath - string
        categories_filepath - string

    Returns: 
        messages - dataframe
        categories - dataframe
    '''

    print("\tstart load_data")
    
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    
    df = merge_data(messages, categories)

    print("\tend load_data")
    		    
    return df

def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()