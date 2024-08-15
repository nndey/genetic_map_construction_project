from pathlib import Path
import pandas as pd
import os
#import psycopg2
#import logging 

# configure logging
#logging.basicConfig(level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#def connect_to_db():
#    try:
#        conn = psycopg2.connect(
 #           host="aws-0-us-east-1.pooler.supabase.com",
  #          database="postgres",
   #         user="postgres.hymndoayxvitvbukpqct",
    #        password="zAHfkaH9iH5j_fG",
     #       port="6543"
       # )
      #  logging.info("Connected to Supabase.")
       # return conn
   # except Exception as e:
    #    logging.error(f"Error connecting to database: {e}")
     #   return None
    
#walking through the file system 
def find_csv_files(start_dir):
    dataframes = []
    # Create a Path object for the starting directory
    start_path = Path(start_dir)
    # Iterate over all subfolders and files in the directory
    for file_path in start_path.rglob('*.csv'):
        print(f"Found CSV file: {file_path}")
        # Import the CSV file
        df = pd.read_csv(file_path, encoding = 'ISO-8859-1' )
        dataframes.append(df)
    return dataframes

def find_excel_files(start_dir):
    dataframes = []
    #create a path object for the starting directory
    start_path = Path(start_dir)
    #iterate over all subfolders and files in the directory
    for file_path in start_path.rglob('*.xls*'):
        print(f"Found Excel file: {file_path}")
        #import excel file
        df = pd.read_excel(file_path, engine = 'openpyxl')
        dataframes.append(df)
    return dataframes

def parse_texture_analyzer_files(file_path_list):
    parsed_TA_files = []
    for file_path in file_path_list:
        #extract sample IDs (LSH - X X X) from the 'Test ID' column
        sample_ids = file_path['Test ID'].str.extract(r'(LSH-\d+)')[0].dropna().unique()
        #select the average row
        average_row = file_path[file_path['Test ID'].str.contains('Average:', na = False)]
        #create a new data frame for the results
        columns_to_keep = file_path.columns[1:] #exclude the 'Test ID' column
        results = pd.DataFrame(columns = ['Sample ID'] + list(columns_to_keep))
        #populate the new data frame with sample IDs and corresponding averages
        for sample_id in sample_ids:
            row = {'Sample ID': sample_id}
            row.update(average_row.iloc[0, 1:].to_dict())
            results = pd.concat([results, pd.DataFrame([row])], ignore_index = True)
        parsed_TA_files.append(results)
    return parsed_TA_files
        
def parse_autotitrator_files(file_path_list):
    parsed_AT_files = []
    for file_path in file_path_list:
        print(f"Columns in {file_path}: {file_path.columns.tolist()}")
        sample_ids = []
        sample_sizes = []
        veq1_values = []
        est_values = []

        #variables to hold the current sample's data
        current_sample_id = None
        current_sample_size = None
        current_veq1 = None
        current_est = None
        #iterate through the dataframe to extract information
        for index, row in file_path.iterrows():
            if pd.notna(row.get('Name')) and "Sample Scope" in row.get('Name', ''):
                # Extract Sample ID (e.g., LSH-198)
                current_sample_id = row['Name'].split("LSH-")[1].split(")")[0]
            elif pd.notna(row.get('Value')) and "Sample size" in row.get('Name', ''):
                # Extract Sample Size
                current_sample_size = float(row['Value'].split()[0])
            elif pd.notna(row.get('Value')) and "VEQ1" in row.get('Name', ''):
                # Extract VEQ1
                current_veq1 = float(row['Value'].split()[0])
            elif pd.notna(row.get('Value')) and "EST" in row.get('Name', ''):
                # Extract EST
                current_est = float(row['Value'].split()[0])

            
                #after EST, we expect a complete set of data, so store it
                sample_ids.append(current_sample_id)
                sample_sizes.append(current_sample_size)
                veq1_values.append(current_veq1)
                est_values.append(current_est)

                #reset current sample variables (if multiple samples exist)
                current_sample_id = None
                current_sample_size = None
                current_veq1 = None
                current_est = None

            #create new data frame
        parsed_AT = pd.DataFrame({
            'unique id': sample_ids,
            'autotitration sample size (g)': sample_sizes,
            'autotitration veq1 (ml)': veq1_values,
            'autotitration est (pH)': est_values
        })
        parsed_AT_files.append(parsed_AT)
    return parsed_AT_files

#work through texture analyzer data
start_directory_TA = "/Users/nyssandey-bongo/Downloads/Summer2024_LSH_Data/Texture Analyzer"
texture_analyzer_file_paths = find_excel_files(start_directory_TA)
new_TA_df_list = parse_texture_analyzer_files(texture_analyzer_file_paths)

#work through autotitrator data
start_directory_AT = "/Users/nyssandey-bongo/Downloads/Summer2024_LSH_Data/Autotitrator"
autotitrator_file_paths = find_csv_files(start_directory_AT)
new_AT_df_list = parse_autotitrator_files(autotitrator_file_paths)

#def main():
    #connect to supabase
    #conn = connect_to_db()
    #if not conn:
     #   return  

#if __name__ == "__main__":
    #main()

#def close_db_connection(conn):
    #if conn:
     #   conn.close()
      #  logging.info("Database connection closed.")

#close database connection
#close_db_connection()
