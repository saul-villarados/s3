import pandas as pd
from config.config import Config 
#class DataLoader:

urls = []

try:
    df = pd.read_csv(Config.path, on_bad_lines='skip', sep='\t')

    urls = df['photo_image_url'].to_list()

    row_count = len(urls)

    print(f"The number of rows after filtering is: {row_count}")

except KeyError as e:
    print(f"Error: One of the specified columns was not found in the DataFrame. Details: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")