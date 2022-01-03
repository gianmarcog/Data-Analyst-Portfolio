import time
import pandas as pd
import schedule
timestr = time.strftime("%Y%m%d-%H%M%S")

confirmed_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/archived_data/archived_time_series/time_series_2019-ncov-Confirmed.csv"
recovered_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/archived_data/archived_time_series/time_series_2019-ncov-Recovered.csv"
death_cases_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/archived_data/archived_time_series/time_series_2019-ncov-Deaths.csv"

# Function to Fetch and Reshape
def get_n_melt_data(data_url,case_type):
    df = pd.read_csv(data_url)
    melted_df = df.melt(id_vars=['Province/State','Country/Region','Lat','Long'])
    melted_df.rename(columns={'variable':'Date', 'value':case_type}, inplace=True)
    return melted_df

def merge_data(confirm_df, recovered_df, deaths_df):
    new_df = confirm_df.join(recovered_df['Recovered']).join(deaths_df['Deaths'])
    return new_df

def fetch_data():
    confirm_df = get_n_melt_data (confirmed_cases_url,"Confirmed")
    recovered_df = get_n_melt_data(recovered_cases_url,"Recovered")
    death_df = get_n_melt_data(death_cases_url,"Deaths")
    print("Merging data")
    final_df = merge_data(confirm_df, recovered_df,death_df)
    filename = "Covid19_merged_dataset_updates_{}.csv".format(timestr)
    print("Saving dataset as {}".format(filename))
    final_df.to_csv(filename)
    print("Finished")


# Task
schedule.every(5).seconds.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(1)
