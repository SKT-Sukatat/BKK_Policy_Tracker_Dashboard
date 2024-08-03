# Function to extract all occurrences of "\d+\.\d+%" (xxx.xx%)
def extract_percent_progress(text):
    progress_percent = re.findall(r'\d{1,3}(?:,\d{3})*\.\d+%', text)
    return ''.join(progress_percent)

def remove_percent_progress(text):
    return re.sub(r'\d{1,3}(?:,\d{3})*\.\d+%', '', text).strip()


# Dealing with Total Progress (%)
df_progress['Total Progress (%)'] = df_progress['Goal'].apply(extract_percent_progress)
df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_percent_progress(x))

df_progress['Goal'] = df_progress['Goal'].str.replace(':', '')

#Check DF info
df_progress.info()