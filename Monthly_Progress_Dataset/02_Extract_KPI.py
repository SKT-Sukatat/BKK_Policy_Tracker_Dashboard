# Function to extract all occurrences of "\d+\.\d+%" (xxx.xx%)
def extract_kpi_numbers(text):
    kpi = re.findall(r'KPI \d+(?:\.\d+){2,3}', text)
    kpi = ''.join(kpi)
    result = kpi.replace('KPI ', '', )
    return result

def remove_kpi_numbers(text):
    return re.sub(r'KPI \d+(?:\.\d+){2,3}', '', text).strip()

df_progress['Related KPI'] = df_progress['Goal'].apply(extract_kpi_numbers)
df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_kpi_numbers(x))

df_progress['Related KPI'] = df_progress['Related KPI'].replace('', np.nan)