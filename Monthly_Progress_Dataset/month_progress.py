# Declare Blank DataFrame
df_progress = pd.DataFrame()

# Access and concat all DataFrames to same DataFrames
for i in range(1,7):
    policy_url = "https://policy.bangkok.go.th/tracking/frontend/web/index.php?r=site%2Findexcheckreportkpi2&page=" + str(i) +"&per-page=50"
    progress_table = pd.read_html(policy_url)
    df_progress = pd.concat([df_progress, progress_table[0]], ignore_index=True, axis = 0)

## Rename Columns
new_col_names = {'KEY_RESULT':'Goal', 'ค่าเป้าหมาย/ปี':'Yearly Goal', 'ผลดำเนินงาน (รวม)':'Total Progress (Unit)', 'หน่วยนับ':'Unit',
       'ตค.66':'Oct 23', 'พย.66':'Nov 23', 'ธค.66':'Dec 23', 'มค.67':'Jan 24', 'กพ.67':'Feb 24', 'มี.ค.67':'Mar 24', 'เม.ย.67':'Apr 24',
       'พค.67':'May 24', 'มิ.ย.67':'Jun 24', 'กค.67':'July 24', 'สค.67':'Aug 24', 'กย.67':'Sept 24'}
df_progress.rename(columns = new_col_names, inplace = True)

# Drop Null Value
df_progress.dropna(axis = 0, inplace = True)

# Reset Index
df_progress = df_progress.reset_index()
df_progress.drop('index', axis = 1, inplace = True)

# Set Index
df_progress['#'] = df_progress['#'].astype('int')
df_progress.set_index(df_progress['#'], inplace = True)
df_progress.drop('#', axis = 1, inplace = True)


# Function to extract all occurrences of "OKR \d+\.\d+\.\d+" (OKR 0.0.0)
def extract_okr_numbers(text):
    okrs = re.findall(r'OKR \d+\.\d+\.\d+(?:\.\d+)?', text)
    okrs = ''.join(okrs)
    result = okrs.replace('OKR ', '', )
    return result

def remove_okr_numbers(text):
    return re.sub(r'OKR \d+\.\d+\.\d+(?:\.\d+)?', '', text).strip()

# Apply the function to the Goal column
df_progress['Related OKRs'] = df_progress['Goal'].apply(extract_okr_numbers)
df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_okr_numbers(x))


# Replace all Blank value by NaN (Null)
df_progress['Related OKRs'] = df_progress['Related OKRs'].replace('', np.nan)


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

def extract_goal_id(text):
    goal_id = re.findall(r'\d+\.\d?', str(text))
    result = ''.join(goal_id)
    return result
# def extract_patterns(text):
#     return re.findall(r'\b\d+\.\d*\b', text)

def remove_goal_id(text):
    return re.sub(r'\d+\.\d?', '', text).strip()


df_progress['No. (Goal)'] = df_progress['Goal'].apply(extract_goal_id)
df_progress['No. (Goal)'] = df_progress['No. (Goal)'].replace('', np.nan)
df_progress['No. (Goal)'] = df_progress['No. (Goal)'].str.strip('.')

df_progress['Goal'] = df_progress['Goal'].apply(lambda x: remove_goal_id(x))

df_progress = df_progress[['Goal','No. (Goal)','Unit', 'Related OKRs', 'Related KPI', 'Yearly Goal', 'Total Progress (Unit)', 'Total Progress (%)',
             'Oct 23', 'Nov 23', 'Dec 23', 'Jan 24', 'Feb 24', 'Mar 24', 'Apr 24', 'May 24', 'Jun 24', 'July 24', 'Aug 24', 'Sept 24']]