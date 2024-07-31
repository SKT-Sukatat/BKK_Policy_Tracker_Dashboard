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