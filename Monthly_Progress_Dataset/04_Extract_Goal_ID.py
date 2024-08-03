def extract_goal_id(text):
    goal_id = re.findall(r'\d+\.\d?', str(text))
    result = ''.join(goal_id)
    return result
# def extract_patterns(text):
#     return re.findall(r'\b\d+\.\d*\b', text)

df_progress['No. (Goal)'] = df_progress['Goal'].apply(extract_goal_id)
df_progress['No. (Goal)'] = df_progress['No. (Goal)'].replace('', np.nan)
df_progress['No. (Goal)'] = df_progress['No. (Goal)'].str.strip('.')