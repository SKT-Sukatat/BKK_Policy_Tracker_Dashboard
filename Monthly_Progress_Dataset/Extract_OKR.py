### Extract OKR value

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