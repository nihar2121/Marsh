# ribe_backend.py
import pandas as pd
import os
from datetime import datetime

def process_file(filepath):
    """
    Processes the uploaded transaction file, categorizes transactions,
    and generates a final output file based on 'Receipt', 'Payment',
    'Bank Charges', and 'Brokerage Transfer' categories.

    Args:
        filepath (str): Path to the uploaded transaction file (CSV or Excel).

    Returns:
        str: Path to the generated final output Excel file.
    """
    # Detect file extension and read appropriately
    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)
    
    # Ensure the relevant columns exist
    required_columns = ['DESCRIPTION', 'DEBIT AMT', 'CREDIT AMT', 'DATE']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Convert 'DEBIT AMT' and 'CREDIT AMT' to numeric, coercing errors to NaN
    df['DEBIT AMT'] = pd.to_numeric(df['DEBIT AMT'], errors='coerce')
    df['CREDIT AMT'] = pd.to_numeric(df['CREDIT AMT'], errors='coerce')

    def assign_category(row):
        """
        Assigns a category to each transaction based on its description and amounts.

        Args:
            row (pd.Series): A row from the DataFrame.

        Returns:
            str: The assigned category.
        """
        desc = str(row.get('DESCRIPTION', '')).lower()
        debit_amt = row.get('DEBIT AMT', 0)
        credit_amt = row.get('CREDIT AMT', 0)

        # 1) Check keywords in DESCRIPTION first
        if 'brokerage transfer' in desc:
            return "Brokerage Transfer"
        if 'taxes and cess' in desc:
            return "Bank Charges"
        if 'billing invoice paid' in desc:
            return "Bank Charges"
        if 'outgoing' in desc:
            return "Payment"

        # 2) If none of the description keywords matched, 
        #    then check if this is a credit => "Receipt"
        #    (Non-empty credit and presumably no debit)
        if pd.notna(credit_amt) and credit_amt != 0 and (pd.isna(debit_amt) or debit_amt == 0):
            return "Receipt"

        # 3) Default category if no other conditions matched
        return ""

    # Create the Category column
    df["Category"] = df.apply(assign_category, axis=1)

    # Initialize counters for DocumentNo
    br_counter = 1  # For Bank Receipts
    bp_counter = 1  # For Bank Payments, Bank Charges, Brokerage Transfers

    # Initialize EntryNo
    entry_no = 1

    # Load the sample entries template
    sample_entries_path = r"C:\Users\nilut\Desktop\Marsh&mclanen\Marsh\Marsh\uploads\Sample Entries (1).xlsx"
    
    # Check if the sample entries file exists
    if not os.path.exists(sample_entries_path):
        raise FileNotFoundError(f"Sample entries file not found at: {sample_entries_path}")

    # Load the sample entries as a DataFrame
    sample_df = pd.read_excel(sample_entries_path)

    # Initialize list to collect new entries
    processed_entries = []

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        category = row['Category']
        description = row['DESCRIPTION']
        debit_amt = row['DEBIT AMT']
        credit_amt = row['CREDIT AMT']
        date = row['DATE']

        # Parse date to extract month abbreviation
        if pd.notna(date):
            if isinstance(date, str):
                try:
                    date_parsed = datetime.strptime(date, '%d.%b %Y')  # Assuming format '02.NOV 2024'
                except ValueError:
                    try:
                        date_parsed = pd.to_datetime(date, dayfirst=True)
                    except:
                        date_parsed = None
            elif isinstance(date, pd.Timestamp):
                date_parsed = date
            elif isinstance(date, datetime):
                date_parsed = date
            else:
                date_parsed = None
        else:
            date_parsed = None

        if date_parsed:
            month_abbr = date_parsed.strftime('%b')
            posting_date = date_parsed.strftime('%Y-%m-%d')  # For Excel date formatting
        else:
            month_abbr = 'Unknown'
            posting_date = ''

        if category == "Receipt":
            # DocumentNo: CITI/013/BR/Nov/001
            doc_prefix = "BR"
            document_no = f"CITI/013/{doc_prefix}/{month_abbr}/{br_counter:03d}"

            # Positive entry: Bank Account
            positive_entry = {
                'EntryNo': entry_no,
                'DocumentNo': document_no,
                'LineNo': 1,
                'AccountType': 'Bank Account',
                'AccountNo': 2600005,
                'PostingDate': posting_date,
                'Amount': credit_amt,
                'Narration': description,
                'NatureofTransaction': 'Bank Receipt'
            }

            # Negative entry: G/L Account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'G/L Account',
                'AccountNo': 1500001,
                'PostingDate': posting_date,
                'Amount': -credit_amt,
                'Narration': description,
                'NatureofTransaction': 'Bank Receipt'
            }

            # Append the entries to processed_entries list
            processed_entries.append(positive_entry)
            processed_entries.append(negative_entry)

            # Increment counters
            br_counter += 1
            entry_no += 2

        elif category in ["Payment", "Bank Charges"]:
            # DocumentNo: CITI/013/BP/Nov/001
            doc_prefix = "BP"
            document_no = f"CITI/013/{doc_prefix}/{month_abbr}/{bp_counter:03d}"

            # Ensure debit_amt is positive
            debit_amt_positive = abs(debit_amt) if pd.notna(debit_amt) else 0

            # Positive entry: G/L Account
            positive_entry = {
                'EntryNo': entry_no,
                'DocumentNo': document_no,
                'LineNo': 1,
                'AccountType': 'G/L Account',
                'AccountNo': 1500001,
                'PostingDate': posting_date,
                'Amount': debit_amt_positive,
                'Narration': description,
                'NatureofTransaction': 'Bank Payment'
            }

            # Negative entry: Bank Account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'Bank Account',
                'AccountNo': 2600005,
                'PostingDate': posting_date,
                'Amount': -debit_amt_positive,
                'Narration': description,
                'NatureofTransaction': 'Bank Payment'
            }

            # Append the entries to processed_entries list
            processed_entries.append(positive_entry)
            processed_entries.append(negative_entry)

            # Increment counters
            bp_counter += 1
            entry_no += 2

        elif category == "Brokerage Transfer":
            # DocumentNo: CITI/013/BP/Nov/001
            doc_prefix = "BP"
            document_no = f"CITI/013/{doc_prefix}/{month_abbr}/{bp_counter:03d}"

            # Ensure debit_amt is positive
            debit_amt_positive = abs(debit_amt) if pd.notna(debit_amt) else 0

            # Positive entry: Bank Account
            positive_entry = {
                'EntryNo': entry_no,
                'DocumentNo': document_no,
                'LineNo': 1,
                'AccountType': 'Bank Account',
                'AccountNo': 2600005,
                'PostingDate': posting_date,
                'Amount': debit_amt_positive,
                'Narration': description,
                'NatureofTransaction': 'Contra'
            }

            # Negative entry: Bank Account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'Bank Account',
                'AccountNo': 1500001,
                'PostingDate': posting_date,
                'Amount': -debit_amt_positive,
                'Narration': description,
                'NatureofTransaction': 'Contra'
            }

            # Append the entries to processed_entries list
            processed_entries.append(positive_entry)
            processed_entries.append(negative_entry)

            # Increment counters
            bp_counter += 1
            entry_no += 2

        else:
            # Ignore other categories or handle them as needed
            continue

    if not processed_entries:
        raise ValueError("No relevant transactions to process.")

    # Create DataFrame from processed_entries
    processed_df = pd.DataFrame(processed_entries)

    # Get all columns from sample_entries template
    sample_columns = sample_df.columns.tolist()

    # Create a DataFrame with sample_columns and populate with processed_df
    final_df = pd.DataFrame(columns=sample_columns)

    # Fill only the required columns, others remain blank
    required_output_columns = [
        'EntryNo', 'DocumentNo', 'LineNo', 'AccountType', 'AccountNo',
        'PostingDate', 'Amount', 'Narration', 'NatureofTransaction'
    ]

    # Populate final_df
    for _, row in processed_df.iterrows():
        entry = {}
        for col in sample_columns:
            if col in required_output_columns:
                entry[col] = row[col]
            else:
                entry[col] = ''  # or pd.NA
        final_df = final_df.append(entry, ignore_index=True)

    # Define the output path
    base_dir, _ = os.path.split(filepath)
    output_path = os.path.join(base_dir, "Final_Output.xlsx")

    # Write to Excel with formatting using xlsxwriter
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook  = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # Define formats
        money_fmt = workbook.add_format({'num_format': '#,##0.00'})
        date_fmt = workbook.add_format({'num_format': 'dd-mmm-yyyy'})
        
        # Apply formats to 'Amount' and 'PostingDate' columns
        if 'Amount' in sample_columns:
            amount_col_idx = sample_columns.index('Amount')
            # Columns are zero-indexed; set width and format
            worksheet.set_column(amount_col_idx, amount_col_idx, 15, money_fmt)
        
        if 'PostingDate' in sample_columns:
            date_col_idx = sample_columns.index('PostingDate')
            worksheet.set_column(date_col_idx, date_col_idx, 15, date_fmt)
        
    return output_path
