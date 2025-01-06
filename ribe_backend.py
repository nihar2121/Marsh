# ribe_backend.py
import pandas as pd
import re
import os

def process_file(filepath):
    """
    Processes the uploaded transaction file, categorizes transactions,
    and generates a final output file based on 'Receipt' categories.

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

    # Now, proceed to process 'Receipt' category
    receipt_df = df[df["Category"] == "Receipt"]

    if receipt_df.empty:
        raise ValueError("No 'Receipt' category transactions found in the uploaded file.")

    # Load the sample entries template
    # Update this path as per your actual location or make it relative to the app directory
    sample_entries_path = r"C:\Users\nilut\Desktop\Marsh&mclanen\Marsh\Marsh\uploads\Sample Entries (1).xlsx"
    
    # Check if the sample entries file exists
    if not os.path.exists(sample_entries_path):
        raise FileNotFoundError(f"Sample entries file not found at: {sample_entries_path}")

    # Load the sample entries as a DataFrame
    sample_df = pd.read_excel(sample_entries_path)

    # Initialize list to collect new entries
    new_entries = []

    # Initialize EntryNo
    entry_no = 1

    # Iterate through receipt_df and add two rows per receipt
    for index, row in receipt_df.iterrows():
        date = row['DATE']
        description = row['DESCRIPTION']
        credit_amt = row['CREDIT AMT']

        # Format date as per sample entries 'PostingDate' column
        # Ensure 'DATE' is in a date format
        if not pd.isnull(date):
            if isinstance(date, str):
                try:
                    posting_date = pd.to_datetime(date, dayfirst=True).date()
                except:
                    posting_date = date
            elif isinstance(date, pd.Timestamp):
                posting_date = date.date()
            else:
                posting_date = date
        else:
            posting_date = ''

        # Create positive entry
        positive_entry = {
            'EntryNo': entry_no,
            'LineNo': 1,
            'AccountType': 'Bank Account',
            'AccountNo': 2600005,
            'PostingDate': posting_date,
            'Amount': credit_amt,
            'Narration': description,
            'NatureofTransaction': 'Bank Receipt'
        }

        # Create negative entry
        negative_entry = {
            'EntryNo': entry_no,
            'LineNo': 2,
            'AccountType': 'G/L Account',
            'AccountNo': 1500001,
            'PostingDate': posting_date,
            'Amount': -credit_amt,
            'Narration': description,
            'NatureofTransaction': 'Bank Receipt'
        }

        # Append the entries to new_entries list
        new_entries.append(positive_entry)
        new_entries.append(negative_entry)

        # Increment entry_no for the next set of entries
        entry_no += 1

    # Create DataFrame from new_entries
    new_entries_df = pd.DataFrame(new_entries)

    # Get all columns from sample_entries template
    sample_columns = sample_df.columns.tolist()

    # Create a DataFrame with sample_columns and populate with new_entries_df
    final_df = pd.DataFrame(columns=sample_columns)

    # Fill only the required columns, others remain blank
    required_output_columns = ['EntryNo', 'LineNo', 'AccountType', 'AccountNo', 'PostingDate', 'Amount', 'Narration', 'NatureofTransaction']
    
    for index, row in new_entries_df.iterrows():
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
            # Columns are zero-indexed
            worksheet.set_column(amount_col_idx, amount_col_idx, 15, money_fmt)
        
        if 'PostingDate' in sample_columns:
            date_col_idx = sample_columns.index('PostingDate')
            worksheet.set_column(date_col_idx, date_col_idx, 15, date_fmt)
        
    return output_path
