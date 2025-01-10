# ribe_backend.py
import pandas as pd
import os
import re
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
    sample_entries_path = r"\\?\UNC\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\RI Entries\Sample Entries (1).xlsx"
    
    # Check if the sample entries file exists
    if not os.path.exists(sample_entries_path):
        raise FileNotFoundError(f"Sample entries file not found at: {sample_entries_path}")

    # Load the sample entries as a DataFrame
    sample_df = pd.read_excel(sample_entries_path)

    # Define the Output Files directories
    ri_entries_dir = os.path.dirname(sample_entries_path)
    output_files_dir = os.path.join(ri_entries_dir, "Output Files")
    single_files_dir = os.path.join(output_files_dir, "Single Files")
    master_file_dir = os.path.join(output_files_dir, "Master File")
    support_files_dir = os.path.join(output_files_dir, "support_files")
    
    # Create directories if they don't exist
    os.makedirs(single_files_dir, exist_ok=True)
    os.makedirs(master_file_dir, exist_ok=True)
    os.makedirs(support_files_dir, exist_ok=True)

    # Define paths for Master File and Support File
    master_file_path = os.path.join(master_file_dir, "Master_File.xlsx")
    support_file_path = os.path.join(support_files_dir, "support_file.xlsx")

    # Load Master File if it exists, else create an empty DataFrame with required columns
    if os.path.exists(master_file_path):
        master_df = pd.read_excel(master_file_path)
    else:
        # Initialize an empty DataFrame with the same columns as sample_df
        master_df = pd.DataFrame(columns=sample_df.columns.tolist())

    # Extract prefix from the file name
    filename = os.path.basename(filepath)
    filename_lower = filename.lower()
    match = re.match(r'^(citi\d{3})', filename_lower)
    if match:
        prefix_raw = match.group(1)
        prefix_formatted = f"{prefix_raw[:4].upper()}/{prefix_raw[4:]}"  # 'citi013' → 'CITI/013'
    else:
        raise ValueError("Filename does not start with a valid prefix (e.g., 'citi013').")

    # Load the support file and create a mapping
    if not os.path.exists(support_file_path):
        raise FileNotFoundError(f"Support file not found at: {support_file_path}")

    try:
        support_df = pd.read_excel(support_file_path, sheet_name='Sheet2')
    except Exception as e:
        raise ValueError(f"Error reading support file: {e}")

    # Ensure required columns exist in support file
    required_support_columns = ['lookup_account', 'base_account', 'to_account']
    for col in required_support_columns:
        if col not in support_df.columns:
            raise ValueError(f"Missing required column in support file: {col}")

    # Create a mapping dictionary
    account_mapping = support_df.set_index('lookup_account').to_dict('index')

    # Retrieve base_account and to_account based on prefix_raw
    mapping = account_mapping.get(prefix_raw.lower())
    if not mapping:
        raise ValueError(f"Lookup account '{prefix_raw}' not found in support file.")

    base_account = mapping['base_account']
    to_account = mapping['to_account']

    # Initialize list to collect new entries
    processed_entries = []

    # Prepare a set of existing narrations for quick lookup
    existing_narrations = set(master_df['Narration'].str.lower().str.strip()) if not master_df.empty else set()

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        category = row['Category']
        description = row['DESCRIPTION']
        debit_amt = row['DEBIT AMT']
        credit_amt = row['CREDIT AMT']
        date = row['DATE']

        # Check if description already exists in master narrations
        if pd.isna(description) or description.lower().strip() in existing_narrations:
            # Skip processing this row
            continue

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
            # DocumentNo: {prefix}/BR/{Month}/Counter
            doc_prefix = "BR"
            document_no = f"{prefix_formatted}/{doc_prefix}/{month_abbr}/{br_counter:03d}"

            # Positive entry: base_account
            positive_entry = {
                'EntryNo': entry_no,
                'DocumentNo': document_no,
                'LineNo': 1,
                'AccountType': 'Bank Account',
                'AccountNo': base_account,
                'PostingDate': posting_date,
                'Amount': credit_amt,
                'Narration': description,
                'NatureofTransaction': 'Bank Receipt',
                # Fill other columns with empty strings
                'ReceiptType': '',
                'CurrencyCode': '',
                'CurrencyRate': '',
                'ExternalDocumentNo': '',
                'BranchDimensionCode': '',
                'CoverNo': '',
                'InsuranceBranch': '',
                'MarshBranch': '',
                'Department': '',
                'ServicerID': '',
                'CE Name': '',
                'ClientName': '',
                'PolicyNo': '',
                'EndorsementNo': '',
                'Risk': '',
                'ASP_PRACTICE': '',
                'IncomeCategory': '',
                'PolInceptionDate': '',
                'Pol.End Dt.': '',
                'Premium': '',
                'Premium GST': '',
                'BrokerageRate': '',
                'INSURER_TYPE': '',
                'INSURER_NAME': '',
                'PROPORTION': '',
                'BRIEF_DESC': '',
                'Curr.': '',
                'Curr_Rate': '',
                'BROKERAGE_FEE_DUE': '',
                'iTrack No.': '',
                'FinanceSPOC': '',
                'Grouping': '',
                'Due Date': '',
                'Overdue': ''
            }

            # Negative entry: to_account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'G/L Account',
                'AccountNo': to_account,
                'PostingDate': posting_date,
                'Amount': -credit_amt,
                'Narration': description,
                'NatureofTransaction': 'Bank Receipt',
                # Fill other columns with empty strings
                'ReceiptType': '',
                'CurrencyCode': '',
                'CurrencyRate': '',
                'ExternalDocumentNo': '',
                'BranchDimensionCode': '',
                'CoverNo': '',
                'InsuranceBranch': '',
                'MarshBranch': '',
                'Department': '',
                'ServicerID': '',
                'CE Name': '',
                'ClientName': '',
                'PolicyNo': '',
                'EndorsementNo': '',
                'Risk': '',
                'ASP_PRACTICE': '',
                'IncomeCategory': '',
                'PolInceptionDate': '',
                'Pol.End Dt.': '',
                'Premium': '',
                'Premium GST': '',
                'BrokerageRate': '',
                'INSURER_TYPE': '',
                'INSURER_NAME': '',
                'PROPORTION': '',
                'BRIEF_DESC': '',
                'Curr.': '',
                'Curr_Rate': '',
                'BROKERAGE_FEE_DUE': '',
                'iTrack No.': '',
                'FinanceSPOC': '',
                'Grouping': '',
                'Due Date': '',
                'Overdue': ''
            }

            # Append the entries to processed_entries list
            processed_entries.append(positive_entry)
            processed_entries.append(negative_entry)

            # Increment counters
            br_counter += 1
            entry_no += 2

        elif category in ["Payment", "Bank Charges"]:
            # DocumentNo: {prefix}/BP/{Month}/Counter
            doc_prefix = "BP"
            document_no = f"{prefix_formatted}/{doc_prefix}/{month_abbr}/{bp_counter:03d}"

            # Ensure debit_amt is positive
            debit_amt_positive = abs(debit_amt) if pd.notna(debit_amt) else 0

            if category == "Payment":
                # Positive entry: base_account (negative amount)
                positive_entry = {
                    'EntryNo': entry_no,
                    'DocumentNo': document_no,
                    'LineNo': 1,
                    'AccountType': 'G/L Account',
                    'AccountNo': base_account,
                    'PostingDate': posting_date,
                    'Amount': -debit_amt_positive,
                    'Narration': description,
                    'NatureofTransaction': 'Bank Payment',
                    # Fill other columns with empty strings
                    'ReceiptType': '',
                    'CurrencyCode': '',
                    'CurrencyRate': '',
                    'ExternalDocumentNo': '',
                    'BranchDimensionCode': '',
                    'CoverNo': '',
                    'InsuranceBranch': '',
                    'MarshBranch': '',
                    'Department': '',
                    'ServicerID': '',
                    'CE Name': '',
                    'ClientName': '',
                    'PolicyNo': '',
                    'EndorsementNo': '',
                    'Risk': '',
                    'ASP_PRACTICE': '',
                    'IncomeCategory': '',
                    'PolInceptionDate': '',
                    'Pol.End Dt.': '',
                    'Premium': '',
                    'Premium GST': '',
                    'BrokerageRate': '',
                    'INSURER_TYPE': '',
                    'INSURER_NAME': '',
                    'PROPORTION': '',
                    'BRIEF_DESC': '',
                    'Curr.': '',
                    'Curr_Rate': '',
                    'BROKERAGE_FEE_DUE': '',
                    'iTrack No.': '',
                    'FinanceSPOC': '',
                    'Grouping': '',
                    'Due Date': '',
                    'Overdue': ''
                }

                # Negative entry: to_account
                negative_entry = {
                    'EntryNo': entry_no + 1,
                    'DocumentNo': document_no,
                    'LineNo': 2,
                    'AccountType': 'Bank Account',
                    'AccountNo': to_account,
                    'PostingDate': posting_date,
                    'Amount': debit_amt_positive,
                    'Narration': description,
                    'NatureofTransaction': 'Bank Payment',
                    # Fill other columns with empty strings
                    'ReceiptType': '',
                    'CurrencyCode': '',
                    'CurrencyRate': '',
                    'ExternalDocumentNo': '',
                    'BranchDimensionCode': '',
                    'CoverNo': '',
                    'InsuranceBranch': '',
                    'MarshBranch': '',
                    'Department': '',
                    'ServicerID': '',
                    'CE Name': '',
                    'ClientName': '',
                    'PolicyNo': '',
                    'EndorsementNo': '',
                    'Risk': '',
                    'ASP_PRACTICE': '',
                    'IncomeCategory': '',
                    'PolInceptionDate': '',
                    'Pol.End Dt.': '',
                    'Premium': '',
                    'Premium GST': '',
                    'BrokerageRate': '',
                    'INSURER_TYPE': '',
                    'INSURER_NAME': '',
                    'PROPORTION': '',
                    'BRIEF_DESC': '',
                    'Curr.': '',
                    'Curr_Rate': '',
                    'BROKERAGE_FEE_DUE': '',
                    'iTrack No.': '',
                    'FinanceSPOC': '',
                    'Grouping': '',
                    'Due Date': '',
                    'Overdue': ''
                }

            else:  # Bank Charges
                # Positive entry: G/L Account (same as original behavior)
                positive_entry = {
                    'EntryNo': entry_no,
                    'DocumentNo': document_no,
                    'LineNo': 1,
                    'AccountType': 'G/L Account',
                    'AccountNo': 1500001,  # Hardcoded as per original code
                    'PostingDate': posting_date,
                    'Amount': debit_amt_positive,
                    'Narration': description,
                    'NatureofTransaction': 'Bank Payment',
                    # Fill other columns with empty strings
                    'ReceiptType': '',
                    'CurrencyCode': '',
                    'CurrencyRate': '',
                    'ExternalDocumentNo': '',
                    'BranchDimensionCode': '',
                    'CoverNo': '',
                    'InsuranceBranch': '',
                    'MarshBranch': '',
                    'Department': '',
                    'ServicerID': '',
                    'CE Name': '',
                    'ClientName': '',
                    'PolicyNo': '',
                    'EndorsementNo': '',
                    'Risk': '',
                    'ASP_PRACTICE': '',
                    'IncomeCategory': '',
                    'PolInceptionDate': '',
                    'Pol.End Dt.': '',
                    'Premium': '',
                    'Premium GST': '',
                    'BrokerageRate': '',
                    'INSURER_TYPE': '',
                    'INSURER_NAME': '',
                    'PROPORTION': '',
                    'BRIEF_DESC': '',
                    'Curr.': '',
                    'Curr_Rate': '',
                    'BROKERAGE_FEE_DUE': '',
                    'iTrack No.': '',
                    'FinanceSPOC': '',
                    'Grouping': '',
                    'Due Date': '',
                    'Overdue': ''
                }

                # Negative entry: Bank Account (same as original behavior)
                negative_entry = {
                    'EntryNo': entry_no + 1,
                    'DocumentNo': document_no,
                    'LineNo': 2,
                    'AccountType': 'Bank Account',
                    'AccountNo': 2600005,  # Hardcoded as per original code
                    'PostingDate': posting_date,
                    'Amount': -debit_amt_positive,
                    'Narration': description,
                    'NatureofTransaction': 'Bank Payment',
                    # Fill other columns with empty strings
                    'ReceiptType': '',
                    'CurrencyCode': '',
                    'CurrencyRate': '',
                    'ExternalDocumentNo': '',
                    'BranchDimensionCode': '',
                    'CoverNo': '',
                    'InsuranceBranch': '',
                    'MarshBranch': '',
                    'Department': '',
                    'ServicerID': '',
                    'CE Name': '',
                    'ClientName': '',
                    'PolicyNo': '',
                    'EndorsementNo': '',
                    'Risk': '',
                    'ASP_PRACTICE': '',
                    'IncomeCategory': '',
                    'PolInceptionDate': '',
                    'Pol.End Dt.': '',
                    'Premium': '',
                    'Premium GST': '',
                    'BrokerageRate': '',
                    'INSURER_TYPE': '',
                    'INSURER_NAME': '',
                    'PROPORTION': '',
                    'BRIEF_DESC': '',
                    'Curr.': '',
                    'Curr_Rate': '',
                    'BROKERAGE_FEE_DUE': '',
                    'iTrack No.': '',
                    'FinanceSPOC': '',
                    'Grouping': '',
                    'Due Date': '',
                    'Overdue': ''
                }

            # Append the entries to processed_entries list
            processed_entries.append(positive_entry)
            processed_entries.append(negative_entry)

            # Increment counters
            bp_counter += 1
            entry_no += 2

        elif category == "Brokerage Transfer":
            # DocumentNo: {prefix}/BP/{Month}/Counter
            doc_prefix = "BP"
            document_no = f"{prefix_formatted}/{doc_prefix}/{month_abbr}/{bp_counter:03d}"

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
                'NatureofTransaction': 'Contra',
                # Fill other columns with empty strings
                'ReceiptType': '',
                'CurrencyCode': '',
                'CurrencyRate': '',
                'ExternalDocumentNo': '',
                'BranchDimensionCode': '',
                'CoverNo': '',
                'InsuranceBranch': '',
                'MarshBranch': '',
                'Department': '',
                'ServicerID': '',
                'CE Name': '',
                'ClientName': '',
                'PolicyNo': '',
                'EndorsementNo': '',
                'Risk': '',
                'ASP_PRACTICE': '',
                'IncomeCategory': '',
                'PolInceptionDate': '',
                'Pol.End Dt.': '',
                'Premium': '',
                'Premium GST': '',
                'BrokerageRate': '',
                'INSURER_TYPE': '',
                'INSURER_NAME': '',
                'PROPORTION': '',
                'BRIEF_DESC': '',
                'Curr.': '',
                'Curr_Rate': '',
                'BROKERAGE_FEE_DUE': '',
                'iTrack No.': '',
                'FinanceSPOC': '',
                'Grouping': '',
                'Due Date': '',
                'Overdue': ''
            }

            # Negative entry: G/L Account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'G/L Account',
                'AccountNo': 1500001,
                'PostingDate': posting_date,
                'Amount': -debit_amt_positive,
                'Narration': description,
                'NatureofTransaction': 'Contra',
                # Fill other columns with empty strings
                'ReceiptType': '',
                'CurrencyCode': '',
                'CurrencyRate': '',
                'ExternalDocumentNo': '',
                'BranchDimensionCode': '',
                'CoverNo': '',
                'InsuranceBranch': '',
                'MarshBranch': '',
                'Department': '',
                'ServicerID': '',
                'CE Name': '',
                'ClientName': '',
                'PolicyNo': '',
                'EndorsementNo': '',
                'Risk': '',
                'ASP_PRACTICE': '',
                'IncomeCategory': '',
                'PolInceptionDate': '',
                'Pol.End Dt.': '',
                'Premium': '',
                'Premium GST': '',
                'BrokerageRate': '',
                'INSURER_TYPE': '',
                'INSURER_NAME': '',
                'PROPORTION': '',
                'BRIEF_DESC': '',
                'Curr.': '',
                'Curr_Rate': '',
                'BROKERAGE_FEE_DUE': '',
                'iTrack No.': '',
                'FinanceSPOC': '',
                'Grouping': '',
                'Due Date': '',
                'Overdue': ''
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

    # Create a list of dictionaries matching the sample_columns
    final_entries = []
    for entry in processed_entries:
        final_entry = {col: entry.get(col, '') for col in sample_columns}
        final_entries.append(final_entry)

    # Create the final DataFrame
    final_df = pd.DataFrame(final_entries, columns=sample_columns)

    # Save Single File
    # Define the output path for Single Files
    output_filename = f"Final_Output_{os.path.splitext(filename)[0]}.xlsx"
    single_file_path = os.path.join(single_files_dir, output_filename)

    # Write to Excel with formatting using xlsxwriter
    with pd.ExcelWriter(single_file_path, engine='xlsxwriter') as writer:
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
        
    # Append to Master File
    # Append the new entries to master_df
    master_df = pd.concat([master_df, final_df], ignore_index=True)

    # Save Master File
    master_df.to_excel(master_file_path, index=False, engine='xlsxwriter')

    return single_file_path
