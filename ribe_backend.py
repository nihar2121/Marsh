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

    Supports both Citi and HSBC transaction files.

    Args:
        filepath (str): Path to the uploaded transaction file (CSV or Excel).

    Returns:
        str: Path to the generated final output Excel file.
    """
    # Extract filename and determine bank type
    filename = os.path.basename(filepath)
    filename_lower = filename.lower()
    bank_match = re.match(r'^(citi\d{3}|hsbc\d{3})', filename_lower)
    if not bank_match:
        raise ValueError("Filename does not start with a valid prefix (e.g., 'citi013' or 'hsbc002').")
    
    bank_prefix = bank_match.group(1)
    bank_type = 'CITI' if bank_prefix.startswith('citi') else 'HSBC'
    account_number = bank_prefix[4:]  # Extract the last three digits
    
    # Format the prefix for DocumentNo
    prefix_formatted = f"{bank_type}/{account_number}"
    
    # Detect file extension and read appropriately
    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)
    
    # Define categorization based on bank type
    if bank_type == 'CITI':
        # Ensure the relevant columns exist for Citi
        required_columns_citi = ['DESCRIPTION', 'DEBIT AMT', 'CREDIT AMT', 'DATE']
        for col in required_columns_citi:
            if col not in df.columns:
                raise ValueError(f"Missing required column for Citi file: {col}")
        
        # Convert 'DEBIT AMT' and 'CREDIT AMT' to numeric, coercing errors to NaN
        df['DEBIT AMT'] = pd.to_numeric(df['DEBIT AMT'], errors='coerce')
        df['CREDIT AMT'] = pd.to_numeric(df['CREDIT AMT'], errors='coerce')
        
        def assign_category(row):
            """
            Assigns a category to each transaction based on its description and amounts for Citi.

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
        
        # Create the Category column for Citi
        df["Category"] = df.apply(assign_category, axis=1)
        
        # Define date column and other relevant columns
        date_column = 'DATE'
        narration_column = 'DESCRIPTION'
        credit_amount_column = 'CREDIT AMT'
        debit_amount_column = 'DEBIT AMT'
        
    else:  # HSBC
        # Ensure the relevant columns exist for HSBC
        required_columns_hsbc = [
            'Acc name', 'Account number', 'Bank name', 'Currency',
            'Bank reference', 'Additional narrative', 'Customer reference',
            'TRN type', 'Value date (dd/mm/yyyy)', 'Credit amount',
            'Debit amount', 'Balance', 'Time', 'Post date', 'Brokerage Transfer'
        ]
        for col in required_columns_hsbc:
            if col not in df.columns:
                raise ValueError(f"Missing required column for HSBC file: {col}")
        
        # Convert 'Credit amount' and 'Debit amount' to numeric, coercing errors to NaN
        df['Credit amount'] = pd.to_numeric(df['Credit amount'].replace({',': ''}), errors='coerce')
        df['Debit amount'] = pd.to_numeric(df['Debit amount'].replace({',': ''}), errors='coerce')
        
        def assign_category_hsbc(row):
            """
            Assigns a category to each transaction based on HSBC-specific rules.

            Args:
                row (pd.Series): A row from the DataFrame.

            Returns:
                str: The assigned category.
            """
            credit_amt = row.get('Credit amount', 0)
            debit_amt = row.get('Debit amount', 0)
            customer_ref = str(row.get('Customer reference', '')).upper()
            additional_narrative = str(row.get('Additional narrative', '')).lower()
            trn_type = str(row.get('TRN type', '')).lower()

            # 1. Check if Credit amount is present
            if pd.notna(credit_amt) and credit_amt != 0 and (pd.isna(debit_amt) or debit_amt == 0):
                return "Receipt"

            # 2. Check Customer reference for Brokerage Transfer keywords
            brokerage_keywords = [
                "BROKERAGE TRNSFR", "BROKERGE TRNSFER", "BROKERAGE TRNSFR",
                "BROKERAGE TRNSFR", "BROKERGE TRANSFR", "BROKRAGE TRANSFR",
                "BROKRAGE TRANSFR", "BROKERGE TRNFER", "BROKERAGE TRSF"
            ]
            if customer_ref in brokerage_keywords:
                return "Brokerage Transfer"

            # 3. Check Additional narrative for 'brokerage' or specific number
            if 'brokerage' in additional_narrative:
                return "Brokerage Transfer"
            if '3402140005' in additional_narrative:
                return "Brokerage Transfer"

            # 4. Check TRN type
            if trn_type in ['charges', 'debit']:
                return "Bank Charges"
            if trn_type == 'transfer':
                return "Payment"

            # 5. Default category
            return ""
        
        # Create the Category column for HSBC
        df["Category"] = df.apply(assign_category_hsbc, axis=1)
        
        # Define date column and other relevant columns
        date_column = 'Post date'  # Use 'Post date' for HSBC
        narration_column = 'Additional narrative'
        credit_amount_column = 'Credit amount'
        debit_amount_column = 'Debit amount'
    
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
    support_files_dir = os.path.join(output_files_dir, "support_files")  # Added for support files

    # Create directories if they don't exist
    os.makedirs(single_files_dir, exist_ok=True)
    os.makedirs(master_file_dir, exist_ok=True)
    os.makedirs(support_files_dir, exist_ok=True)  # Ensure support_files directory exists

    # Define Master File path
    master_file_path = os.path.join(master_file_dir, "Master_File.xlsx")

    # Load Master File if it exists, else create an empty DataFrame with required columns
    if os.path.exists(master_file_path):
        master_df = pd.read_excel(master_file_path)
    else:
        # Initialize an empty DataFrame with the same columns as sample_df
        master_df = pd.DataFrame(columns=sample_df.columns.tolist())

    # -------------------- Added Code Starts Here --------------------
    # Define the path to the support file
    support_file_path = os.path.join(output_files_dir, "support_files", "support_file.xlsx")
    
    # Check if the support file exists
    if not os.path.exists(support_file_path):
        raise FileNotFoundError(f"Support file not found at: {support_file_path}")
    
    # Load the support file Sheet2
    support_df = pd.read_excel(support_file_path, sheet_name='Sheet2')
    
    # Ensure required columns exist in support file
    support_required_columns = ['lookup_account', 'base_account', 'to_account', 'category']
    for col in support_required_columns:
        if col not in support_df.columns:
            raise ValueError(f"Missing required column in support file: {col}")
    
    # Filter support_df for the current prefix_raw
    support_filtered = support_df[support_df['lookup_account'] == bank_prefix]
    
    if support_filtered.empty:
        raise ValueError(f"No support file entry found for account: {bank_prefix}")
    
    # Create a dictionary mapping category to (base_account, to_account)
    account_mapping = {}
    for _, row in support_filtered.iterrows():
        category = row['category']
        base_account = row['base_account']
        to_account = row['to_account']
        account_mapping[category] = {
            'base_account': base_account,
            'to_account': to_account
        }
    
    # Verify that all required categories are present in the support file
    required_categories = ["Payment", "Receipt", "Bank Charges", "Brokerage Transfer"]
    for cat in required_categories:
        if cat not in account_mapping:
            raise ValueError(f"Category '{cat}' not found in support file for account: {bank_prefix}")
    # --------------------- Added Code Ends Here ---------------------

    # Initialize list to collect new entries
    processed_entries = []

    # Prepare a set of existing narrations for quick lookup
    existing_narrations = set(master_df['Narration'].str.lower().str.strip()) if not master_df.empty else set()

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        category = row['Category']
        narration = row[narration_column]
        credit_amt = row[credit_amount_column] if bank_type == 'CITI' else row['Credit amount']
        debit_amt = row[debit_amount_column] if bank_type == 'CITI' else row['Debit amount']
        date = row[date_column]

        # Check if narration already exists in master narrations
        if pd.isna(narration) or narration.lower().strip() in existing_narrations:
            # Skip processing this row
            continue

        # Parse date to extract month abbreviation
        if pd.notna(date):
            if isinstance(date, str):
                try:
                    if bank_type == 'CITI':
                        date_parsed = datetime.strptime(date, '%d.%b %Y')  # Assuming format '02.NOV 2024'
                    else:  # HSBC
                        date_parsed = datetime.strptime(date, '%d/%m/%Y')  # Assuming format 'dd/mm/yyyy'
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

        # Retrieve base_account and to_account based on category from the support file
        if category in account_mapping:
            base_account = account_mapping[category]['base_account']
            to_account = account_mapping[category]['to_account']
        else:
            # If category is not one of the required, skip processing
            continue

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
                'Narration': narration,
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
                'Overdue': '',
                'Post Date': posting_date if bank_type == 'HSBC' else ''
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
                'Narration': narration,
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
                'Overdue': '',
                'Post Date': posting_date if bank_type == 'HSBC' else ''
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

            # Determine amount based on category
            if category == "Payment" or category == "Brokerage Transfer":
                amount = debit_amt
            else:  # Bank Charges
                amount = debit_amt

            # Ensure debit_amt is positive
            debit_amt_positive = abs(amount) if pd.notna(amount) else 0

            # Positive entry: to_account
            positive_entry = {
                'EntryNo': entry_no,
                'DocumentNo': document_no,
                'LineNo': 1,
                'AccountType': 'G/L Account',
                'AccountNo': to_account,
                'PostingDate': posting_date,
                'Amount': debit_amt_positive,
                'Narration': narration,
                'NatureofTransaction': 'Bank Payment' if category != "Brokerage Transfer" else 'Bank Payment',
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
                'Overdue': '',
                'Post Date': posting_date if bank_type == 'HSBC' else ''
            }

            # Negative entry: base_account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'Bank Account',  # Ensuring consistency
                'AccountNo': base_account,
                'PostingDate': posting_date,
                'Amount': -debit_amt_positive,
                'Narration': narration,
                'NatureofTransaction': 'Bank Payment' if category != "Brokerage Transfer" else 'Bank Payment',
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
                'Overdue': '',
                'Post Date': posting_date if bank_type == 'HSBC' else ''
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

            # Positive entry: to_account
            positive_entry = {
                'EntryNo': entry_no,
                'DocumentNo': document_no,
                'LineNo': 1,
                'AccountType': 'Bank Account',  # Changed to 'Bank Account' for Contra transactions
                'AccountNo': to_account,
                'PostingDate': posting_date,
                'Amount': debit_amt_positive,
                'Narration': narration,
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
                'Overdue': '',
                'Post Date': posting_date if bank_type == 'HSBC' else ''
            }

            # Negative entry: base_account
            negative_entry = {
                'EntryNo': entry_no + 1,
                'DocumentNo': document_no,
                'LineNo': 2,
                'AccountType': 'Bank Account',  # Changed from 'G/L Account' to 'Bank Account' for Contra transactions
                'AccountNo': base_account,
                'PostingDate': posting_date,
                'Amount': -debit_amt_positive,
                'Narration': narration,
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
                'Overdue': '',
                'Post Date': posting_date if bank_type == 'HSBC' else ''
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
        
        if 'Post Date' in sample_columns and bank_type == 'HSBC':
            post_date_col_idx = sample_columns.index('Post Date')
            worksheet.set_column(post_date_col_idx, post_date_col_idx, 15, date_fmt)
        
    # Append to Master File
    # Append the new entries to master_df
    master_df = pd.concat([master_df, final_df], ignore_index=True)

    # Save Master File
    master_df.to_excel(master_file_path, index=False, engine='xlsxwriter')

    return single_file_path
