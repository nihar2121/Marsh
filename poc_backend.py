import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import os

def read_lookup_files():
    try:
        template_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\Unused Template Final.xlsx'
        risk_code_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\Risk code.xlsx'
        cust_neft_list_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\CUST_NEFT LIST.xlsx'
        table_3_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\table_3.csv'
        table_4_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\table_4.csv'
        table_5_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\table_5.csv'
        #oriental_table_1_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\oriental_table_1.csv'
        #oriental_table_2_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\oriental_table_2.csv'
        #oriental_table_3_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\oriental_table_3.csv'

        template_data = pd.read_excel(template_path, header=1) if os.path.exists(template_path) else None
        risk_code_data = pd.read_excel(risk_code_path, header=1) if os.path.exists(risk_code_path) else None
        cust_neft_data = pd.read_excel(cust_neft_list_path, header=0) if os.path.exists(cust_neft_list_path) else None
        table_3 = pd.read_csv(table_3_path) if os.path.exists(table_3_path) else None
        table_4 = pd.read_csv(table_4_path) if os.path.exists(table_4_path) else None
        table_5 = pd.read_csv(table_5_path) if os.path.exists(table_5_path) else None
        #oriental_table_1 = pd.read_csv(oriental_table_1_path) if os.path.exists(oriental_table_1_path) else None
       # oriental_table_2 = pd.read_csv(oriental_table_2_path) if os.path.exists(oriental_table_2_path) else None
      #  oriental_table_3 = pd.read_csv(oriental_table_3_path) if os.path.exists(oriental_table_3_path) else None

        return template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5# oriental_table_1, oriental_table_2, oriental_table_3
    except Exception as e:
        print(f"Error reading lookup files: {str(e)}")
        raise

def parse_custom_date(date_str):
    if pd.isnull(date_str):
        return None
    try:
        return pd.to_datetime(date_str, format='%d-%b-%y').strftime('%d/%m/%Y')
    except ValueError:
        try:
            return pd.to_datetime(date_str, format='%d-%b-%Y').strftime('%d/%m/%Y')
        except ValueError:
            return None

def parse_date(date_str):
    # Try parsing the date in multiple formats
    formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%y']
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str), fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT

def process_new_india_assurance(file, template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5, subject, column_mapping):

    print("Column Mappings Passed to the Function:", column_mapping)

    try:
        # Dynamically reading the file based on its extension
        file_extension = os.path.splitext(file)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file, header=14)
        elif file_extension == '.ods':
            data = pd.read_excel(file, engine='odf', header=14)
        elif file_extension == '.xls':
            data = pd.read_excel(file, engine='xlrd', header=14)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file, engine='pyxlsb', header=14)        
        elif file_extension == '.csv':
            data = pd.read_csv(file, header=14)
        elif file_extension == '.txt':
            data = pd.read_csv(file, delimiter='\t', header=14)  # Assuming tab-separated, modify delimiter if needed

        # Select only alternating rows (17, 19, 21, etc.)
        data = data.iloc[1::2]  # Skip every other row, starting from the second row after header
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Stop reading when 'Invoice Number' column contains 'Grand Total'
        if 'Invoice Number' in data.columns:
            stop_index = data[data['Invoice Number'].str.contains('Grand Total', na=False)].index
            if not stop_index.empty:
                data = data.loc[:stop_index[0] - 1]

        # Clean data
        data = data.dropna(how='all')
        data = data.ffill()
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Mapping columns dynamically based on the passed mapping
        for left_col, right_col in column_mapping.items():
            if left_col in data.columns and right_col in template_data.columns:
                template_data[right_col] = data[left_col]

        # Set 'Customer Name' and 'Account Type'
        template_data['Customer Name'] = "The New India Assurance Co. Ltd."
        template_data['Account Type'] = "Customer"
        template_data.rename(columns={
            'Customer Name': 'Debtor Name',
            'Account Type': 'AccountType',
            'Account Type Duplicate': 'AccountTypeDuplicate'
        }, inplace=True)

        # Add Nature of Transaction column with 'Brokerage Statement' in all rows
        template_data['Nature of Transaction'] = "Brokerage Statement"

        # Clean and format Policy No. and Endorsement No. (remove colons, trim spaces)
        for col in ['Policy No.', 'Endorsement No.']:
            if col in template_data.columns:
                template_data[col] = template_data[col].astype(str).str.replace(':', '', regex=False).str.strip()

        # Converting date columns to dd/mm/yyyy format with leading zeros
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in template_data.columns:
                template_data[column] = template_data[column].apply(parse_date)
                template_data[column] = template_data[column].dt.strftime('%d/%m/%Y').fillna('')

        # Clean 'Premium' and 'Brokerage' columns to remove commas or non-numeric characters
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']
        for column in numeric_columns:
            if column in template_data.columns:
                template_data[column] = template_data[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                template_data[column] = pd.to_numeric(template_data[column], errors='coerce').fillna(0)

        # Calculate the Brokerage Rate based on Premium and Brokerage in the template data
        if 'Premium' in template_data.columns and 'Brokerage' in template_data.columns:
            # Avoid division by zero
            template_data['Brokerage Rate'] = template_data.apply(
                lambda row: (row['Brokerage'] / row['Premium'] * 100) if row['Premium'] != 0 else 0,
                axis=1
            )
            template_data['Brokerage Rate'] = template_data['Brokerage Rate'].round(2)

        for column in numeric_columns:
            if column in template_data.columns:
                template_data[column] = template_data[column].round(2)
                template_data[column] = template_data[column].apply(lambda x: "{0:.2f}".format(x))

        # Perform other transformations based on right-side columns in template_data
        template_data['Risk'] = template_data['Risk'].astype(str).str.split('.').str[0]
        risk_code_data['LOB'] = risk_code_data['LOB'].astype(str).str.split('.').str[0]

        if 'Risk' in template_data.columns and 'LOB' in risk_code_data.columns and 'NAME' in risk_code_data.columns:
            template_data = template_data.merge(risk_code_data[['LOB', 'NAME']], left_on='Risk', right_on='LOB', how='left')
            template_data['Risk'] = template_data['NAME']
            template_data.drop(columns=['LOB', 'NAME'], inplace=True)

        template_data = template_data.merge(cust_neft_data[['Name', 'No.2']], left_on='Debtor Name', right_on='Name', how='left')
        template_data['Debtor Branch Ref'] = template_data['No.2']
        template_data['Service Tax Ledger'] = template_data['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
        template_data.drop(columns=['Name', 'No.2'], inplace=True)

        template_data['TDS Ledger'] = template_data['Debtor Name']

        # Creating the narration in dd/mm/yyyy format
        if not table_4.empty and not table_5.empty:
            date_col = table_4['Date'].iloc[0]
            amount_col = table_4['Amount'].iloc[0]
            supplier_name_col = table_5['SupplierName'].iloc[0]

            # Convert date to dd/mm/yyyy format
            date_col = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            narration = f"BNG NEFT DT-{date_col} rcvd towrds brkg Rs. {amount_col} from {supplier_name_col} with GST 18%"

            template_data['Narration'] = narration
            template_data['Bank Ledger'] = table_4['Bank'].iloc[0]
            template_data['RepDate'] = datetime.today().strftime('%d-%b-%y')
            template_data['NPT'] = ''  # Assuming NPT is empty
            template_data['NPT 2'] = subject
        # Modify Bank Ledger using the lookup
        
        if 'NPT 2' in template_data.columns:
            template_data['NPT 2'] = template_data['NPT 2'].apply(lambda x: x.replace('FW:', '').replace('RE:', '').strip() if isinstance(x, str) else x)

        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'

        }

        if 'Bank Ledger' in template_data.columns:
            for key, value in bank_ledger_lookup.items():
                template_data['Bank Ledger'] = template_data['Bank Ledger'].replace(key,value)

        # Setting P&L JV for the last three rows with Invoice No from table_5
        invoice_no = table_5['Invoice No'].iloc[0]
        template_data['P & L JV'] = template_data.apply(lambda row: 'Endorsement' if pd.notna(row['Endorsement No.']) and str(row['Endorsement No.']).strip() else '', axis=1)

        columns_to_drop = ['Cheque No.', 'Cheque Date']
        template_data.drop(columns=[col for col in columns_to_drop if col in template_data.columns], inplace=True)

        if 'Unnamed: 19' in template_data.columns:
            template_data['AccountTypeDuplicate'] = template_data['AccountType']
            template_data.drop(columns=['Unnamed: 19'], inplace=True)
        else:
            template_data['AccountTypeDuplicate'] = template_data['AccountType']

        # Remove last row if it's a total or empty
        template_data = template_data[~template_data['Policy No.'].str.contains('Total', na=False)]
        template_data = template_data[~template_data['Policy No.'].isnull()]

        gst_tds_2_percent = float(table_3['GST TDS @2%'].iloc[0].replace(',', ''))
        narration_value = float(str(amount_col).replace(',', ''))
        sum_brokerage = template_data['Brokerage'].astype(float).sum()
        gst_tds_18_percent = sum_brokerage * 0.18
        first_new_row_brokerage = gst_tds_18_percent
        second_new_row_brokerage = -gst_tds_2_percent
        total_brokerage_with_new_rows = sum_brokerage + first_new_row_brokerage + second_new_row_brokerage
        third_new_row_brokerage = narration_value - total_brokerage_with_new_rows

        new_rows = pd.DataFrame({
            'Entry No.': '',
            'Debtor Name': template_data['Debtor Name'].iloc[0],
            'Nature of Transaction': ["GST Receipts", "Brokerage Statement", "Brokerage Statement"],
            'AccountType': template_data['AccountType'].iloc[0],
            'Debtor Branch Ref': template_data['Debtor Branch Ref'].iloc[0],
            'Client Name': ["GST @ 18%", "GST-TDS 2% MAH (AY 2025-26)", "TDS Receivable - AY 2025-26"],
            'Policy No.': '',
            'Risk': '',
            'Endorsement No.': ["", "", ""],  # Endorsement No. should be blank for the last 3 rows
            'Policy Type': '',
            'Policy Start Date': '',
            'Policy End Date': '',
            'Premium': '0.00',
            'Brokerage Rate': '',
            'Brokerage': [first_new_row_brokerage, second_new_row_brokerage, third_new_row_brokerage],
            'Narration': template_data['Narration'].iloc[-1],
            'NPT': '',
            'Bank Ledger': template_data['Bank Ledger'].iloc[-1],
            'AccountTypeDuplicate': ['Customer', 'G/L Account', 'G/L Account'],
            'Service Tax Ledger': [template_data['Service Tax Ledger'].iloc[0], '2700054', '2300022'],
            'TDS Ledger': [template_data['TDS Ledger'].iloc[0], 'GST-TDS 2% MAH (AY 2025-26)', 'TDS Receivable - AY 2025-26'],
            'RepDate': template_data['RepDate'].iloc[-1],
            'Branch': template_data['Branch'].iloc[-1],
            'Income category': template_data['Income category'].iloc[-1],
            'ASP Practice': template_data['ASP Practice'].iloc[-1],
            'P & L JV': invoice_no,  # Invoice No for last 3 rows
            'NPT 2': template_data['NPT 2'].iloc[-1]
        })

        # Ensure numeric columns in new_rows are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        template_data = pd.concat([template_data, new_rows], ignore_index=True)

        # After concatenating new rows, reassign 'Entry No.'
        template_data['Entry No.'] = range(1, len(template_data) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT 2'
       ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in template_data.columns:
                template_data[col] = ''

        template_data = template_data[desired_columns]

        # Generate the shortened subject and timestamp for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.','').replace(':','')  # Shorten to 50 characters
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # File paths for Excel and CSV files
        excel_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\New India Assurance Template Files\excel_file'
        csv_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\New India Assurance Template Files\csv_file'

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)
        
        excel_file_path = os.path.join(excel_dir, f'{short_subject}_{timestamp}.xlsx')
        csv_file_path = os.path.join(csv_dir, f'{short_subject}_{timestamp}.csv')

        print(excel_file_path)
        print(csv_file_path)
        # Save files
        template_data.to_excel(excel_file_path, index=False)
        template_data.to_csv(csv_file_path, index=False)

        return template_data, excel_file_path

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise

def process_oriental_insurance_co(file_path, template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=None)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=None)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=None)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=None)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=None)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=None)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        # Clean column names and data
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Initialize variables
        dataframes = []
        header_indices = []

        # Identify header rows based on specific keywords
        for idx, row in data.iterrows():
            # Convert all cells to string for consistent comparison
            row_str = row.astype(str)

            if row_str.str.contains('OFFICE_CODE', case=False, na=False).any():
                header_indices.append(idx)
            elif row_str.str.contains('Marsh Code', case=False, na=False).any():
                header_indices.append(idx)
        # Add the last row index to capture the final section
        header_indices.append(len(data))

        # Split data into sections based on header indices (Removed duplicate loop)
        for i in range(len(header_indices) - 1):
            start_idx = header_indices[i]
            end_idx = header_indices[i + 1]

            header_row = data.iloc[start_idx].values
            df_section = data.iloc[start_idx + 1:end_idx].reset_index(drop=True)

            # Remove empty rows in df_section
            df_section = df_section.dropna(how='all').reset_index(drop=True)
            df_section.columns = header_row

            # Append the dataframe to the list if it's not empty
            if not df_section.empty:
                dataframes.append(df_section)

        # Now process each dataframe
        processed_dataframes = []
        csv_file_paths = []
        excel_file_paths = []
        for idx, df in enumerate(dataframes):
            # Remove any completely empty rows
            df = df.dropna(how='all').reset_index(drop=True)

            # Remove rows where only 'Comm' column has a value
            if 'Comm' in df.columns:
                df = df[~((df.notna().sum(axis=1) == 1) & (df['Comm'].notna()))].reset_index(drop=True)

            # Clean column names and data
            df.columns = df.columns.str.strip()
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # Create a copy of the template_data
            processed_df = template_data.copy()

            # Process mappings from frontend (attachment columns on left, template columns on right)
            for attachment_col, template_col in mappings.items():
                if attachment_col in df.columns:
                    processed_df[template_col] = df[attachment_col]
                else:
                    processed_df[template_col] = ''

            # Format 'Policy Start Date' and 'Policy End Date' into 'dd/mm/yyyy'
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if not pd.isnull(x) and x != '' else '')
                    processed_df[column] = processed_df[column].fillna('')

            # Create necessary columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            # Rename 'Client Name' column to 'Debtor Name' and create 'AccountTypeDuplicate'
            if 'Client Name' in processed_df.columns:
                processed_df['Debtor Name'] = processed_df['Client Name']
            else:
                processed_df['Debtor Name'] = ''
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            # NPT2 should be the whole subject with 'FW:' and 'RE:' removed
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()

            # Add other necessary columns with empty values or default values
            processed_df['Debtor Branch Ref'] = ''
            processed_df['Branch'] = ''
            processed_df['Income category'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['P & L JV'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Combine 'REP_VALUE_70' and 'Risk' columns into 'Risk' column with a dash
            if 'REP_VALUE_70' in df.columns and 'Risk' in df.columns:
                processed_df['Risk'] = df['REP_VALUE_70'].astype(str) + '-' + df['Risk'].astype(str)
            elif 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk']
            else:
                processed_df['Risk'] = ''

            # Clean numeric columns (Handled negative numbers correctly)
            numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '')
                    # Replace '(' with '-' and remove ')'
                    processed_df[column] = processed_df[column].str.replace('(', '', regex=False).str.replace(')', '', regex=False)
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
                else:
                    processed_df[column] = 0.00  # Ensure numeric zero value

            # Ensure 'Policy No.' and 'Endorsement No.' are included if available
            if 'Policy No.' not in processed_df.columns and 'Policy No.' in df.columns:
                processed_df['Policy No.'] = df['Policy No.']
            if 'Endorsement No.' not in processed_df.columns and 'Endorsement No.' in df.columns:
                processed_df['Endorsement No.'] = df['Endorsement No.']

            # === Begin of Added Code ===

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
            table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
            table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
            amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
            amount_matching_row = table_4.loc[amount_matching_row_index]
            narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
            bank_value = amount_matching_row['Bank']
            date_col = amount_matching_row['Date']
            insurer_name = amount_matching_row['Insurer Name']
            invoice_no = amount_matching_row['Invoice No']
            narration_from_table_4 = amount_matching_row['Narration']

            # Set 'NPT2' using 'Narration' from table_4
            processed_df['NPT2'] = narration_from_table_4

            # Remove special characters from 'Narration' for file naming
            # Convert narration_from_table_4 to string to avoid TypeError
            safe_narration = ''.join(e for e in str(narration_from_table_4) if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'] == insurer_name]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''

            # Set 'Debtor Branch Ref' in processed_df
            processed_df['Debtor Branch Ref'] = debtor_branch_ref

            # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '', regex=False)

            # Set 'Debtor Name' as 'Insurer Name'
            processed_df['Debtor Name'] = insurer_name

            # Get 'SupplierName' and 'SupplierState' from table_5.csv matching 'TotalTaxAmt' closest to sum_brokerage
            table_5['TotalTaxAmt_cleaned'] = table_5['TotalTaxAmt'].astype(str).str.replace(',', '').str.replace(')', '').str.replace('(', '').astype(float)
            table_5['Brokerage_Diff'] = abs(table_5['TotalTaxAmt_cleaned'] - sum_brokerage)
            matching_row_index = table_5['Brokerage_Diff'].idxmin()
            matching_row = table_5.loc[matching_row_index]
            supplier_state =  matching_row['SupplierState'] if 'SupplierState' in matching_row and pd.notnull(matching_row['SupplierState']) else matching_row.get('MarshState','')
            supplier_name_col = matching_row['SupplierName'] if 'SupplierName' in matching_row and pd.notnull(matching_row['SupplierName']) else matching_row.get('Insurer','')

            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Read 'Chart of Account' file
            chart_of_account_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\Chart of Account.xlsx'
            chart_of_account = pd.read_excel(chart_of_account_path)
            chart_of_account['SupplierState'] = chart_of_account['SupplierState'].astype(str).str.strip()

            # Get 'Name-AY 2025-26' and 'Gl No' based on 'SupplierState'
            chart_matching_rows = chart_of_account[chart_of_account['SupplierState'] == supplier_state]
            if not chart_matching_rows.empty:
                name_ay_2025_26 = chart_matching_rows['Name-AY 2025-26'].iloc[0]
                gl_no = chart_matching_rows['Gl No'].iloc[0]
            else:
                name_ay_2025_26 = ''
                gl_no = ''

            # Get 'GST TDS' from table_3.csv

            # Check for 'TotalTaxAmt' or 'Total' column
            if 'TotalTaxAmt' in table_3.columns:
                total_tax_amt_col = 'TotalTaxAmt'
            elif 'Total' in table_3.columns:
                total_tax_amt_col = 'Total'
            else:
                raise ValueError("Neither 'TotalTaxAmt' nor 'Total' column found in table_3.csv")

            # Check for 'GST TDS' or 'GST TDS @2%' column
            if 'GST TDS' in table_3.columns:
                gst_tds_col = 'GST TDS'
            elif 'GST TDS @2%' in table_3.columns:
                gst_tds_col = 'GST TDS @2%'
            else:
                raise ValueError("Neither 'GST TDS' nor 'GST TDS @2%' column found in table_3.csv")

            # Clean and convert the columns to float
            table_3['TotalTaxAmt_cleaned'] = table_3[total_tax_amt_col].astype(str).str.replace(',', '').astype(float)
            table_3['GST TDS_cleaned'] = table_3[gst_tds_col].astype(str).str.replace(',', '').astype(float)

            # Calculate the difference between 'TotalTaxAmt' and sum of brokerage
            table_3['Brokerage_Diff'] = abs(table_3['TotalTaxAmt_cleaned'] - sum_brokerage)

            # Find the row with the minimum difference
            gst_tds_matching_row_index = table_3['Brokerage_Diff'].idxmin()
            gst_tds_matching_row = table_3.loc[gst_tds_matching_row_index]

            # Get the 'GST TDS' value
            gst_tds_2_percent = gst_tds_matching_row['GST TDS_cleaned']

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Create narration using original amount and no space between 'Rs.' and amount
            narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"

            # Set 'Narration' in processed_df
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' as in New India
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
            processed_df['TDS Ledger'] = processed_df['Debtor Name']

            # Handle 'Endorsement No.' column: remove zeros, set to null if zero
            processed_df['Endorsement No.'] = processed_df['Endorsement No.'].replace(['0', 0], '').replace('', np.nan)

            # Set 'P & L JV': if 'Endorsement No.' is not null or empty, set to 'Endorsement', else blank
            processed_df['P & L JV'] = processed_df['Endorsement No.'].apply(lambda x: 'Endorsement' if pd.notna(x) and str(x).strip() else '')

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                # Avoid division by zero
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Handle 'Branch' column
            # 'DO' column from attachment file 'df'
            if 'DO' in df.columns:
                do_values = df['DO']
                processed_df['DO'] = do_values

                # For each 'DO' value, find matching 'SupplierCode' in 'table_5'
                def get_branch(do_value):
                    if pd.isnull(do_value):
                        return ''
                    do_value_str = str(do_value).strip()
                    # Find rows where 'SupplierCode' contains the 'DO' value
                    supplier_code_match = table_5[table_5['SupplierCode'].astype(str).str.contains(do_value_str, na=False)]
                    if not supplier_code_match.empty:
                        supplier_state = supplier_code_match['SupplierState'].iloc[0]
                        # Read 'state_lookups.xlsx'
                        state_lookups_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx'
                        state_lookups = pd.read_excel(state_lookups_path)
                        state_lookups['state'] = state_lookups['state'].astype(str).str.strip().str.lower()
                        state_lookups['shortform'] = state_lookups['shortform'].astype(str).str.strip().str.lower()
                        # Find 'shortform' for the 'supplier_state'
                        shortform_match = state_lookups[state_lookups['state'] == supplier_state.lower()]
                        if not shortform_match.empty:
                            shortform = shortform_match['shortform'].iloc[0]
                            return shortform
                    return ''

                processed_df['Branch'] = processed_df['DO'].apply(get_branch)
                # Drop 'DO' column as it's not needed in final output
                processed_df.drop(columns=['DO'], inplace=True)
            else:
                processed_df['Branch'] = ''

            # Round numeric columns to 2 decimal places and format
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].round(2)
                    processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

            # Calculate Brokerage values for the new rows
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent
            second_new_row_brokerage = -gst_tds_2_percent
            total_brokerage_with_new_rows = sum_brokerage + first_new_row_brokerage + second_new_row_brokerage
            # Convert 'narration_value_original' to float after removing commas
            narration_value_float = float(str(narration_value_original).replace(',', ''))
            third_new_row_brokerage = narration_value_float - total_brokerage_with_new_rows

            # Create the three additional rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", name_ay_2025_26, "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", "", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, second_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account', 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    gl_no,  # For second new row, use 'Gl No' from 'Chart of Account'
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], name_ay_2025_26, 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': ['','',''],
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no, invoice_no, invoice_no],  # Set 'P & L JV' blank in new rows
                'NPT2': processed_df['NPT2'].iloc[-1]
            })

            # Ensure numeric columns are formatted properly
            for column in numeric_columns:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
                'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
                'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
            ]

            # Ensure all desired columns are present and others are blank
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # === End of Added Code ===

            # Remove empty rows in processed_df (rows where all columns except 'Entry No.' are empty)
            processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

            # Update 'Entry No.' after removing empty rows
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Append to processed_dataframes list
            processed_dataframes.append(processed_df)

            # Generate the shortened subject and date for the filename
            short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
            short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
            date_str = datetime.now().strftime("%Y%m%d")  # Date only

            # Define output directories
            base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Oriental Insurance Template Files'
            excel_dir = os.path.join(base_dir, 'excel_file')
            csv_dir = os.path.join(base_dir, 'csv_file')

            # Ensure directories exist
            os.makedirs(excel_dir, exist_ok=True)
            os.makedirs(csv_dir, exist_ok=True)

            # Save each processed dataframe
            excel_file_name = f'{short_narration}_section_{idx+1}_{date_str}.xlsx'
            csv_file_name = f'{short_narration}_section_{idx+1}_{date_str}.csv'
            excel_file_path = os.path.join(excel_dir, excel_file_name)
            csv_file_path = os.path.join(csv_dir, csv_file_name)
            processed_df.to_excel(excel_file_path, index=False)
            processed_df.to_csv(csv_file_path, index=False)
            print(f"Saved Excel file: {excel_file_path}")
            print(f"Saved CSV file: {csv_file_path}")

            # Collect the file paths
            # Collect the csv file paths
            excel_file_paths.append(excel_file_path)
        # Return the first processed dataframe and the path to the first CSV file
        return processed_dataframes[0], excel_file_paths[0]
    except Exception as e:
        print(f"Error processing Oriental Insurance Co data: {str(e)}")
        raise


def process_united_india_insurance(file_path, template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension
        file_extension = os.path.splitext(file_path)[1].lower()
 
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=None)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=None)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=None)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=None)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=None)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=None)
        else:
            raise ValueError("Unsupported file format")
 
        # Remove empty rows to avoid empty dataframes
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        # Clean column names and data
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        # Initialize variables
        dataframes = []
        header_indices = []
 
        # Identify header rows based on specific keywords ('OFFICE_CODE')
        for idx, row in data.iterrows():
            # Convert all cells to string for consistent comparison
            row_str = row.astype(str)

            if row_str.str.contains('OFFICE_CODE', case=False, na=False).any():
                header_indices.append(idx)

        # Add the last row index to capture the final section
        header_indices.append(len(data))

        # Check if any headers were found
        if len(header_indices) <= 1:
            # If no 'OFFICE_CODE' found, use the first row as header
            header_indices = [0, len(data)]

        # Split data into sections based on header indices
        for i in range(len(header_indices) - 1):
            start_idx = header_indices[i]
            end_idx = header_indices[i + 1]

            # If we are using the first row as header
            if start_idx == 0:
                header_row = data.iloc[start_idx].values
                df_section = data.iloc[start_idx + 1:end_idx].reset_index(drop=True)
            else:
                header_row = data.iloc[start_idx].values
                df_section = data.iloc[start_idx + 1:end_idx].reset_index(drop=True)

            # Remove empty rows in df_section
            df_section = df_section.dropna(how='all').reset_index(drop=True)
            df_section.columns = header_row

            # Append the dataframe to the list if it's not empty
            if not df_section.empty:
                dataframes.append(df_section)

 
        # Now process each dataframe
        processed_dataframes = []
        csv_file_paths = []
        excel_file_paths = []
        for idx, df in enumerate(dataframes):
            # Remove any completely empty rows
            df = df.dropna(how='all').reset_index(drop=True)

            # Clean column names and data
            df.columns = df.columns.str.strip()
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # Remove last row if only 'Premium' or 'Brokerage' has a value
            if not df.empty:
                last_row = df.iloc[-1]
                non_empty_cols = last_row.dropna().index.tolist()
                premium_brokerage_cols = ['Premium', 'Brokerage']
                # Check if non-empty columns are subset of premium_brokerage_cols
                if set(non_empty_cols).issubset(premium_brokerage_cols):
                    # Drop the last row
                    df = df.iloc[:-1].reset_index(drop=True)

            # Create a copy of the template_data
            processed_df = template_data.copy()
 
            # Process mappings from frontend (attachment columns on left, template columns on right)
            for attachment_col, template_col in mappings.items():
                if attachment_col in df.columns:
                    processed_df[template_col] = df[attachment_col]
                else:
                    processed_df[template_col] = ''
 
            # Format 'Policy End Date' into 'dd/mm/yyyy'
            date_columns = ['Policy End Date', 'Policy Start Date']
            for column in date_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if not pd.isnull(x) and x != '' else '')
                    processed_df[column] = processed_df[column].fillna('')

 
            # Create necessary columns similar to Oriental Insurance processing
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = processed_df['Client Name']
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['Income category'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['P & L JV'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''
            processed_df['Policy Start Date'] = ''
 
            # Handle 'Policy No.' and 'Endorsement No.'
            if 'Policy No.' in processed_df.columns:
                def split_policy_no(policy_no):
                    if pd.isnull(policy_no):
                        return policy_no, ''
                    parts = str(policy_no).split('/')
                    if len(parts) > 1:
                        endorsement_no = parts[-1]
                        if endorsement_no == '0':
                            endorsement_no = ''
                        policy_no_main = '/'.join(parts[:-1])
                    else:
                        endorsement_no = ''
                        policy_no_main = policy_no
                    return policy_no, endorsement_no
                policy_endorsement = processed_df['Policy No.'].apply(split_policy_no)
                processed_df['Policy No.'], processed_df['Endorsement No.'] = zip(*policy_endorsement)
 
            # Clean numeric columns
            numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
                else:
                    processed_df[column] = 0.00  # Ensure numeric zero value
 
            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                # Avoid division by zero
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)
 
            # Round numeric columns to 2 decimal places and format
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].round(2)
                    processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))
 
            # Handle 'Branch' column using 'OFFICE_NAME' from the attachment
            if 'OFFICE_NAME' in df.columns:
                processed_df['Branch'] = df['OFFICE_NAME']
            else:
                processed_df['Branch'] = ''
 
            # === Begin of Additional Processing ===
 
            # The rest of the processing mirrors Oriental Insurance logic
 
            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()
 
            # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
            table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
            table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
            amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
            amount_matching_row = table_4.loc[amount_matching_row_index]
            narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
            bank_value = amount_matching_row['Bank']
            date_col = amount_matching_row['Date']
            insurer_name = amount_matching_row['Insurer Name']
            invoice_no = amount_matching_row['Invoice No']
            narration_from_table_4 = amount_matching_row['Narration'] if 'Narration' in amount_matching_row and pd.notnull(amount_matching_row['Narration']) else amount_matching_row.get('Narration (Ref)','')
 
            # Set 'NPT2' using 'Narration' from table_4
            processed_df['NPT2'] = narration_from_table_4
 
            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in str(narration_from_table_4) if e.isalnum() or e == ' ').strip()
 
            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'] == insurer_name]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
 
            # Set 'Debtor Branch Ref' in processed_df
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
 
            # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
 
            # Set 'Debtor Name' as 'Insurer Name'
            processed_df['Debtor Name'] = insurer_name
 
            # Get 'SupplierName' and 'SupplierState' from table_5.csv matching 'TotalTaxAmt' closest to sum_brokerage
            table_5['TotalTaxAmt_cleaned'] = table_5['TotalTaxAmt'].astype(str).str.replace(',', '').astype(float)
            table_5['Brokerage_Diff'] = abs(table_5['TotalTaxAmt_cleaned'] - sum_brokerage)
            matching_row_index = table_5['Brokerage_Diff'].idxmin()
            matching_row = table_5.loc[matching_row_index]
            supplier_state =  matching_row['SupplierState'] if 'SupplierState' in matching_row and pd.notnull(matching_row['SupplierState']) else matching_row.get('MarshState','')
            supplier_name_col = matching_row['SupplierName']
 
            # Read 'Chart of Account' file
            chart_of_account_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\Chart of Account.xlsx'
            chart_of_account = pd.read_excel(chart_of_account_path)
            chart_of_account['SupplierState'] = chart_of_account['SupplierState'].astype(str).str.strip().str.lower()
            supplier_state = str(supplier_state).strip().lower()

            # Convert columns to lowercase before mapping
            # Get 'Name-AY 2025-26' and 'Gl No' based on 'SupplierState'
            chart_matching_rows = chart_of_account[chart_of_account['SupplierState'] == supplier_state]
            if not chart_matching_rows.empty:
                name_ay_2025_26 = chart_matching_rows['Name-AY 2025-26'].iloc[0]
                gl_no = chart_matching_rows['Gl No'].iloc[0]
            else:
                name_ay_2025_26 = ''
                gl_no = ''
 
            # Get 'GST TDS' from table_3.csv
            # Check for 'TotalTaxAmt' or 'Total' column
            if 'TotalTaxAmt' in table_3.columns:
                total_tax_amt_col = 'TotalTaxAmt'
            elif 'Total' in table_3.columns:
                total_tax_amt_col = 'Total'
            else:
                raise ValueError("Neither 'TotalTaxAmt' nor 'Total' column found in table_3.csv")
 
            # Check for 'GST TDS' or 'GST TDS @2%' column
            if 'GST TDS' in table_3.columns:
                gst_tds_col = 'GST TDS'
            elif 'GST TDS @2%' in table_3.columns:
                gst_tds_col = 'GST TDS @2%'
            else:
                raise ValueError("Neither 'GST TDS' nor 'GST TDS @2%' column found in table_3.csv")
 
            # Clean and convert the columns to float
            table_3['TotalTaxAmt_cleaned'] = table_3[total_tax_amt_col].astype(str).str.replace(',', '').astype(float)
            table_3['GST TDS_cleaned'] = table_3[gst_tds_col].astype(str).str.replace(',', '').astype(float)
 
            # Calculate the difference between 'TotalTaxAmt' and sum of brokerage
            table_3['Brokerage_Diff'] = abs(table_3['TotalTaxAmt_cleaned'] - sum_brokerage)
 
            # Find the row with the minimum difference
            gst_tds_matching_row_index = table_3['Brokerage_Diff'].idxmin()
            gst_tds_matching_row = table_3.loc[gst_tds_matching_row_index]
 
            # Get the 'GST TDS' value
            gst_tds_2_percent = gst_tds_matching_row['GST TDS_cleaned']
 
            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
             # Check if 'GST' column exists in data
            gst_present = any(
                'GST' in col or 'GST @18%' in col for col in data.columns
            )

            # Create narration using original amount and no space between 'Rs.' and amount
            if gst_present:
                narration = (
                    f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs."
                    f"{narration_value_original} from {supplier_name_col} with GST"
                )
            else:
                narration = (
                    f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs."
                    f"{narration_value_original} from {supplier_name_col} without GST 18%"
                )
 
            # Set 'Narration' in processed_df
            processed_df['Narration'] = narration
 
            # Map 'Bank Ledger' as in Oriental Insurance
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'

            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value
 
            # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
 
            # Set 'P & L JV'
            processed_df['P & L JV'] = processed_df['Endorsement No.'].apply(lambda x: 'Endorsement' if pd.notna(x) and str(x).strip() else '')
 
            # For 'Branch', use 'OFFICE_NAME' from attachment, get 'SupplierState', and map using 'branch_lookups.xlsx'
            branch_lookups_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx'
            branch_lookups = pd.read_excel(branch_lookups_path)
            branch_lookups['state'] = branch_lookups['state'].astype(str).str.strip().str.lower()
            branch_lookups['shortform'] = branch_lookups['shortform'].astype(str).str.strip()
            supplier_state = str(supplier_state).strip().lower()
 
            # Map 'Branch' using 'SupplierState'
            branch_match = branch_lookups[branch_lookups['state'] == supplier_state]
            if not branch_match.empty:
                branch_value = branch_match['shortform'].iloc[0]
            else:
                branch_value = ''
            processed_df['Branch'] = branch_value
 
            # Calculate Brokerage values for the new rows
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent
            second_new_row_brokerage = -gst_tds_2_percent
            total_brokerage_with_new_rows = sum_brokerage + first_new_row_brokerage + second_new_row_brokerage
            # Convert 'narration_value_original' to float after removing commas
            narration_value_float = float(str(narration_value_original).replace(',', ''))
            third_new_row_brokerage = narration_value_float - total_brokerage_with_new_rows
 
            # Create the three additional rows (same as Oriental Insurance)
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", name_ay_2025_26, "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", "", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, second_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account', 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    gl_no,  # For second new row, use 'Gl No' from 'Chart of Account'
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], name_ay_2025_26, 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': ['','',''],
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no, invoice_no, invoice_no],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })
 
            # Ensure numeric columns are formatted properly
            for column in numeric_columns:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))
 
            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)
 
            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
 
            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
                'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
                'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
            ]
 
            # Ensure all desired columns are present
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]
 
            # === End of Additional Processing ===
 
            # Remove empty rows in processed_df
            processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)
 
            # Update 'Entry No.' after removing empty rows
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
 
            # Append to processed_dataframes list
            processed_dataframes.append(processed_df)
 
            # Generate the shortened subject and date for the filename
            short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
            short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
            date_str = datetime.now().strftime("%Y%m%d")  # Date only
 
            # Define output directories for United India Insurance
            base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\United India Insurance Template Files'
            excel_dir = os.path.join(base_dir, 'excel_file')
            csv_dir = os.path.join(base_dir, 'csv_file')
 
            # Ensure directories exist
            os.makedirs(excel_dir, exist_ok=True)
            os.makedirs(csv_dir, exist_ok=True)
 
            # Save each processed dataframe
            excel_file_name = f'{short_narration}_section_{idx+1}_{date_str}.xlsx'
            csv_file_name = f'{short_narration}_section_{idx+1}_{date_str}.csv'
            excel_file_path = os.path.join(excel_dir, excel_file_name)
            csv_file_path = os.path.join(csv_dir, csv_file_name)
            processed_df.to_excel(excel_file_path, index=False)
            processed_df.to_csv(csv_file_path, index=False)
            print(f"Saved Excel file: {excel_file_path}")
            print(f"Saved CSV file: {csv_file_path}")
 
            # Collect the file paths
 
            excel_file_paths.append(excel_file_path)
 
        # Return the first processed dataframe and the path to the first Excel file
        return processed_dataframes[0], excel_file_paths[0]
 
    except Exception as e:
        print(f"Error processing United India Insurance data: {str(e)}")
        raise

def process_tata_aia_insurance(file_path, template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, assuming the first row is the header
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)

        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Remove rows where only 'Comm' column has data or only 'Premium' column has data, and rest are blank
        if 'Comm' in data.columns and 'Premium' in data.columns:
            data = data[~(((data['Comm'].notna()) & (data.drop(columns=['Comm']).isna().all(axis=1))) |
                          ((data['Premium'].notna()) & (data.drop(columns=['Premium']).isna().all(axis=1))))].reset_index(drop=True)
        elif 'Comm' in data.columns:
            data = data[~((data['Comm'].notna()) & (data.drop(columns=['Comm']).isna().all(axis=1)))].reset_index(drop=True)
        elif 'Premium' in data.columns:
            data = data[~((data['Premium'].notna()) & (data.drop(columns=['Premium']).isna().all(axis=1)))].reset_index(drop=True)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            # If mappings are not provided, proceed without them
            pass

        # Create necessary columns
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df.get('Client Name', 'Tata AIA Insurance Co. Ltd.')
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['Income category'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['P & L JV'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''
        processed_df['Policy Start Date'] = ''
        processed_df['Policy End Date'] = ''
        processed_df['Endorsement No.'] = ''  # Endorsement No. is blank

        # Clean numeric columns
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00  # Ensure numeric zero

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # Handle 'Branch' column (No branch logic required)
        processed_df['Branch'] = ''

        # === Begin of Additional Processing ===

        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
        table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
        amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
        amount_matching_row = table_4.loc[amount_matching_row_index]
        narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
        bank_value = amount_matching_row['Bank']
        date_col = amount_matching_row['Date']
        insurer_name = amount_matching_row['Insurer Name']
        invoice_no = amount_matching_row['Invoice No']
        narration_from_table_4 = amount_matching_row['Narration'] if 'Narration' in amount_matching_row and pd.notnull(amount_matching_row['Narration']) else amount_matching_row.get('Narration (Ref)','')

        # Set 'NPT2' using 'Narration' from table_4
        processed_df['NPT2'] = narration_from_table_4

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'] == insurer_name]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Get 'GST TDS' from table_3.csv
        table_5['TotalTaxAmt_cleaned'] = table_5['TotalTaxAmt'].astype(str).str.replace(',', '').astype(float)
        table_5['Brokerage_Diff'] = abs(table_5['TotalTaxAmt_cleaned'] - sum_brokerage)
        matching_row_index = table_5['Brokerage_Diff'].idxmin()
        matching_row = table_5.loc[matching_row_index]
        supplier_name_col = matching_row['SupplierName'] if 'SupplierName' in matching_row and pd.notnull(matching_row['SupplierName']) else matching_row.get('Insurer','')

        gst_columns = [col for col in table_3.columns if 'GST' in col.upper()]
        has_gst = len(gst_columns) > 0

        if has_gst:
            # GST column is available
            gst_tds_col = gst_columns[0]  # Use the first GST column found
            # Clean and convert the GST TDS column to float
            table_3['GST_TDS_cleaned'] = table_3[gst_tds_col].astype(str).str.replace(',', '').astype(float)
            gst_tds_2_percent = table_3['GST_TDS_cleaned'].iloc[0]
        else:
            # GST column is not available
            gst_tds_2_percent = 0.00

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Create narration using original amount and no space between 'Rs.' and amount
        gst_present = any(
            'GST' in col or 'GST @18%' in col for col in data.columns
        )

            # Create narration using original amount and no space between 'Rs.' and amount
        if gst_present:
            narration = (
                f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            )
        else:
            narration = (
                f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            )

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'

        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Set 'P & L JV'
        processed_df['P & L JV'] = ''

        # Since 'SupplierState' is not needed, we can hardcode or set default values for 'Name-AY 2025-26' and 'Gl No'
        # For this example, I'll set them to empty strings or default values
        name_ay_2025_26 = 'TDS Receivable - AY 2025-26'  # Or any default value as per your requirement
        gl_no = '2300022'  # Default GL number for TDS

        # Calculate Brokerage values for the new rows

        sum_brokerage = processed_df['Brokerage'].astype(float).sum()
        narration_value_float = float(str(narration_value_original).replace(',', ''))
        if has_gst:
            # GST is applicable
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent
            second_new_row_brokerage = -gst_tds_2_percent
            total_brokerage_with_new_rows = sum_brokerage + first_new_row_brokerage + second_new_row_brokerage
            third_new_row_brokerage = narration_value_float - total_brokerage_with_new_rows
        else:
            # GST is not applicable
            first_new_row_brokerage = 0.00
            second_new_row_brokerage = 0.00
            third_new_row_brokerage = narration_value_float - sum_brokerage

        # Ensure 'AccountType' does not have 'G/L Account' in any rows
        processed_df['AccountType'] = processed_df['AccountType'].replace('G/L Account', 'Customer')

        # Create the additional rows based on GST availability
        if has_gst:
            # Add three rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement", "Brokerage Statement"],
                'AccountType': [processed_df['AccountType'].iloc[0], processed_df['AccountType'].iloc[0], processed_df['AccountType'].iloc[0]],
                'Debtor Branch Ref': [processed_df['Debtor Branch Ref'].iloc[0], '', ''],
                'Client Name': ["GST @ 18%", name_ay_2025_26, "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': '',
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, second_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': ['Customer', 'G/L Account', 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    gl_no,  # Use default or hardcoded value
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], name_ay_2025_26, 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no, invoice_no, invoice_no],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })
        else:
            # Add only one row for TDS
            third_new_row_brokerage = narration_value_float - sum_brokerage
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': '',
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': 'G/L Account',
                'Service Tax Ledger': '2300022',  # TDS Ledger
                'TDS Ledger': 'TDS Receivable - AY 2025-26',
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': invoice_no,
                'NPT2': processed_df['NPT2'].iloc[-1]
            })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order

        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # === End of Additional Processing ===

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for Tata AIA Insurance

        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Tata AIA Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Ensure that numeric columns are cast as numbers before saving
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file

        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Tata AIA Insurance data: {str(e)}")
        raise

def process_icici_lombard_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                    table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, engine="calamine",header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)

        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Remove rows where only 'Comm' column has data or only 'Premium' column has data, and rest are blank
        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            # If mappings are not provided, proceed without them
            pass
        print(processed_df)
        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
        print(processed_df['Policy Start Date'])
        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

        if 'Policy No.' in processed_df.columns:
            # Extract numbers after last '/'
            def extract_endorsement_no(policy_no):
                if pd.isnull(policy_no):
                    return ''
                policy_no_str = str(policy_no)
                parts = policy_no_str.split('/')
                if len(parts) > 1:
                    endorsement_no = parts[-1]
                    if endorsement_no == '000':
                        return ''
                    else:
                        return endorsement_no
                else:
                    return ''
            processed_df['Endorsement No.'] = processed_df['Policy No.'].apply(extract_endorsement_no)

        # Now, for 'P & L JV' column, map 'ENDORSEMENT_TYPE' or 'POL_ENDORSEMENT_TYPE' to 'P & L JV'
        if 'ENDORSEMENT_TYPE' in data.columns or 'POL_ENDORSEMENT_TYPE' in data.columns:
            endorsement_column = 'ENDORSEMENT_TYPE' if 'ENDORSEMENT_TYPE' in data.columns else 'POL_ENDORSEMENT_TYPE'
            endorsement_type_mapping = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet3'
            )
            endorsement_type_mapping['Endorsement Type'] = endorsement_type_mapping['Endorsement Type'].astype(str).str.strip().str.lower()
            endorsement_type_mapping['lookup value'] = endorsement_type_mapping['lookup value'].astype(str).str.strip()
            data[endorsement_column] = data[endorsement_column].astype(str).str.strip().str.lower()
            endorsement_lookup = endorsement_type_mapping.set_index('Endorsement Type')['lookup value'].to_dict()
            processed_df['P & L JV'] = data[endorsement_column].map(endorsement_lookup).fillna('')
        else:
            processed_df['P & L JV'] = ''

        # Replace any 'nan' strings with empty strings in 'P & L JV' column
        if 'P & L JV' in processed_df.columns:
            processed_df['P & L JV'] = processed_df['P & L JV'].replace(['nan', 'NaN', np.nan], '')

        # Print the first 10 rows of 'Policy Start Date' and 'Policy End Date' before conversion
        # Calculate 'Brokerage Rate', and format 'Premium' and 'Commission' (assuming 'Commission' is 'Brokerage' column) as numbers rounded to two decimals
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']

        if 'Brokerage' not in processed_df.columns and 'Commission' in processed_df.columns:
            print(r'it did come here thoug')
            processed_df['Brokerage'] = processed_df['Commission']
        for column in ['Premium', 'Brokerage']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # For 'Branch' column, map 'SAS_BRANCH_NAME' or 'BRANCH_NAME' to 'Branch' using 'state_lookups.xlsx' 'Sheet2'
        if 'SAS_BRANCH_NAME' in data.columns or 'BRANCH_NAME' in data.columns:
            branch_column = 'SAS_BRANCH_NAME' if 'SAS_BRANCH_NAME' in data.columns else 'BRANCH_NAME'
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet2'
            )
            state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
            data[branch_column] = data[branch_column].astype(str).str.strip().str.lower()
            branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
            processed_df['Branch'] = data[branch_column].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # For 'Income category' column, map 'BUSINESS_TYPE' to 'Income category' using 'state_lookups.xlsx' 'Sheet4'
        if 'BUSINESS_TYPE' in data.columns:
            state_lookups_sheet4 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet4'
            )
            state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
            state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
            data['BUSINESS_TYPE'] = data['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
            income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
            processed_df['Income category'] = data['BUSINESS_TYPE'].map(income_category_lookup).fillna('')
        else:
            processed_df['Income category'] = ''

        # Create necessary columns similar to 'united'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df['Debtor Name'] if 'Debtor Name' in processed_df.columns else 'ICICI Lombard General Insurance Co. Ltd.'
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''

        # Similar to 'united', calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
        table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
        amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
        amount_matching_row = table_4.loc[amount_matching_row_index]
        narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
        bank_value = amount_matching_row['Bank']
        date_col = amount_matching_row['Date']
        insurer_name = amount_matching_row['Insurer Name']
        invoice_no = amount_matching_row['Invoice No']
        narration_from_table_4 = amount_matching_row['Narration'] if 'Narration' in amount_matching_row and pd.notnull(amount_matching_row['Narration']) else amount_matching_row.get('Narration (Ref)','')

        # Set 'NPT2' using 'Narration' from table_4
        processed_df['NPT2'] = narration_from_table_4

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'] == insurer_name]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Get 'SupplierName' and 'SupplierState' from table_5.csv matching 'TotalTaxAmt' closest to sum_brokerage
        table_5['TotalTaxAmt_cleaned'] = table_5['TotalTaxAmt'].astype(str).str.replace(',', '').astype(float)
        table_5['Brokerage_Diff'] = abs(table_5['TotalTaxAmt_cleaned'] - sum_brokerage)
        matching_row_index = table_5['Brokerage_Diff'].idxmin()
        matching_row = table_5.loc[matching_row_index]
        supplier_name_col = matching_row['SupplierName'] if 'SupplierName' in matching_row and pd.notnull(matching_row['SupplierName']) else matching_row.get('Insurer Name','')

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Create narration using original amount and no space between 'Rs.' and amount
        narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'

        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for the new rows
        gst_tds_18_percent = sum_brokerage * 0.18
        first_new_row_brokerage = gst_tds_18_percent

        # Convert 'narration_value_original' to float after removing commas
        narration_value_float = float(str(narration_value_original).replace(',', ''))

        third_new_row_brokerage = narration_value_float - (sum_brokerage + first_new_row_brokerage)

        # Create the two additional rows (only 1st and 3rd rows like in 'united')
        new_rows = pd.DataFrame({
            'Entry No.': '',
            'Debtor Name': processed_df['Debtor Name'].iloc[0],
            'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
            'AccountType': processed_df['AccountType'].iloc[0],
            'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
            'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
            'Policy No.': '',
            'Risk': '',
            'Endorsement No.': ["", ""],
            'Policy Type': '',
            'Policy Start Date': '',
            'Policy End Date': '',
            'Premium': '0.00',
            'Brokerage Rate': '',
            'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
            'Narration': narration,
            'NPT': '',
            'Bank Ledger': bank_ledger_value,
            'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
            'Service Tax Ledger': [
                processed_df['Service Tax Ledger'].iloc[0],
                '2300022'  # Third new row remains '2300022'
            ],
            'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
            'RepDate': processed_df['RepDate'].iloc[-1],
            'Branch': '',
            'Income category': processed_df['Income category'].iloc[-1],
            'ASP Practice': processed_df['ASP Practice'].iloc[-1],
            'P & L JV': [invoice_no, invoice_no],
            'NPT2': processed_df['NPT2'].iloc[-1]
        })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for ICICI Lombard Insurance
        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\ICICI Lombard Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing ICICI Lombard Insurance data: {str(e)}")
        raise
def parse_date_flexible(date_str):
    """
    A flexible date parser that handles multiple formats, including 'YYYYMMDD'.
    """
    if pd.isnull(date_str) or date_str == '':
        return ''  # Return empty for null or empty strings

    if isinstance(date_str, (float, int)):
        # Handle Excel numeric dates and 'YYYYMMDD' integer format
        date_int = int(date_str)
        if date_int > 59 and date_int < 2958465:
            # Excel date format
            return datetime(1899, 12, 30) + timedelta(days=date_int - 1)
        elif 19000101 <= date_int <= 29991231:
            # 'YYYYMMDD' format
            try:
                return datetime.strptime(str(date_int), '%Y%m%d')
            except ValueError:
                return ''
        else:
            return ''
    elif isinstance(date_str, str):
        # Remove any non-numeric characters for 'YYYYMMDD' format
        date_str_cleaned = ''.join(filter(str.isdigit, date_str))
        date_formats = [
            '%d-%b-%y', '%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d',
            '%d-%m-%Y', '%d/%m/%y', '%Y%m%d'
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_str_cleaned, fmt)
            except ValueError:
                continue  # Try next format if current fails

        # Fallback: Try parsing with pandas' `to_datetime`
        try:
            return pd.to_datetime(date_str, dayfirst=True)
        except ValueError:
            return ''  # Return empty string if parsing fails

    elif isinstance(date_str, datetime):
        return date_str  # Return the datetime object as is

    else:
        return ''  # Return empty for any unsupported types


def process_star_health_insurer(file_path, template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5, subject, mappings):

    try:

        # Read the file based on its extension
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Remove last row if only 'Premium' or 'Brokerage' has a value
        if not data.empty:
            last_row = data.iloc[-1]
            non_empty_cols = last_row.dropna().index.tolist()
            premium_brokerage_cols = ['Premium', 'Brokerage']
            if set(non_empty_cols).issubset(premium_brokerage_cols):
                data = data.iloc[:-1].reset_index(drop=True)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        for attachment_col, template_col in mappings.items():
            if attachment_col in data.columns:
                processed_df[template_col] = data[attachment_col]
            else:
                processed_df[template_col] = ''

        # Format 'Policy Start Date' and 'Policy End Date' into 'dd/mm/yyyy'
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if not pd.isnull(x) and x != '' else '')
                processed_df[column] = processed_df[column].fillna('')

        # Create necessary columns similar to United India Insurance processing
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df.get('Client Name', 'Star Health and Allied Insurance Co. Ltd.')
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['P & L JV'] = ''  # Keep 'P & L JV' as blank
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''
        # Clean numeric columns
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00  # Ensure numeric zero value

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            # Avoid division by zero
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # Handle 'Branch' column using 'STATE NAME' from the attachment
        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet2'
            )
            state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
            processed_df['Branch'] = processed_df['Branch'].astype(str).str.strip().str.lower()
            branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                    sheet_name='Sheet4'
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
                state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
                processed_df['Income category'] = processed_df['Income category'].astype(str).str.strip().str.lower()
                income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                processed_df['Income category'] = processed_df['Income category'].map(income_category_lookup).fillna('')
                print(r'this works!')
            else:
                processed_df['Income category'] = ''
                print(r'why this is else')


        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
        table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
        amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
        amount_matching_row = table_4.loc[amount_matching_row_index]
        narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
        bank_value = amount_matching_row['Bank']
        date_col = amount_matching_row['Date']
        insurer_name = amount_matching_row['Insurer Name']
        narration_from_table_4 = amount_matching_row['Narration'] if 'Narration' in amount_matching_row and pd.notnull(amount_matching_row['Narration']) else amount_matching_row.get('Narration (Ref)','')
        invoice_no = amount_matching_row['Invoice No']  # Get 'Invoice No.' directly from table_4 without matching

        # Set 'NPT2' using 'Narration' from table_4
        processed_df['NPT2'] = narration_from_table_4

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Create narration using original amount and no space between 'Rs.' and amount
        narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {insurer_name} with GST 18%"

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'
        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for the new rows
        gst_tds_18_percent = sum_brokerage * 0.18
        first_new_row_brokerage = gst_tds_18_percent

        # Convert 'narration_value_original' to float after removing commas
        narration_value_float = float(str(narration_value_original).replace(',', ''))

        third_new_row_brokerage = narration_value_float - (sum_brokerage + first_new_row_brokerage)

        # Create the two additional rows (like in ICICI Lombard)
        new_rows = pd.DataFrame({
            'Entry No.': '',
            'Debtor Name': processed_df['Debtor Name'].iloc[0],
            'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
            'AccountType': processed_df['AccountType'].iloc[0],
            'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
            'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
            'Policy No.': '',
            'Risk': '',
            'Endorsement No.': ["", ""],  # Keep 'Endorsement No.' as blank
            'Policy Type': '',
            'Policy Start Date': '',
            'Policy End Date': '',
            'Premium': '0.00',
            'Brokerage Rate': '',
            'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
            'Narration': narration,
            'NPT': '',
            'Bank Ledger': bank_ledger_value,
            'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
            'Service Tax Ledger': [
                processed_df['Service Tax Ledger'].iloc[0],
                '2300022'  # Second new row remains '2300022'
            ],
            'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
            'RepDate': processed_df['RepDate'].iloc[-1],
            'Branch': '',
            'Income category': processed_df['Income category'].iloc[-1],
            'ASP Practice': processed_df['ASP Practice'].iloc[-1],
            'P & L JV': [invoice_no, invoice_no],  # Keep 'P & L JV' as blank
            'NPT2': processed_df['NPT2'].iloc[-1]
        })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for Star Health Insurance
        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Star Health Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Star Health Insurance data: {str(e)}")
        raise
def process_hdfc_life_insurance_co(file_path, template_data, risk_code_data, cust_neft_data,
                                   table_3, table_4, table_5, subject, mappings):
    try:
        import numpy as np  # Import numpy for numerical comparisons

        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            # If mappings are not provided, proceed without them
            pass

        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']

        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

        if 'Policy No.' in processed_df.columns:
            # Extract numbers after last '/'
            def extract_endorsement_no(policy_no):
                if pd.isnull(policy_no):
                    return ''
                policy_no_str = str(policy_no)
                parts = policy_no_str.split('/')
                if len(parts) > 1:
                    endorsement_no = parts[-1]
                    if endorsement_no == '000':
                        return ''
                    else:
                        return endorsement_no
                else:
                    return ''
            processed_df['Endorsement No.'] = processed_df['Policy No.'].apply(extract_endorsement_no)

        # For 'P & L JV' column, map 'ENDORSEMENT_TYPE' or 'POL_ENDORSEMENT_TYPE' to 'P & L JV'
        if 'ENDORSEMENT_TYPE' in data.columns or 'POL_ENDORSEMENT_TYPE' in data.columns:
            endorsement_column = 'ENDORSEMENT_TYPE' if 'ENDORSEMENT_TYPE' in data.columns else 'POL_ENDORSEMENT_TYPE'
            endorsement_type_mapping = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet3'
            )
            endorsement_type_mapping['Endorsement Type'] = endorsement_type_mapping['Endorsement Type'].astype(str).str.strip().str.lower()
            endorsement_type_mapping['lookup value'] = endorsement_type_mapping['lookup value'].astype(str).str.strip()
            data[endorsement_column] = data[endorsement_column].astype(str).str.strip().str.lower()
            endorsement_lookup = endorsement_type_mapping.set_index('Endorsement Type')['lookup value'].to_dict()
            processed_df['P & L JV'] = data[endorsement_column].map(endorsement_lookup).fillna('')
        else:
            processed_df['P & L JV'] = ''

        # For 'Branch' column, map existing 'Branch' column using 'state_lookups.xlsx' 'Sheet2'
        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet2'
            )
            state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
            processed_df['Branch'] = processed_df['Branch'].astype(str).str.strip().str.lower()
            branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # For 'Income category' column, map existing 'Income category' column using 'state_lookups.xlsx' 'Sheet4'
        if 'Income category' in processed_df.columns:
            state_lookups_sheet4 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet4'
            )
            state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
            state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
            processed_df['Income category'] = processed_df['Income category'].astype(str).str.strip().str.lower()
            income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
            processed_df['Income category'] = processed_df['Income category'].map(income_category_lookup).fillna('')
        else:
            processed_df['Income category'] = ''

        # Create necessary columns similar to 'ICICI Lombard'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df['Debtor Name'] if 'Debtor Name' in processed_df.columns else 'HDFC Life Insurance Co. Ltd.'
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''

        # Calculate 'Brokerage Rate', and format 'Premium' and 'Brokerage' as numbers rounded to two decimals
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']

        for column in ['Premium', 'Brokerage']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Check if sum_brokerage is approximately equal to net_amount_value
        # Get 'Net Amount' or the last column from 'table_3'
        net_amount_column = table_3.columns[-1]  # Assuming last column is 'Net Amount'
        # For net_amount_value_formatted with commas
        net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
        net_amount_value = net_amount_values_numeric.sum()
        net_amount_value_formatted = "{:,.2f}".format(net_amount_value)  # Include commas

        # Check if sum_brokerage is approximately equal to net_amount_value
        brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
        table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
        amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
        amount_matching_row = table_4.loc[amount_matching_row_index]
        narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
        bank_value = amount_matching_row['Bank']
        date_col = amount_matching_row['Date']
        insurer_name = amount_matching_row['Insurer Name']
        invoice_no = amount_matching_row['Invoice No']
        narration_from_table_4 = amount_matching_row['Narration'] if 'Narration' in amount_matching_row and pd.notnull(amount_matching_row['Narration']) else amount_matching_row.get('Narration (Ref)', '')

        # Get 'GST' or 'GST @18%' presence in 'table_3' columns
        gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

        # Set 'NPT2' using 'Narration' from table_4
        processed_df['NPT2'] = narration_from_table_4

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Get 'supplier_name_col' from 'table_4'
        supplier_name_col = ''
        for col in ['Insurer Name', 'Insurer', 'SupplierName']:
            if col in table_4.columns and not table_4[col].empty:
                supplier_name_col = table_4[col].iloc[0]
                break

        # Create narration with or without 'with GST 18%' based on 'gst_present' and whether brokerage equals net amount
# Create narration, checking if either the first or last column matches the sum of Brokerage
        first_column_name = table_3.columns[0]
        last_column_name = table_3.columns[-1]

        first_column_value = table_3[first_column_name].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        last_column_value = table_3[last_column_name].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')

        first_column_match = pd.to_numeric(first_column_value, errors='coerce').sum()
        last_column_match = pd.to_numeric(last_column_value, errors='coerce').sum()

# Check if either the first or last column has a match with sum_brokerage
        either_match = np.isclose(sum_brokerage, first_column_match, atol=0.01) or np.isclose(sum_brokerage, last_column_match, atol=0.01)

        if gst_present:
            if either_match:
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original}({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
        else:
            if either_match:
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            else:
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original}({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'
        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for the new rows
        narration_value_float = float(str(narration_value_original).replace(',', ''))

        # Find 'TDS' or 'TDS @10%' column in table_3
        tds_column = None
        for col in table_3.columns:
            if 'TDS' in col or 'TDS @10%' in col:
                tds_column = col
                break

        if tds_column is not None:
            # Sum up the 'TDS' column
            tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
            third_new_row_brokerage = tds_values_numeric.sum()
            third_new_row_client_name = tds_column
        else:
            # Get second column available
            data_columns = table_3.columns.tolist()[1:]  # Exclude first column
            if len(data_columns) >= 1:
                second_column = data_columns[0]
                second_column_values_cleaned = table_3[second_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                second_column_values_numeric = pd.to_numeric(second_column_values_cleaned, errors='coerce').fillna(0)
                third_new_row_brokerage = second_column_values_numeric.sum()
                third_new_row_brokerage = - third_new_row_brokerage
                third_new_row_client_name = second_column
            else:
                third_new_row_brokerage = 0.0
                third_new_row_client_name = ''

        if gst_present:
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent

            # Create the additional rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no, invoice_no],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })
        else:
            # Create the additional row
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': [""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': ['G/L Account'],
                'Service Tax Ledger': [
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for HDFC Life Insurance Co
        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\HDFC Life Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing HDFC Life Insurance Co data: {str(e)}")
        raise

def process_shriram_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                 table_3, table_4, table_5, subject, mappings):
    try:
        import numpy as np  # Import numpy for numerical comparisons

        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            # If mappings are not provided, proceed without them
            pass

        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']

        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

        if 'Policy No.' in processed_df.columns:
            # Extract numbers after last '/'
            def extract_endorsement_no(policy_no):
                if pd.isnull(policy_no):
                    return ''
                policy_no_str = str(policy_no)
                parts = policy_no_str.split('/')
                if len(parts) > 1:
                    endorsement_no = parts[-1]
                    if endorsement_no == '000':
                        return ''
                    else:
                        return endorsement_no
                else:
                    return ''
            processed_df['Endorsement No.'] = processed_df['Policy No.'].apply(extract_endorsement_no)

        # For 'P & L JV' column, map 'ENDORSEMENT_TYPE' or 'POL_ENDORSEMENT_TYPE' to 'P & L JV'
        if 'ENDORSEMENT_TYPE' in data.columns or 'POL_ENDORSEMENT_TYPE' in data.columns:
            endorsement_column = 'ENDORSEMENT_TYPE' if 'ENDORSEMENT_TYPE' in data.columns else 'POL_ENDORSEMENT_TYPE'
            endorsement_type_mapping = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet3'
            )
            endorsement_type_mapping['Endorsement Type'] = endorsement_type_mapping['Endorsement Type'].astype(str).str.strip().str.lower()
            endorsement_type_mapping['lookup value'] = endorsement_type_mapping['lookup value'].astype(str).str.strip()
            data[endorsement_column] = data[endorsement_column].astype(str).str.strip().str.lower()
            endorsement_lookup = endorsement_type_mapping.set_index('Endorsement Type')['lookup value'].to_dict()
            processed_df['P & L JV'] = data[endorsement_column].map(endorsement_lookup).fillna('')
        else:
            processed_df['P & L JV'] = ''

        # For 'Branch' column, map existing 'Branch' column using 'state_lookups.xlsx' 'Sheet2'
        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet2'
            )
            state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
            processed_df['Branch'] = processed_df['Branch'].astype(str).str.strip().str.lower()
            branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # For 'Income category' column, map existing 'Income category' column using 'state_lookups.xlsx' 'Sheet4'
        if 'Income category' in processed_df.columns:
            state_lookups_sheet4 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet4'
            )
            state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
            state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
            processed_df['Income category'] = processed_df['Income category'].astype(str).str.strip().str.lower()
            income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
            processed_df['Income category'] = processed_df['Income category'].map(income_category_lookup).fillna('')
        else:
            processed_df['Income category'] = ''

        # Create necessary columns similar to 'ICICI Lombard'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df['Debtor Name'] if 'Debtor Name' in processed_df.columns else 'Shriram Life Insurance Co. Ltd.'
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''

        # Calculate 'Brokerage Rate', and format 'Premium' and 'Brokerage' as numbers rounded to two decimals
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']

        for column in ['Premium', 'Brokerage']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Net Amount' or the last column from 'table_3'
        net_amount_column = table_3.columns[-1]  # Assuming last column is 'Net Amount'
        net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
        net_amount_value = net_amount_values_numeric.sum()
        net_amount_value_formatted = "{:,.2f}".format(net_amount_value)  # Include commas

        # Check if sum_brokerage is approximately equal to net_amount_value
        brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        table_4['Amount_cleaned'] = table_4['Amount'].astype(str).str.replace(',', '').astype(float)
        table_4['Brokerage_Diff'] = abs(table_4['Amount_cleaned'] - sum_brokerage)
        amount_matching_row_index = table_4['Brokerage_Diff'].idxmin()
        amount_matching_row = table_4.loc[amount_matching_row_index]
        narration_value_original = amount_matching_row['Amount']  # Use original amount with commas
        bank_value = amount_matching_row['Bank']
        date_col = amount_matching_row['Date']
        insurer_name = amount_matching_row['Insurer Name']
        invoice_no = amount_matching_row['Invoice No']
        narration_from_table_4 = amount_matching_row['Narration'] if 'Narration' in amount_matching_row and pd.notnull(amount_matching_row['Narration']) else amount_matching_row.get('Narration (Ref)', '')

        # Get 'GST' or 'GST @18%' presence in 'table_3' columns
        gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

        # Set 'NPT2' using 'Narration' from table_4
        processed_df['NPT2'] = narration_from_table_4

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Get 'supplier_name_col' from 'table_4'
        supplier_name_col = ''
        for col in ['Insurer Name', 'Insurer', 'SupplierName']:
            if col in table_4.columns and not table_4[col].empty:
                supplier_name_col = table_4[col].iloc[0]
                break

        # Create narration without the bracketed net amount
        if gst_present:
            narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
        else:
            narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'
        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for the new rows
        narration_value_float = float(str(narration_value_original).replace(',', ''))

        # Find 'TDS' or 'TDS @10%' column in table_3
        tds_column = None
        for col in table_3.columns:
            if 'TDS' in col or 'TDS @10%' in col:
                tds_column = col
                break

        if tds_column is not None:
            # Sum up the 'TDS' column
            tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
            third_new_row_brokerage = tds_values_numeric.sum()
            third_new_row_client_name = tds_column
        else:
            # Get second column available
            data_columns = table_3.columns.tolist()[1:]  # Exclude first column
            if len(data_columns) >= 1:
                second_column = data_columns[0]
                second_column_values_cleaned = table_3[second_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                second_column_values_numeric = pd.to_numeric(second_column_values_cleaned, errors='coerce').fillna(0)
                third_new_row_brokerage = second_column_values_numeric.sum()
                third_new_row_brokerage = - third_new_row_brokerage
                third_new_row_client_name = second_column
            else:
                third_new_row_brokerage = 0.0
                third_new_row_client_name = ''

        if gst_present:
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent

            # Create the additional rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no, invoice_no],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })
        else:
            # Create the additional row
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': [""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': ['G/L Account'],
                'Service Tax Ledger': [
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': processed_df['Income category'].iloc[-1],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_no],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Generate the shortened subject and date for the filename
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for Shriram Insurance Co
        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Shriram Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Shriram Insurance Co data: {str(e)}")
        raise

def process_kotak_mahindra_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                     table_3, table_4, table_5, subject, mappings):
    try:
        import numpy as np  # Import numpy for numerical comparisons
        import pandas as pd
        import os
        from datetime import datetime

        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            # If mappings are not provided, proceed without them
            pass

        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']

        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

        # For 'P & L JV' column, handle 'CANCELLATION' and 'ENDORSEMENTPOLICY'
        def determine_p_and_l_jv(row):
            row_lower = row.astype(str).str.lower()
            if row_lower.str.contains('cancellation').any():
                return 'CANCELLATION ENDORSEMENT'
            elif row_lower.str.contains('endorsementpolicy').any():
                return 'ENDORSEMENT'
            else:
                return ''

        processed_df['P & L JV'] = data.apply(determine_p_and_l_jv, axis=1)

        # For 'Branch' column, map existing 'Branch' column using 'state_lookups.xlsx' 'Sheet2'
        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet2'
            )
            state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
            processed_df['Branch'] = processed_df['Branch'].astype(str).str.strip().str.lower()
            branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # For 'Income category' column, map existing 'Income category' column using 'state_lookups.xlsx' 'Sheet4'
        if 'Income category' in processed_df.columns:
            state_lookups_sheet4 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet4'
            )
            state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
            state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
            processed_df['Income category'] = processed_df['Income category'].astype(str).str.strip().str.lower()
            income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
            processed_df['Income category'] = processed_df['Income category'].map(income_category_lookup).fillna('')
        else:
            processed_df['Income category'] = ''

        # Create necessary columns similar to 'ICICI Lombard'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df['Debtor Name'] if 'Debtor Name' in processed_df.columns else 'Kotak Mahindra General Insurance Co. Ltd.'
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''

        # Calculate 'Brokerage Rate', and format 'Premium' and 'Brokerage' as numbers rounded to two decimals
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']

        for column in ['Premium', 'Brokerage']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Net Amount' or the last column from 'table_3'
        net_amount_column = table_3.columns[-1]  # Assuming last column is 'Net Amount'
        net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
        net_amount_value = net_amount_values_numeric.sum()
        net_amount_value_formatted = "{:,.2f}".format(net_amount_value)  # Include commas

        # Check if sum_brokerage is approximately equal to net_amount_value
        brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        # Take the first available values for 'Insurer Name', 'Narration', etc.
        amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
        amount_total = amount_values_numeric.sum()
        narration_value_original = "{:,.2f}".format(amount_total)  # Use total amount with commas

        bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
        date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
        insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
        if 'Narration' in table_4.columns and not table_4['Narration'].empty:
            narration_from_table_4 = table_4['Narration'].iloc[0]
        elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
            narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
        else:
            narration_from_table_4 = ''
        invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''

        # Get 'GST' or 'GST @18%' presence in 'table_3' columns
        gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

        # Set 'NPT2' using 'Narration' from table_4
        processed_df['NPT2'] = narration_from_table_4

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Get 'supplier_name_col' from 'table_4'
        supplier_name_col = ''
        for col in ['Insurer Name', 'Insurer', 'SupplierName']:
            if col in table_4.columns and not table_4[col].empty:
                supplier_name_col = table_4[col].iloc[0]
                break

        # Create narration without the bracketed net amount
        if gst_present:
            narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
        else:
            narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'
        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for the new rows
        narration_value_float = float(str(narration_value_original).replace(',', ''))

        # Find 'TDS' or 'TDS @10%' column in table_3
        tds_column = None
        for col in table_3.columns:
            if 'TDS' in col or 'TDS @10%' in col:
                tds_column = col
                break

        if tds_column is not None:
            # Sum up the 'TDS' column
            tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
            third_new_row_brokerage = tds_values_numeric.sum()
            third_new_row_brokerage = - abs(third_new_row_brokerage)

            third_new_row_client_name = tds_column
        else:
            # Get second column available
            data_columns = table_3.columns.tolist()[1:]  # Exclude first column
            if len(data_columns) >= 1:
                second_column = data_columns[0]
                second_column_values_cleaned = table_3[second_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                second_column_values_numeric = pd.to_numeric(second_column_values_cleaned, errors='coerce').fillna(0)
                third_new_row_brokerage = second_column_values_numeric.sum()
                third_new_row_brokerage = - abs(third_new_row_brokerage)
                third_new_row_client_name = second_column
            else:
                third_new_row_brokerage = 0.0
                third_new_row_client_name = ''

        if gst_present:
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent

            # Create the additional rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': ['',''],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_nos, invoice_nos],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })
        else:
            # Create the additional row
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': [""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': ['G/L Account'],
                'Service Tax Ledger': [
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': [''],
                'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                'P & L JV': [invoice_nos],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for Kotak Mahindra Insurance
        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Kotak Mahindra Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Kotak Mahindra Insurance data: {str(e)}")
        raise

def process_universal_sampo_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                      table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            # If mappings are not provided, proceed without them
            pass

        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']

        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

        # Extract Endorsement No. from Policy No.
        if 'Policy No.' in processed_df.columns:
            def split_policy_no(policy_no):
                if pd.isnull(policy_no):
                    return policy_no, ''
                parts = str(policy_no).split('/')
                if len(parts) > 1:
                    endorsement_no = parts[-1]
                    if endorsement_no in ['0','00','000']:
                        endorsement_no = ''
                    policy_no_main = '/'.join(parts[:-1])
                else:
                    endorsement_no = ''
                    policy_no_main = policy_no
                return policy_no, endorsement_no
            policy_endorsement = processed_df['Policy No.'].apply(split_policy_no)
            processed_df['Policy No.'], processed_df['Endorsement No.'] = zip(*policy_endorsement)


        # For 'P & L JV' column, write 'Endorsement' if Endorsement No is not '000' and not empty
        def determine_p_and_l_jv(endorsement_no):
            if pd.isnull(endorsement_no) or endorsement_no == '' or endorsement_no == '000':
                return ''
            else:
                return 'Endorsement'
        processed_df['P & L JV'] = processed_df['Endorsement No.'].apply(determine_p_and_l_jv)

        # For 'Branch' column, map existing 'Branch' column using 'state_lookups.xlsx' 'Sheet2'
        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet2'
            )
            state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
            processed_df['Branch'] = processed_df['Branch'].astype(str).str.strip().str.lower()
            branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # For 'Income category' column, map existing 'Income category' column using 'state_lookups.xlsx' 'Sheet4'
        if 'Income category' in processed_df.columns:
            state_lookups_sheet4 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                sheet_name='Sheet4'
            )
            state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
            state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
            processed_df['Income category'] = processed_df['Income category'].astype(str).str.strip().str.lower()
            income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
            processed_df['Income category'] = processed_df['Income category'].map(income_category_lookup).fillna('')
        else:
            processed_df['Income category'] = ''

        # Create necessary columns similar to 'ICICI Lombard'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = processed_df['Debtor Name'] if 'Debtor Name' in processed_df.columns else 'Universal Sompo General Insurance Co. Ltd.'
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''

        # Calculate 'Brokerage Rate', and format 'Premium' and 'Brokerage' as numbers rounded to two decimals
        numeric_columns = ['Premium', 'Brokerage Rate', 'Brokerage']

        for column in ['Premium', 'Brokerage']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            else:
                processed_df[column] = 0.00

        # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                axis=1
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns to 2 decimal places and format
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Net Amount' or the last column from 'table_3'
        net_amount_column = table_3.columns[-1]  # Assuming last column is 'Net Amount'
        net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
        net_amount_value = net_amount_values_numeric.sum()
        net_amount_value_formatted = "{:,.2f}".format(net_amount_value)  # Include commas

        # Check if sum_brokerage is approximately equal to net_amount_value
        brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

        # Get 'Amount', 'Bank', 'Date', 'Insurer Name', 'Narration' from table_4.csv
        # Take the first available values for 'Insurer Name', 'Narration', etc.
        amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
        amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
        amount_total = amount_values_numeric.sum()
        narration_value_original = "{:,.2f}".format(amount_total)  # Use total amount with commas

        bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
        date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
        insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''

        # Handle 'Narration' or 'Narration (Ref)'
        if 'Narration' in table_4.columns and not table_4['Narration'].empty:
            narration_from_table_4 = table_4['Narration'].iloc[0]
        elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
            narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
        else:
            narration_from_table_4 = ''

        # Get 'Invoice No's and join them with commas
        invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''

        # Get 'GST' or 'GST @18%' presence in 'table_3' columns
        gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name' from table_4
        debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
        if not debtor_branch_ref_row.empty:
            debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
        else:
            debtor_branch_ref = ''

        # Set 'Debtor Branch Ref' in processed_df
        processed_df['Debtor Branch Ref'] = debtor_branch_ref

        # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
        processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Get 'supplier_name_col' from 'table_4'
        supplier_name_col = ''
        for col in ['Insurer Name', 'Insurer', 'SupplierName']:
            if col in table_4.columns and not table_4[col].empty:
                supplier_name_col = table_4[col].iloc[0]
                break

        # Create narration without the bracketed net amount
        if gst_present:
            if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
            else:
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
        else:
            if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
            else:
                narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger' as in other functions
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001'
        }
        bank_ledger_value = bank_value
        for key, value in bank_ledger_lookup.items():
            if bank_value == key:
                bank_ledger_value = value
                break
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name' (which is 'Insurer Name')
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for the new rows
        narration_value_float = float(str(narration_value_original).replace(',', ''))

        # Find 'TDS' or 'TDS @10%' column in table_3
        tds_column = None
        for col in table_3.columns:
            if 'TDS' in col or 'TDS @10%' in col:
                tds_column = col
                break

        if tds_column is not None:
            # Sum up the 'TDS' column
            tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
            third_new_row_brokerage = tds_values_numeric.sum()
            third_new_row_brokerage = -abs(third_new_row_brokerage)  # Ensure it's negative
            third_new_row_client_name = tds_column
        else:
            # Get second column available
            data_columns = table_3.columns.tolist()[1:]  # Exclude first column
            if len(data_columns) >= 1:
                second_column = data_columns[0]
                second_column_values_cleaned = table_3[second_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                second_column_values_numeric = pd.to_numeric(second_column_values_cleaned, errors='coerce').fillna(0)
                third_new_row_brokerage = second_column_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)  # Ensure it's negative
                third_new_row_client_name = second_column
            else:
                third_new_row_brokerage = 0.0
                third_new_row_client_name = ''

        if gst_present:
            gst_tds_18_percent = sum_brokerage * 0.18
            first_new_row_brokerage = gst_tds_18_percent

            # Create the additional rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': ['', ''],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_nos, invoice_nos],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })
        else:
            # Create the additional row
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': [""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': ['G/L Account'],
                'Service Tax Ledger': [
                    '2300022'  # Third new row remains '2300022'
                ],
                'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': [''],
                'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                'P & L JV': [invoice_nos],
                'NPT2': processed_df['NPT2'].iloc[-1]
            })


        # Ensure numeric columns are formatted properly
        for column in numeric_columns:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType', 'Debtor Branch Ref',
            'Client Name', 'Policy No.', 'Risk', 'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage', 'Narration', 'NPT', 'Bank Ledger',
            'AccountTypeDuplicate', 'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2'
        ]

        # Ensure all desired columns are present
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows in processed_df
        processed_df = processed_df.dropna(how='all', subset=processed_df.columns.difference(['Entry No.'])).reset_index(drop=True)

        # Update 'Entry No.' after removing empty rows
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate the shortened subject and date for the filename
        short_subject = subject[:50].strip().replace(' ', '_').replace('.', '').replace(':', '')  # Shorten to 50 characters
        short_narration = safe_narration[:50].strip().replace(' ', '_')  # Shorten to 50 characters
        date_str = datetime.now().strftime("%Y%m%d")  # Date only

        # Define output directories for Universal Sompo Insurance
        base_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Universal Sompo Insurance Template Files'
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{short_narration}_{date_str}.xlsx'
        csv_file_name = f'{short_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Universal Sompo Insurance data: {str(e)}")
        raise
def process_zuna_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                   table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Create a copy of the template_data
        processed_df = template_data.copy()

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                else:
                    processed_df[template_col] = ''
        else:
            pass  # Proceed without mappings if not provided

        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = (
                    processed_df[column]
                    .astype(str)
                    .str.replace(',', '')
                    .str.replace('(', '')
                    .str.replace(')', '')
                )
                processed_df[column] = pd.to_numeric(
                    processed_df[column], errors='coerce'
                ).fillna(0)

        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date', 'Policy End Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(
                    lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else ''
                )
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls

        # Update 'P & L JV' based on 'Endorsement No.'
        if 'Endorsement No.' in processed_df.columns:
            processed_df['Endorsement No.'] = processed_df['Endorsement No.'].astype(str).str.strip()
            processed_df['Endorsement No.'] = processed_df['Endorsement No.'].replace('nan', '')
            # Remove any .0 from numbers converted to strings
            processed_df['Endorsement No.'] = processed_df['Endorsement No.'].str.replace(r'\.0$', '', regex=True)
            # Set 'P & L JV' to 'Endorsement' if 'Endorsement No.' is not blank
            processed_df['P & L JV'] = processed_df.apply(
                lambda row: 'Endorsement' if row['Endorsement No.'] != '' else '', axis=1
            )
        else:
            processed_df['P & L JV'] = ''

        # Branch lookup
        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                r'\state_lookups.xlsx',
                sheet_name='Sheet2',
            )
            state_lookups_sheet2['state'] = (
                state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            )
            state_lookups_sheet2['shortform'] = (
                state_lookups_sheet2['shortform'].astype(str).str.strip()
            )
            processed_df['Branch'] = (
                processed_df['Branch'].astype(str).str.strip().str.lower()
            )
            branch_lookup = state_lookups_sheet2.set_index('state')[
                'shortform'
            ].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # Income category lookup
        if 'Income category' in processed_df.columns:
            state_lookups_sheet4 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                r'\state_lookups.xlsx',
                sheet_name='Sheet4',
            )
            state_lookups_sheet4['BUSINESS_TYPE'] = (
                state_lookups_sheet4['BUSINESS_TYPE']
                .astype(str)
                .str.strip()
                .str.lower()
            )
            state_lookups_sheet4['lookups'] = (
                state_lookups_sheet4['lookups'].astype(str).str.strip()
            )
            processed_df['Income category'] = (
                processed_df['Income category']
                .astype(str)
                .str.strip()
                .str.lower()
            )
            income_category_lookup = (
                state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
            )
            processed_df['Income category'] = processed_df['Income category'].map(
                income_category_lookup
            ).fillna('')
        else:
            processed_df['Income category'] = ''

        # Create necessary columns
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = (
            processed_df['Debtor Name']
            if 'Debtor Name' in processed_df.columns
            else 'Zuna General Insurance'
        )
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''

        # Calculate 'Brokerage Rate' and format numbers
        for column in ['Premium', 'Brokerage']:
            if column in processed_df.columns:
                processed_df[column] = (
                    processed_df[column]
                    .astype(str)
                    .str.replace(',', '')
                    .str.replace('(', '')
                    .str.replace(')', '')
                )
                processed_df[column] = pd.to_numeric(
                    processed_df[column], errors='coerce'
                ).fillna(0)
            else:
                processed_df[column] = 0.00

        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (
                    (float(row['Brokerage']) / float(row['Premium']) * 100)
                    if float(row['Premium']) != 0
                    else 0
                ),
                axis=1,
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

        # Round numeric columns
        for column in numeric_columns + ['Brokerage Rate']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(
                    lambda x: "{0:.2f}".format(x)
                )

        # Calculate sum of 'Brokerage'
        sum_brokerage = processed_df['Brokerage'].astype(float).sum()

        # Get 'Net Amount' from 'table_3'
        net_amount_column = table_3.columns[-1]  # Assuming last column is 'Net Amount'
        net_amount_values_cleaned = (
            table_3[net_amount_column]
            .astype(str)
            .str.replace(',', '')
            .str.replace('(', '')
            .str.replace(')', '')
        )
        net_amount_values_numeric = pd.to_numeric(
            net_amount_values_cleaned, errors='coerce'
        ).fillna(0)
        net_amount_value = net_amount_values_numeric.sum()
        net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

        # Check if sum_brokerage is approximately equal to net_amount_value
        brokerage_equals_net_amount = np.isclose(
            sum_brokerage, net_amount_value, atol=0.01
        )

        # Get details from 'table_4'
        amount_values_cleaned = (
            table_4['Amount']
            .astype(str)
            .str.replace(',', '')
            .str.replace('(', '')
            .str.replace(')', '')
        )
        amount_values_numeric = pd.to_numeric(
            amount_values_cleaned, errors='coerce'
        ).fillna(0)
        amount_total = amount_values_numeric.sum()
        narration_value_original = "{:,.2f}".format(amount_total)

        bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
        date_col = (
            table_4['Date'].iloc[0]
            if 'Date' in table_4.columns
            else datetime.today().strftime('%d/%m/%Y')
        )
        insurer_name = (
            table_4['Insurer Name'].iloc[0]
            if 'Insurer Name' in table_4.columns
            else ''
        )

        # Handle 'Narration' or 'Narration (Ref)'
        if 'Narration' in table_4.columns and not table_4['Narration'].empty:
            narration_from_table_4 = table_4['Narration'].iloc[0]
        elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
            narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
        else:
            narration_from_table_4 = ''

        # Get 'Invoice No's
        invoice_nos = (
            ', '.join(table_4['Invoice No'].dropna().astype(str).unique())
            if 'Invoice No' in table_4.columns
            else ''
        )

        # Check if 'GST' column exists in data
        gst_present = any(
            'GST' in col or 'GST @18%' in col for col in data.columns
        )

        # Remove special characters from 'Narration' for file naming
        safe_narration = ''.join(
            e for e in narration_from_table_4 if e.isalnum() or e == ' '
        ).strip()

        # Get 'Debtor Branch Ref' from 'cust_neft_data'
        debtor_branch_ref_row = cust_neft_data[
            cust_neft_data['Name'].str.lower() == insurer_name.lower()
        ]
        debtor_branch_ref = (
            debtor_branch_ref_row['No.2'].iloc[0]
            if not debtor_branch_ref_row.empty
            else ''
        )

        # Set 'Debtor Branch Ref' and 'Service Tax Ledger'
        processed_df['Debtor Branch Ref'] = debtor_branch_ref
        processed_df['Service Tax Ledger'] = processed_df[
            'Debtor Branch Ref'
        ].str.replace('CUST_NEFT_', '')

        # Set 'Debtor Name' as 'Insurer Name'
        processed_df['Debtor Name'] = insurer_name

        # Convert date to dd/mm/yyyy format
        date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

        # Get 'supplier_name_col' from 'table_4'
        supplier_name_col = ''
        for col in ['Insurer Name', 'Insurer', 'SupplierName']:
            if col in table_4.columns and not table_4[col].empty:
                supplier_name_col = table_4[col].iloc[0]
                break

        # Create narration with or without 'with GST'
        if gst_present:
            narration = (
                f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs."
                f"{narration_value_original} from {supplier_name_col} with GST 18%"
            )
        else:
            narration = (
                f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs."
                f"{narration_value_original} from {supplier_name_col} without GST 18%"
            )

        # Set 'Narration' in processed_df
        processed_df['Narration'] = narration

        # Map 'Bank Ledger'
        bank_ledger_lookup = {
            'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
            'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
            'HSBC': 'HSBC A/C-030-618375-001',
        }
        bank_ledger_value = bank_ledger_lookup.get(bank_value, bank_value)
        processed_df['Bank Ledger'] = bank_ledger_value

        # Set 'TDS Ledger' as 'Debtor Name'
        processed_df['TDS Ledger'] = processed_df['Debtor Name']

        # Calculate Brokerage values for new rows
        tds_column = None
        for col in data.columns:
            if 'TDS' in col or 'TDS @10%' in col:
                tds_column = col
                break

        if tds_column:
            tds_values_cleaned = (
                data[tds_column]
                .astype(str)
                .str.replace(',', '')
                .str.replace('(', '')
                .str.replace(')', '')
            )
            tds_values_numeric = pd.to_numeric(
                tds_values_cleaned, errors='coerce'
            ).fillna(0)
            third_new_row_brokerage = -abs(tds_values_numeric.sum())
        else:
            third_new_row_brokerage = 0.0

        if gst_present:
            gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
            first_new_row_brokerage = gst_amount

            # Create additional rows
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': ["", ""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': [
                    processed_df['AccountTypeDuplicate'].iloc[0],
                    'G/L Account',
                ],
                'Service Tax Ledger': [
                    processed_df['Service Tax Ledger'].iloc[0],
                    '2300022',
                ],
                'TDS Ledger': [
                    processed_df['TDS Ledger'].iloc[0],
                    'TDS Receivable - AY 2025-26',
                ],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': ['', ''],
                'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                'P & L JV': [invoice_nos, invoice_nos],
                'NPT2': processed_df['NPT2'].iloc[-1],
            })
        else:
            # Create additional row
            new_rows = pd.DataFrame({
                'Entry No.': '',
                'Debtor Name': processed_df['Debtor Name'].iloc[0],
                'Nature of Transaction': ["Brokerage Statement"],
                'AccountType': processed_df['AccountType'].iloc[0],
                'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                'Client Name': ["TDS Receivable - AY 2025-26"],
                'Policy No.': '',
                'Risk': '',
                'Endorsement No.': [""],
                'Policy Type': '',
                'Policy Start Date': '',
                'Policy End Date': '',
                'Premium': '0.00',
                'Brokerage Rate': '',
                'Brokerage': [third_new_row_brokerage],
                'Narration': narration,
                'NPT': '',
                'Bank Ledger': bank_ledger_value,
                'AccountTypeDuplicate': ['G/L Account'],
                'Service Tax Ledger': ['2300022'],
                'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                'RepDate': processed_df['RepDate'].iloc[-1],
                'Branch': '',
                'Income category': [''],
                'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                'P & L JV': [invoice_nos],
                'NPT2': processed_df['NPT2'].iloc[-1],
            })

        # Ensure numeric columns are formatted properly
        for column in numeric_columns + ['Brokerage Rate']:
            if column in new_rows.columns:
                new_rows[column] = pd.to_numeric(
                    new_rows[column], errors='coerce'
                ).fillna(0)
                new_rows[column] = new_rows[column].round(2)
                new_rows[column] = new_rows[column].apply(
                    lambda x: "{0:.2f}".format(x)
                )

        # Concatenate new_rows to processed_df
        processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

        # Update 'Entry No.'
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Rearranging columns to desired order
        desired_columns = [
            'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
            'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
            'Endorsement No.', 'Policy Type', 'Policy Start Date',
            'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
            'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
            'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
            'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
        ]
        for col in desired_columns:
            if col not in processed_df.columns:
                processed_df[col] = ''
        processed_df = processed_df[desired_columns]

        # Remove empty rows and update 'Entry No.'
        processed_df = processed_df.dropna(
            how='all',
            subset=processed_df.columns.difference(['Entry No.'])
        ).reset_index(drop=True)
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)

        # Generate filename components
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Zuna General Insurance'
            r' Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        processed_df.to_excel(excel_file_path, index=False)
        processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Zuna General Insurance data: {str(e)}")
        raise



def process_icici_prudential_life_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                            table_3, table_4, table_5, subject, mappings):
    try:
        print("Starting the processing of ICICI Prudential Life Insurance data...")

        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        print(f"Detected file extension: {file_extension}")

        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print("File read successfully.")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(f"Data after removing empty rows: {data.shape}")

        # Clean column names and data
        data.columns = data.columns.str.strip()
        data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        print("Cleaned column names and data.")

        # Create a copy of the template_data
        processed_df = template_data.copy()
        print("Template data copied.")

        # Process mappings from frontend (attachment columns on left, template columns on right)
        if mappings:
            print("Applying mappings...")
            for attachment_col, template_col in mappings.items():
                if attachment_col in data.columns:
                    processed_df[template_col] = data[attachment_col]
                    print(f"Mapped '{attachment_col}' to '{template_col}'.")
                else:
                    processed_df[template_col] = ''
                    print(f"Column '{attachment_col}' not found in data. Filled '{template_col}' with empty strings.")
        else:
            print("No mappings provided. Proceeding without mappings.")

        # Clean 'Client Name' field to remove extra spaces
        if 'Client Name' in processed_df.columns:
            processed_df['Client Name'] = processed_df['Client Name'].astype(str).str.strip()
            processed_df['Client Name'] = processed_df['Client Name'].str.replace(r'\s+', ' ', regex=True)
            print("Cleaned 'Client Name' field to remove extra spaces.")

        if 'Branch' in processed_df.columns:
            state_lookups_sheet2 = pd.read_excel(
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                r'\state_lookups.xlsx',
                sheet_name='Sheet2',
            )
            state_lookups_sheet2['state'] = (
                state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
            )
            state_lookups_sheet2['shortform'] = (
                state_lookups_sheet2['shortform'].astype(str).str.strip()
            )
            processed_df['Branch'] = (
                processed_df['Branch'].astype(str).str.strip().str.lower()
            )
            branch_lookup = state_lookups_sheet2.set_index('state')[
                'shortform'
            ].to_dict()
            processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
        else:
            processed_df['Branch'] = ''

        # Ensure numeric columns are handled correctly after mappings
        numeric_columns = ['Premium', 'Brokerage']
        for column in numeric_columns:
            if column in processed_df.columns:
                processed_df[column] = (
                    processed_df[column]
                    .astype(str)
                    .str.replace(',', '', regex=False)
                    .str.replace('(', '', regex=False)
                    .str.replace(')', '', regex=False)
                )
                processed_df[column] = pd.to_numeric(
                    processed_df[column], errors='coerce'
                ).fillna(0)
                print(f"Processed numeric column '{column}'.")
            else:
                processed_df[column] = 0.00
                print(f"Column '{column}' not found in processed_df. Filled with 0.00.")

        # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
        date_columns = ['Policy Start Date']
        for column in date_columns:
            if column in processed_df.columns and not processed_df[column].empty:
                processed_df[column] = processed_df[column].apply(parse_date_flexible)
                processed_df[column] = processed_df[column].apply(
                    lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else ''
                )
                processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls
                print(f"Processed date column '{column}'.")
            else:
                print(f"Column '{column}' not found or empty in processed_df.")

        # Since no lookups are needed for 'Income category' and 'P & L JV', we skip those steps
        print("Skipped lookups for 'Income category' and 'P & L JV'.")

        # Function to clean the subject
        def clean_subject(subject):
            # Remove prefixes like 'FW:', 'FWD:', 'RE:' at the beginning, case-insensitive
            subject = subject.strip()
            pattern = r'^(fw:|fwd:|re:)\s*'
            cleaned_subject = re.sub(pattern, '', subject, flags=re.IGNORECASE).strip()
            return cleaned_subject

        # Create necessary columns
        processed_df['Entry No.'] = range(1, len(processed_df) + 1)
        processed_df['Debtor Name'] = (
            processed_df['Debtor Name']
            if 'Debtor Name' in processed_df.columns
            else 'ICICI Prudential Life Insurance'
        )
        processed_df['AccountType'] = "Customer"
        processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
        processed_df['Nature of Transaction'] = "Brokerage Statement"
        processed_df['TDS Ledger'] = processed_df['Debtor Name']
        processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
        processed_df['NPT2'] = clean_subject(subject)
        processed_df['Debtor Branch Ref'] = ''
        processed_df['ASP Practice'] = ''
        processed_df['NPT'] = ''
        processed_df['Bank Ledger'] = ''
        processed_df['Service Tax Ledger'] = ''
        processed_df['Narration'] = ''
        processed_df['Policy Type'] = ''
        print("Created necessary columns.")

        # Calculate 'Brokerage Rate' and format numbers
        if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
            processed_df['Brokerage Rate'] = processed_df.apply(
                lambda row: (
                    (float(row['Brokerage']) / float(row['Premium']) * 100)
                    if float(row['Premium']) != 0
                    else 0
                ),
                axis=1,
            )
            processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)
            print("Calculated 'Brokerage Rate'.")
        else:
            processed_df['Brokerage Rate'] = 0.00
            print("Columns 'Premium' or 'Brokerage' not found in processed_df.")

        # Round numeric columns
        for column in numeric_columns + ['Brokerage Rate']:
            if column in processed_df.columns:
                processed_df[column] = processed_df[column].round(2)
                processed_df[column] = processed_df[column].apply(
                    lambda x: "{0:.2f}".format(x)
                )
                print(f"Rounded numeric column '{column}'.")
            else:
                print(f"Column '{column}' not found in processed_df during rounding.")

        # Get unique 'Policy End Date's
        if 'Policy End Date' in processed_df.columns:
            unique_end_dates = processed_df['Policy End Date'].unique()
            print(f"Found unique 'Policy End Date's: {unique_end_dates}")
        else:
            unique_end_dates = [None]
            print("Column 'Policy End Date' not found in processed_df.")

        # Prepare to store dataframes for each end date
        dataframes = []
        file_paths = []

        for idx, end_date in enumerate(unique_end_dates, start=1):
            print(f"Processing section {idx} for Policy End Date: {end_date}")

            # Filter processed_df for the current 'Policy End Date'
            if end_date:
                df_section = processed_df[processed_df['Policy End Date'] == end_date].copy()
            else:
                df_section = processed_df.copy()
            print(f"Section {idx} dataframe shape: {df_section.shape}")

            # Calculate sum of 'Brokerage' for this section
            sum_brokerage = df_section['Brokerage'].astype(float).sum()
            print(f"Sum of 'Brokerage' for section {idx}: {sum_brokerage}")

            # Get the brokerage value from 'table_3'
            if not table_3.empty:
                brokerage_column_name = table_3.columns[0]  # First available column
                net_column_name = table_3.columns[2] if len(table_3.columns) >= 3 else table_3.columns[-1]
                print(f"Using '{brokerage_column_name}' and '{net_column_name}' from 'table_3'.")

                table_3_brokerage_values = table_3[brokerage_column_name].astype(str).str.replace(',', '', regex=False).astype(float)
                table_3_net_values = table_3[net_column_name].astype(str).str.replace(',', '', regex=False).astype(float)

                # Find matching net value from 'table_3' corresponding to 'sum_brokerage'
                net_value = None
                for b_val, n_val in zip(table_3_brokerage_values, table_3_net_values):
                    if np.isclose(b_val, sum_brokerage, atol=0.01):
                        net_value = n_val
                        print(f"Found matching net value in 'table_3': {net_value}")
                        break

                if net_value is None:
                    net_value = table_3_net_values.iloc[0]
                    print(f"No matching net value found. Using first net value: {net_value}")
            else:
                net_value = 0.0
                print("Table 'table_3' is empty. Using net value: 0.0")

            # Compare net_value with 'Amount' column in 'table_4' to find matching row
            matching_row = None
            if 'Amount' in table_4.columns:
                amount_values_cleaned = (
                    table_4['Amount']
                    .astype(str)
                    .str.replace(',', '', regex=False)
                    .str.replace('(', '', regex=False)
                    .str.replace(')', '', regex=False)
                )
                amount_values_numeric = pd.to_numeric(
                    amount_values_cleaned, errors='coerce'
                ).fillna(0)

                for idx_table4, amount in enumerate(amount_values_numeric):
                    if np.isclose(amount, net_value, atol=0.01):
                        matching_row = table_4.iloc[idx_table4]
                        print(f"Found matching amount in 'table_4' at index {idx_table4}.")
                        break

            if matching_row is not None:
                # Extract necessary values from the matching row
                narration_from_table_4 = matching_row.get('Narration', '')
                invoice_nos = matching_row.get('Invoice No', '')
                bank_value = matching_row.get('Bank', '')
                date_col = matching_row.get('Date', datetime.today().strftime('%d/%m/%Y'))
                insurer_name = matching_row.get('Insurer Name', '')
                print(f"Extracted values from matching row in 'table_4'.")
            else:
                # If no matching amount found, use default or first row values
                narration_from_table_4 = table_4['Narration'].iloc[0] if 'Narration' in table_4.columns else ''
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
                date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
                insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
                print("No matching amount found in 'table_4'. Using default values.")

            # Remove extra spaces from 'Narration' for file naming
            safe_narration = ' '.join(narration_from_table_4.split())
            safe_narration = ''.join(
                e for e in safe_narration if e.isalnum() or e == ' '
            ).strip()
            print(f"Safe narration for file naming: '{safe_narration}'")

            # Get 'Debtor Branch Ref' from 'cust_neft_data'
            debtor_branch_ref_row = cust_neft_data[
                cust_neft_data['Name'].str.lower() == insurer_name.lower()
            ]
            debtor_branch_ref = (
                debtor_branch_ref_row['No.2'].iloc[0]
                if not debtor_branch_ref_row.empty
                else ''
            )
            print(f"Debtor Branch Ref: '{debtor_branch_ref}'")

            # Set 'Debtor Branch Ref' and 'Service Tax Ledger'
            df_section['Debtor Branch Ref'] = debtor_branch_ref
            df_section['Service Tax Ledger'] = df_section[
                'Debtor Branch Ref'
            ].str.replace('CUST_NEFT_', '', regex=False)

            # Set 'Debtor Name' as 'Insurer Name'
            df_section['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
            print(f"Formatted date: {date_col_formatted}")

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break
            print(f"Supplier name: '{supplier_name_col}'")

            # Check if 'GST' column exists in data
            gst_present = any(
                'GST' in col or 'GST @18%' in col for col in data.columns
            )
            print(f"GST present: {gst_present}")

            # Create narration with or without 'with GST'
            if gst_present:
                narration = (
                    f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs."
                    f"{net_value:.2f} from {supplier_name_col} with GST 18%"
                )
            else:
                narration = (
                    f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs."
                    f"{net_value:.2f} from {supplier_name_col} without GST 18%"
                )
            print(f"Narration set: '{narration}'")

            # Set 'Narration' in df_section
            df_section['Narration'] = narration

            # Map 'Bank Ledger'
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001',
            }
            bank_ledger_value = bank_ledger_lookup.get(bank_value, bank_value)
            df_section['Bank Ledger'] = bank_ledger_value
            print(f"Bank Ledger set to: '{bank_ledger_value}'")

            # Set 'TDS Ledger' as 'Debtor Name'
            df_section['TDS Ledger'] = df_section['Debtor Name']

            # Calculate Brokerage values for new rows
            tds_column = None
            for col in data.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break

            if tds_column:
                tds_values_cleaned = (
                    data[tds_column]
                    .astype(str)
                    .str.replace(',', '', regex=False)
                    .str.replace('(', '', regex=False)
                    .str.replace(')', '', regex=False)
                )
                tds_values_numeric = pd.to_numeric(
                    tds_values_cleaned, errors='coerce'
                ).fillna(0)
                third_new_row_brokerage = -abs(tds_values_numeric.sum())
                print(f"TDS value calculated: {third_new_row_brokerage}")
            else:
                third_new_row_brokerage = 0.0
                print("TDS column not found. Setting TDS value to 0.0.")

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                print(f"GST amount calculated: {first_new_row_brokerage}")

                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': df_section['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': df_section['AccountType'].iloc[0],
                    'Debtor Branch Ref': df_section['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [
                        df_section['AccountTypeDuplicate'].iloc[0],
                        'G/L Account',
                    ],
                    'Service Tax Ledger': [
                        df_section['Service Tax Ledger'].iloc[0],
                        '2300022',
                    ],
                    'TDS Ledger': [
                        df_section['TDS Ledger'].iloc[0],
                        'TDS Receivable - AY 2025-26',
                    ],
                    'RepDate': df_section['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': df_section['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': df_section['NPT2'].iloc[-1],
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': df_section['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': df_section['AccountType'].iloc[0],
                    'Debtor Branch Ref': df_section['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': df_section['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [df_section['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': df_section['NPT2'].iloc[-1],
                })
            print("Created additional rows.")

            # Ensure numeric columns are formatted properly
            for column in numeric_columns + ['Brokerage Rate']:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(
                        new_rows[column], errors='coerce'
                    ).fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(
                        lambda x: "{0:.2f}".format(x)
                    )
                    print(f"Formatted numeric column '{column}' in new rows.")

            # Concatenate new_rows to df_section
            df_section = pd.concat([df_section, new_rows], ignore_index=True)
            print(f"Concatenated new rows to section {idx} dataframe.")

            # Update 'Entry No.'
            df_section['Entry No.'] = range(1, len(df_section) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in df_section.columns:
                    df_section[col] = ''
            df_section = df_section[desired_columns]
            print(f"Rearranged columns for section {idx} dataframe.")

            # Remove empty rows and update 'Entry No.'
            df_section = df_section.dropna(
                how='all',
                subset=df_section.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            df_section['Entry No.'] = range(1, len(df_section) + 1)
            print(f"Cleaned up section {idx} dataframe.")

            # Generate filename components
            safe_narration_file = safe_narration.replace(' ', '_')[:50]
            date_str = datetime.now().strftime("%Y%m%d")
            section_file_name = f'{safe_narration_file}_section{idx}_{date_str}'
            print(f"Generated filename components for section {idx}: '{section_file_name}'")

            # Define output directories
            base_dir = (
                r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                r'\Common folder AP & AR\Brokerage Statement Automation\ICICI Prudential Life Insurance'
                r' Template Files'
            )
            excel_dir = os.path.join(base_dir, 'excel_file')
            csv_dir = os.path.join(base_dir, 'csv_file')

            # Ensure directories exist
            os.makedirs(excel_dir, exist_ok=True)
            os.makedirs(csv_dir, exist_ok=True)
            print(f"Ensured directories exist for section {idx}.")

            # Save the processed dataframe
            excel_file_name = f'{section_file_name}.xlsx'
            csv_file_name = f'{section_file_name}.csv'
            excel_file_path = os.path.join(excel_dir, excel_file_name)
            csv_file_path = os.path.join(csv_dir, csv_file_name)
            df_section.to_excel(excel_file_path, index=False)
            df_section.to_csv(csv_file_path, index=False)
            print(f"Saved Excel file for section {idx}: {excel_file_path}")
            print(f"Saved CSV file for section {idx}: {csv_file_path}")

            # Append dataframe and file path to lists
            dataframes.append(df_section)
            file_paths.append(excel_file_path)

        print("Completed processing all sections.")

        # Return the list of processed dataframes and the list of paths to the Excel files
        return dataframes[0], file_paths[0]

    except Exception as e:
        print(f"Error processing ICICI Prudential Life Insurance data: {str(e)}")
        raise



def process_cholamandalam_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                            table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")
        
        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        
        # ---- Updated Splitting Logic Starts Here ----
        # Handle repeating header rows and split data into sections
        
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison
        
        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)
        
        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----
        
        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        processed_df[template_col] = section[attachment_col]
                    else:
                        processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided
            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(r'[(),]', '', regex=True)
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain
            # In the 'Risk' column, remove all numbers
            if 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()
            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)
            # Round numeric columns to 2 decimal places and format
            for column in numeric_columns + ['Brokerage Rate']:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].round(2)
                    processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))
            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                    sheet_name='Sheet2'
                )
                state_lookups_sheet2['state'] = state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                state_lookups_sheet2['shortform'] = state_lookups_sheet2['shortform'].astype(str).str.strip()
                processed_df['Branch'] = processed_df['Branch'].astype(str).str.strip().str.lower()
                branch_lookup = state_lookups_sheet2.set_index('state')['shortform'].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                processed_df['Branch'] = ''
            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Cholamandalam General Insurance Co. Ltd.'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''
            # Process 'Endorsement No.' from 'Policy No.'
            if 'Policy No.' in processed_df.columns:
                def extract_endorsement_no(policy_no):
                    if isinstance(policy_no, str) and '/' in policy_no:
                        endorsement_no = policy_no.split('/')[-1][-2:]  # Get last 2 characters after last '/'
                        if endorsement_no == '00':
                            return ''
                        else:
                            return endorsement_no
                    else:
                        return ''
                processed_df['Endorsement No.'] = processed_df['Policy No.'].apply(extract_endorsement_no)
            else:
                processed_df['Endorsement No.'] = ''
            # Set 'P & L JV' based on 'Endorsement No.'
            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] == '' else 'Endorsement', axis=1
            )
            # Process 'Client Name' using 'name_lokup_chola.xlsx'
            if 'Client Name' in processed_df.columns:
                name_lookup_df = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files\name_lookup_chola.xlsx'
                )
                insurer_names = name_lookup_df['Client Names (insurer statement)'].astype(str).str.strip()
                marsh_names = name_lookup_df['Client Names (Marsh)'].astype(str).str.strip()
                name_lookup_dict = dict(zip(insurer_names, marsh_names))
                def replace_client_name(client_name):
                    for insurer_name in insurer_names:
                        if insurer_name.lower() in client_name.lower():
                            return name_lookup_dict[insurer_name]
                    return client_name  # If no match, keep original
                processed_df['Client Name'] = processed_df['Client Name'].astype(str)
                processed_df['Client Name'] = processed_df['Client Name'].apply(replace_client_name)
            else:
                processed_df['Client Name'] = ''
            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()
            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)
            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)
            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)
            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''
            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)
            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()
            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name
            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break
            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration
            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value
            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0
            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            # Ensure numeric columns are formatted properly
            for column in numeric_columns + ['Brokerage Rate']:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))
            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)
            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]
            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            # Append the processed_df to the list of processed sections
            processed_sections.append(processed_df)
        
        # Concatenate all processed sections
        final_processed_df = pd.concat(processed_sections, ignore_index=True)
        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")
        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Cholamandalam General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')
        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)
        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")
        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path
    except Exception as e:
        print(f"Error processing Cholamandalam General Insurance data: {str(e)}")
        raise


def process_liberty_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                      table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison
        
        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)
        
        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----
        
        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        processed_df[template_col] = section[attachment_col]
                    else:
                        processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided

            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(r'[(),]', '', regex=True)
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)


            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # In the 'Risk' column, remove all numbers
            if 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Round numeric columns to 2 decimal places and format
            for column in numeric_columns + ['Brokerage Rate']:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].round(2)
                    processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            # Branch lookup
            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                processed_df['Branch'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Liberty Videocon General Insurance Co. Ltd'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            if 'Policy No.' in processed_df.columns:
                # Fill NaN values with empty strings
                processed_df['Policy No.'] = processed_df['Policy No.'].fillna('').astype(str)
                # Only add the single quote if the policy number is not empty
                processed_df['Policy No.'] = processed_df['Policy No.'].apply(lambda x: "'" + x if x != '' else '')
            else:
                processed_df['Policy No.'] = ''



            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Ensure numeric columns are formatted properly
            for column in numeric_columns + ['Brokerage Rate']:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Liberty General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing liberty general insurance: {str(e)}")
        raise

def proess_acko_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                      table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=3)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=3)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=3)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=3)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=3)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=3)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison
        
        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)
        
        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----
        
        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        processed_df[template_col] = section[attachment_col]
                    else:
                        processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided

            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # In the 'Risk' column, remove all numbers
            if 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Round numeric columns to 2 decimal places and format
            for column in numeric_columns + ['Brokerage Rate']:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].round(2)
                    processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] == '' else 'Endorsement', axis=1
            )
        
            # Branch lookup
            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                processed_df['Branch'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Acko General Insurance Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Process 'Client Name' by adding a leading apostrophe

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Ensure numeric columns are formatted properly
            for column in numeric_columns + ['Brokerage Rate']:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\ACKO General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing acko general insurance: {str(e)}")
        raise
def process_sbi_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                      table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print("Initial Data:")
        print(data.head(10))
        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print("Data after removing empty rows:")
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison
        
        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)
        
        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----
        
        processed_sections = []
        for idx, section in enumerate(sections):
            print(f"Processing section {idx + 1}:")
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        processed_df[template_col] = section[attachment_col]
                    else:
                        processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided

            # Ensure 'Endorsement No.' is treated as string to avoid scientific notation
            if 'Endorsement No.' in processed_df.columns:
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].astype(str).str.strip()
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].replace('nan', '')
                # Remove any .0 from numbers converted to strings
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].str.replace(r'\.0$', '', regex=True)
                print("Processed 'Endorsement No.' column:")
                print(processed_df['Endorsement No.'])

            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # In the 'Risk' column, remove all numbers
            if 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Round numeric columns to 2 decimal places and format
            for column in numeric_columns + ['Brokerage Rate']:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].round(2)
                    processed_df[column] = processed_df[column].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''
        
            # Ensure 'Endorsement No.' is cleaned
            if 'Endorsement No.' in processed_df.columns:
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].fillna('').astype(str).str.strip()
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].replace('nan', '')
                # Remove any .0 from numbers converted to strings
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].str.replace(r'\.0$', '', regex=True)
            
            # Set 'P & L JV' based on 'Endorsement No.'
            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] == '' else 'Endorsement', axis=1
            )
            print("Processed 'P & L JV' column:")
            print(processed_df[['Endorsement No.', 'P & L JV']])

            # Branch lookup

            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                processed_df['Branch'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'SBI General Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Ensure numeric columns are formatted properly
            for column in numeric_columns + ['Brokerage Rate']:
                if column in new_rows.columns:
                    new_rows[column] = pd.to_numeric(new_rows[column], errors='coerce').fillna(0)
                    new_rows[column] = new_rows[column].round(2)
                    new_rows[column] = new_rows[column].apply(lambda x: "{0:.2f}".format(x))

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\SBI General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing sbi general insurance: {str(e)}")
        raise

def process_godigit_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                      table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        processed_df[template_col] = section[attachment_col]
                    else:
                        processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided

            # Correct handling of numeric columns without altering values
            numeric_columns = ['Premium', 'Brokerage']

            for column in numeric_columns:
                if column in processed_df.columns:
                    # Remove commas and parentheses, handle negative numbers
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '').str.strip()
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0.0)
                    # Do NOT round to two decimal places to preserve original values
                    # processed_df[column] = processed_df[column].round(2)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # In the 'Risk' column, remove all numbers
            if 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Only format 'Brokerage Rate' column
            if 'Brokerage Rate' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] in ['', 0, 'G01'] else 'Endorsement', axis=1
            )

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2' with 'Branch2' fallback
            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                # Map 'Branch' using the lookup
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup)

                # If 'Branch' is blank, and 'Branch2' exists, attempt to map 'Branch2'
                if 'Branch2' in processed_df.columns:
                    # Prepare 'Branch2' values
                    processed_df['Branch2_mapped'] = (
                        processed_df['Branch2'].astype(str).str.strip().str.lower().map(branch_lookup)
                    )
                    # Fill 'Branch' with 'Branch2_mapped' where 'Branch' is NaN or empty
                    processed_df['Branch'] = processed_df['Branch'].fillna(processed_df['Branch2_mapped']).fillna('')
                    # Drop the temporary 'Branch2_mapped' column
                    processed_df.drop(columns=['Branch2_mapped'], inplace=True)
                else:
                    processed_df['Branch'] = processed_df['Branch'].fillna('')
            else:
                processed_df['Branch'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'GoDigit General Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            # Convert narration_from_table_4 to string to avoid TypeError
            safe_narration = ''.join(e for e in str(narration_from_table_4) if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''

            processed_df['Debtor Branch Ref'] = debtor_branch_ref

            # 'Service Tax Ledger' is derived from 'Debtor Branch Ref'
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '', regex=False)

            # Set 'Debtor Name' as 'Insurer Name'
            processed_df['Debtor Name'] = insurer_name

            # Get 'SupplierName' and 'SupplierState' from 'table_5' matching 'TotalTaxAmt' closest to sum_brokerage
            table_5['TotalTaxAmt_cleaned'] = table_5['TotalTaxAmt'].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '').astype(float)
            table_5['Brokerage_Diff'] = abs(table_5['TotalTaxAmt_cleaned'] - sum_brokerage)
            matching_row_index = table_5['Brokerage_Diff'].idxmin()
            matching_row = table_5.loc[matching_row_index]
            supplier_state = matching_row['SupplierState'] if 'SupplierState' in matching_row and pd.notnull(matching_row['SupplierState']) else matching_row.get('MarshState', '')
            supplier_name_col = matching_row['SupplierName'] if 'SupplierName' in matching_row and pd.notnull(matching_row['SupplierName']) else matching_row.get('Insurer', '')

            # Read 'Chart of Account' file
            chart_of_account_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\Chart of Account.xlsx'
            chart_of_account = pd.read_excel(chart_of_account_path)
            chart_of_account['SupplierState'] = chart_of_account['SupplierState'].astype(str).str.strip()

            # Get 'Name-AY 2025-26' and 'Gl No' based on 'SupplierState'
            chart_matching_rows = chart_of_account[chart_of_account['SupplierState'] == supplier_state]
            if not chart_matching_rows.empty:
                name_ay_2025_26 = chart_matching_rows['Name-AY 2025-26'].iloc[0]
                gl_no = chart_matching_rows['Gl No'].iloc[0]
            else:
                name_ay_2025_26 = ''
                gl_no = ''

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Ensure 'Brokerage Rate' is properly formatted in new rows
            if 'Brokerage Rate' in new_rows.columns:
                new_rows['Brokerage Rate'] = pd.to_numeric(new_rows['Brokerage Rate'], errors='coerce').fillna(0)
                new_rows['Brokerage Rate'] = new_rows['Brokerage Rate'].round(2)
                new_rows['Brokerage Rate'] = new_rows['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove rows where both 'Premium' and 'Brokerage' are 0
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df = processed_df[~((processed_df['Premium'].astype(float) == 0) & (processed_df['Brokerage'].astype(float) == 0))].reset_index(drop=True)

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\GoDigit General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing GoDigit general insurance: {str(e)}")
        raise

def process_raheja_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                     table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []

        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_cols in mappings.items():
                    if not isinstance(template_cols, list):
                        template_cols = [template_cols]
                    if attachment_col in section.columns:
                        for template_col in template_cols:
                            processed_df[template_col] = section[attachment_col]
                    else:
                        for template_col in template_cols:
                            processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided

            processed_df['Income category'] = processed_df['P & L JV']

            # Process 'Risk' column by combining 'Risk1', 'Risk2', 'Risk3' into 'Risk' as per logic
            if 'Risk1' in processed_df.columns and 'Risk2' in processed_df.columns and 'Risk3' in processed_df.columns:
                def get_risk(row):
                    if pd.notnull(row['Risk1']) and str(row['Risk1']).strip() != '':
                        return row['Risk1']
                    elif pd.notnull(row['Risk2']) and str(row['Risk2']).strip() != '':
                        return row['Risk2']
                    elif pd.notnull(row['Risk3']) and str(row['Risk3']).strip() != '':
                        return row['Risk3']
                    else:
                        return ''
                processed_df['Risk'] = processed_df.apply(get_risk, axis=1)
                # Remove 'Risk1', 'Risk2', 'Risk3' columns from processed_df
                processed_df.drop(columns=['Risk1', 'Risk2', 'Risk3'], inplace=True)
            else:
                # If any of the 'Risk' columns are missing, set 'Risk' column to ''
                processed_df['Risk'] = ''

            # Process 'Client Name' column by concatenating 'Client Name' and 'Client Name2' when 'Client Name2' is not blank
            if 'Client Name' in processed_df.columns and 'Client Name2' in processed_df.columns:
                def get_client_name(row):
                    if pd.notnull(row['Client Name2']) and str(row['Client Name2']).strip() != '':
                        return str(row['Client Name']) + ' ' + str(row['Client Name2'])
                    else:
                        return str(row['Client Name'])
                processed_df['Client Name'] = processed_df.apply(get_client_name, axis=1)
                # Remove 'Client Name2' column from processed_df
                processed_df.drop(columns=['Client Name2'], inplace=True)
            else:
                pass

            # Remove rows where both 'Premium' and 'Brokerage' are 0
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df[['Premium', 'Brokerage']] = processed_df[['Premium', 'Brokerage']].apply(pd.to_numeric, errors='coerce').fillna(0)
                processed_df = processed_df[~((processed_df['Premium'] == 0) & (processed_df['Brokerage'] == 0))]
            else:
                pass

            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '').str.strip()
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # In the 'Risk' column, remove all numbers
            if 'Risk' in processed_df.columns:
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.replace(r'\d+', '', regex=True).str.strip()

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Only format 'Brokerage Rate' column
            if 'Brokerage Rate' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] in ['', 1] else 'Endorsement' if row['P & L JV'] == '' else row['P & L JV'], axis=1
            )

            # Process 'Endorsement No.' and 'P & L JV' columns
            if 'Endorsement No.' in processed_df.columns:
                def process_endorsement(row):
                    endorsement_no = str(row['Endorsement No.']).strip()
                    p_l_jv = str(row.get('P & L JV', '')).strip()
                    if endorsement_no == '0':
                        row['Endorsement No.'] = ''
                    elif endorsement_no == '1':
                        row['Endorsement No.'] = '1'
                        row['P & L JV'] = ''
                    else:
                        if row['Endorsement No.'] != '':
                            row['P & L JV'] = 'Endorsement'
                    return row
                processed_df = processed_df.apply(process_endorsement, axis=1)
            else:
                pass

            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                processed_df['Branch'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\state_lookups.xlsx',
                    sheet_name='Sheet4'
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = state_lookups_sheet4['BUSINESS_TYPE'].astype(str).str.strip().str.lower()
                state_lookups_sheet4['lookups'] = state_lookups_sheet4['lookups'].astype(str).str.strip()
                processed_df['Income category'] = processed_df['Income category'].astype(str).str.strip().str.lower()
                income_category_lookup = state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                processed_df['Income category'] = processed_df['Income category'].map(income_category_lookup).fillna('')
                print(r'this works!')
            else:
                processed_df['Income category'] = ''
                print(r'why this is else')

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Raheja Qbe General Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [first_new_row_brokerage, third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': ['', ''],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [third_new_row_brokerage],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [''],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Only format 'Brokerage Rate' column in new rows
            if 'Brokerage Rate' in new_rows.columns:
                new_rows['Brokerage Rate'] = pd.to_numeric(new_rows['Brokerage Rate'], errors='coerce').fillna(0)
                new_rows['Brokerage Rate'] = new_rows['Brokerage Rate'].round(2)
                new_rows['Brokerage Rate'] = new_rows['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Raheja General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing raheja general insurance: {str(e)}")
        raise





def process_royal_sundaram_general_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                             table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))
        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Create a copy of the template_data
            processed_df = template_data.copy()
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        processed_df[template_col] = section[attachment_col]
                    else:
                        processed_df[template_col] = ''
            else:
                pass  # Proceed without mappings if not provided

            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    # Remove commas and parentheses without altering the original values
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '').str.strip()
                    # Convert to numeric without rounding
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df['Brokerage Rate'] = processed_df.apply(
                    lambda row: (float(row['Brokerage']) / float(row['Premium']) * 100) if float(row['Premium']) != 0 else 0,
                    axis=1
                )
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].round(2)

            # Format numeric columns to string with two decimal places without rounding off the original values
            for column in numeric_columns + ['Brokerage Rate']:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].apply(lambda x: f"{x:.2f}")

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] == '' else 'Endorsement', axis=1
            )
            # Branch lookup

            if 'Endorsement No.' in processed_df.columns:
                def process_endorsement(row):
                    endorsement_no = str(row['Endorsement No.']).strip()
                    p_l_jv = str(row.get('P & L JV', '')).strip()
                    if endorsement_no == '0':
                        row['Endorsement No.'] = ''
                    elif endorsement_no == '1':
                        row['Endorsement No.'] = '1'
                        row['P & L JV'] = ''
                    else:
                        if row['Endorsement No.'] != '':
                            row['P & L JV'] = 'Endorsement'
                    return row

                processed_df = processed_df.apply(process_endorsement, axis=1)
            else:
                pass

            if 'Branch' in processed_df.columns:
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                processed_df['Branch'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Royal Sundaram General Insurance Co Ltd'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage' without rounding
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                third_new_row_brokerage = tds_values_numeric.sum()
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Ensure numeric columns are formatted properly without altering values
            for column in numeric_columns + ['Brokerage Rate']:
                if column in new_rows.columns:
                    new_rows[column] = new_rows[column].astype(str).apply(lambda x: x.strip())
                    new_rows[column] = new_rows[column].apply(lambda x: f"{float(x):.2f}" if x != '' else '')

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Royal Sundaram Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Royal Sundaram general insurance: {str(e)}")
        raise
def process_tata_aig_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                               table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))
        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        mapped_df[template_col] = section[attachment_col]
                    else:
                        mapped_df[template_col] = ''
            else:
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # ---- Data Expansion Logic Starts Here ----
            # Create separate DataFrames for Brokerage1/Premium1, Brokerage2/Premium2, Brokerage3/Premium3
            df_list = []
            # List to maintain the order and insert blank rows
            dataframes = []

            # Helper function to set 'ASP Practice' based on mapping names
            def get_asp_practice(brokerage_num):
                attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{brokerage_num}'), '')
                if 'TP' in attachment_col.upper():
                    return 'Motor TP'
                elif 'TERR' in attachment_col.upper():
                    return 'Terrorism'
                else:
                    return ''

            for i in range(1, 4):
                brokerage_col = f'Brokerage{i}'
                premium_col = f'Premium{i}'
                if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                    df = mapped_df.copy()
                    df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                    if not df.empty:
                        df['Brokerage'] = df[brokerage_col]
                        df['Premium'] = df[premium_col]
                        # Set 'ASP Practice'
                        if i == 1:
                            df['ASP Practice'] = 'Normal'
                        else:
                            asp_practice_value = get_asp_practice(i)
                            df['ASP Practice'] = asp_practice_value
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                        df = df.drop(columns=cols_to_drop, errors='ignore')
                        df_list.append(df)
            # Arrange the DataFrames and insert blank rows
            for idx_df, df in enumerate(df_list):
                if idx_df > 0:
                    # Insert a blank row
                    blank_row = pd.DataFrame(columns=df.columns)
                    dataframes.append(blank_row)
                dataframes.append(df)

            # Concatenate all DataFrames
            if dataframes:
                processed_df = pd.concat(dataframes, ignore_index=True)
            else:
                # If no dataframes were created, use the mapped_df as is
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '').replace(')', ''))
                        premium = float(str(row['Premium']).replace(',', '').replace('(', '').replace(')', ''))
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] == '' else 'Endorsement', axis=1
            )
            # Branch lookup

            if 'Endorsement No.' in processed_df.columns:
                def process_endorsement(row):
                    endorsement_no = str(row['Endorsement No.']).strip()
                    p_l_jv = str(row.get('P & L JV', '')).strip()
                    if endorsement_no == '0':
                        row['Endorsement No.'] = ''
                        row['P & L JV'] = ''
                    else:
                        if row['Endorsement No.'] != '':
                            row['P & L JV'] = 'Endorsement'
                        else:
                            row['P & L JV'] = ''
                    return row

                processed_df = processed_df.apply(process_endorsement, axis=1)
            else:
                pass

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Tata AIG General Insurance Co. Ltd.'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = processed_df.get('ASP Practice', '')
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(str).apply(
                lambda x: float(str(x).replace(',', '').replace('(', '').replace(')', '')) if x != '' else 0.0
            ).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            # Modification: Take the first value instead of sum
            if len(net_amount_values_numeric) > 0:
                net_amount_value = net_amount_values_numeric.iloc[0]
            else:
                net_amount_value = 0.0
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                # Modification: Take the first value instead of sum
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Tata AIG Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Tata AIG general insurance: {str(e)}")
        raise

def process_bajaj_allianz_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                    table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Remove rows where any column contains 'Grand Total'
        data = data[~data.apply(lambda row: row.astype(str).str.contains('Grand Total', case=False, na=False).any(), axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        mapped_df[template_col] = section[attachment_col]
                    else:
                        mapped_df[template_col] = ''
            else:
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # ---- Data Expansion Logic Starts Here ----
            # Create separate DataFrames for Brokerage1/Premium1, Brokerage2/Premium2, Brokerage3/Premium3
            df_list = []
            # List to maintain the order and insert blank rows
            dataframes = []

            # Helper function to set 'ASP Practice' based on mapping names
            def get_asp_practice(brokerage_num):
                attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{brokerage_num}'), '')
                if 'TP' in attachment_col.upper():
                    return 'Motor TP'
                elif 'TERR' in attachment_col.upper():
                    return 'Terrorism'
                else:
                    return ''

            for i in range(1, 4):
                brokerage_col = f'Brokerage{i}'
                premium_col = f'Premium{i}'
                if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                    df = mapped_df.copy()
                    df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                    if not df.empty:
                        df['Brokerage'] = df[brokerage_col]
                        df['Premium'] = df[premium_col]
                        # Set 'ASP Practice'
                        if i == 1:
                            df['ASP Practice'] = 'Normal'
                        else:
                            asp_practice_value = get_asp_practice(i)
                            df['ASP Practice'] = asp_practice_value
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                        df = df.drop(columns=cols_to_drop, errors='ignore')
                        df_list.append(df)
            # Arrange the DataFrames and insert blank rows
            for idx_df, df in enumerate(df_list):
                if idx_df > 0:
                    # Insert a blank row
                    blank_row = pd.DataFrame(columns=df.columns)
                    dataframes.append(blank_row)
                dataframes.append(df)

            # Concatenate all DataFrames
            if dataframes:
                processed_df = pd.concat(dataframes, ignore_index=True)
            else:
                # If no dataframes were created, use the mapped_df as is
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            processed_df['Income category'] = processed_df['P & L JV']

            # ---- New Logic Starts Here ----
            # If the 'Risk' column has only numbers
            if processed_df['Risk'].apply(lambda x: str(x).strip().isdigit()).all():
                # Open 'Risk code.xlsx' from support files folder, open 'Sheet1'
                risk_code_path = (
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files\Risk code.xlsx'
                )
                risk_code_df = pd.read_excel(risk_code_path, sheet_name='Sheet1')
                # Clean column names
                risk_code_df.columns = risk_code_df.columns.str.strip()
                # Ensure 'Risk' and 'PRODUCT_4DIGIT_CODE' are strings
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.strip()
                risk_code_df['PRODUCT_4DIGIT_CODE'] = risk_code_df['PRODUCT_4DIGIT_CODE'].astype(str).str.strip()
                # Merge
                processed_df = processed_df.merge(
                    risk_code_df[['PRODUCT_4DIGIT_CODE', 'PRODUCT_NAME']],
                    how='left',
                    left_on='Risk',
                    right_on='PRODUCT_4DIGIT_CODE'
                )
                # Update 'Risk' column with 'PRODUCT_NAME' where match found
                processed_df['Risk'] = processed_df['PRODUCT_NAME'].fillna(processed_df['Risk'])
                # Drop the extra columns
                processed_df = processed_df.drop(columns=['PRODUCT_4DIGIT_CODE', 'PRODUCT_NAME'])
            # ---- New Logic Ends Here ----

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '').replace(')', ''))
                        premium = float(str(row['Premium']).replace(',', '').replace('(', '').replace(')', ''))
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            # Update 'Endorsement No.' and 'P & L JV' based on 'Policy No.'
            if 'Endorsement No.' in processed_df.columns:
                def process_endorsement(row):
                    policy_no = str(row['Policy No.']).strip()
                    if '-' in policy_no:
                        parts = policy_no.split('-')
                        last_part = parts[-1]
                        if last_part.startswith('E'):
                            row['Endorsement No.'] = last_part
                            row['P & L JV'] = 'Endorsement'
                        else:
                            row['Endorsement No.'] = ''
                            row['P & L JV'] = ''
                    else:
                        row['Endorsement No.'] = ''
                        row['P & L JV'] = ''
                    return row
                processed_df = processed_df.apply(process_endorsement, axis=1)
            else:
                pass

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Bajaj Allianz General Insurance Co. Ltd.'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = processed_df.get('ASP Practice', '')
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(str).apply(
                lambda x: float(str(x).replace(',', '').replace('(', '').replace(')', '')) if x != '' else 0.0
            ).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            # Modification: Take the first value instead of sum
            if len(net_amount_values_numeric) > 0:
                net_amount_value = net_amount_values_numeric.iloc[0]
            else:
                net_amount_value = 0.0
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''

            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')

            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                # Modification: Take the first value instead of sum
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Bajaj Allianz Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Bajaj Allianz general insurance: {str(e)}")
        raise



def process_hdfc_ergo_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Remove rows where any column contains 'Grand Total'
        data = data[~data.apply(lambda row: row.astype(str).str.contains('Grand Total', case=False, na=False).any(), axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        mapped_df[template_col] = section[attachment_col]
                    else:
                        mapped_df[template_col] = ''
            else:
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # ---- Data Expansion Logic Starts Here ----
            # Create separate DataFrames for Brokerage1/Premium1, Brokerage2/Premium2, Brokerage3/Premium3
            df_list = []
            # List to maintain the order and insert blank rows
            dataframes = []

            # Helper function to set 'ASP Practice' based on mapping names
            def get_asp_practice(brokerage_num):
                attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{brokerage_num}'), '')
                if 'TP' in attachment_col.upper():
                    return 'Motor TP'
                elif 'TERR' in attachment_col.upper():
                    return 'Terrorism'
                else:
                    return ''

            for i in range(1, 4):
                brokerage_col = f'Brokerage{i}'
                premium_col = f'Premium{i}'
                if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                    df = mapped_df.copy()
                    df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                    if not df.empty:
                        df['Brokerage'] = df[brokerage_col]
                        df['Premium'] = df[premium_col]
                        # Set 'ASP Practice'
                        if i == 1:
                            df['ASP Practice'] = 'Normal'
                        else:
                            asp_practice_value = get_asp_practice(i)
                            df['ASP Practice'] = asp_practice_value
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                        df = df.drop(columns=cols_to_drop, errors='ignore')
                        df_list.append(df)
            # Arrange the DataFrames and insert blank rows
            for idx_df, df in enumerate(df_list):
                if idx_df > 0:
                    # Insert a blank row
                    blank_row = pd.DataFrame(columns=df.columns)
                    dataframes.append(blank_row)
                dataframes.append(df)

            # Concatenate all DataFrames
            if dataframes:
                processed_df = pd.concat(dataframes, ignore_index=True)
            else:
                # If no dataframes were created, use the mapped_df as is
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            # Replace blank values in 'Premium' column with 'No value found'
            if 'Premium' in processed_df.columns:
                processed_df['Premium'] = processed_df['Premium'].apply(lambda x: 'No value found' if pd.isnull(x) or x == '' else x)

            processed_df['Income category'] = processed_df['P & L JV']

            # ---- New Logic Starts Here ----
            # If the 'Risk' column has only numbers
            if processed_df['Risk'].apply(lambda x: str(x).strip().isdigit()).all():
                # Open 'Risk code.xlsx' from support files folder, open 'Sheet1'
                risk_code_path = (
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files\Risk code.xlsx'
                )
                risk_code_df = pd.read_excel(risk_code_path, sheet_name='Sheet1')
                # Clean column names
                risk_code_df.columns = risk_code_df.columns.str.strip()
                # Ensure 'Risk' and 'PRODUCT_4DIGIT_CODE' are strings
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.strip()
                risk_code_df['PRODUCT_4DIGIT_CODE'] = risk_code_df['PRODUCT_4DIGIT_CODE'].astype(str).str.strip()
                # Merge
                processed_df = processed_df.merge(
                    risk_code_df[['PRODUCT_4DIGIT_CODE', 'PRODUCT_NAME']],
                    how='left',
                    left_on='Risk',
                    right_on='PRODUCT_4DIGIT_CODE'
                )
                # Update 'Risk' column with 'PRODUCT_NAME' where match found
                processed_df['Risk'] = processed_df['PRODUCT_NAME'].fillna(processed_df['Risk'])
                # Drop the extra columns
                processed_df = processed_df.drop(columns=['PRODUCT_4DIGIT_CODE', 'PRODUCT_NAME'])
            # ---- New Logic Ends Here ----

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '').replace(')', ''))
                        premium = float(str(row['Premium']).replace(',', '').replace('(', '').replace(')', ''))
                        if isinstance(premium, str) and premium.lower() == 'no value found':
                            return 0
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            processed_df['P & L JV'] = processed_df.apply(
                lambda row: '' if row['Endorsement No.'] == '' else 'Endorsement', axis=1
            )
            # Branch lookup

            if 'Endorsement No.' in processed_df.columns:
                def process_endorsement(row):
                    endorsement_no = str(row['Endorsement No.']).strip()
                    p_l_jv = str(row.get('P & L JV', '')).strip()
                    if endorsement_no == '0':
                        row['Endorsement No.'] = ''
                        row['P & L JV'] = ''
                    else:
                        if row['Endorsement No.'] != '':
                            row['P & L JV'] = 'Endorsement'
                        else:
                            row['P & L JV'] = ''
                    return row

                processed_df = processed_df.apply(process_endorsement, axis=1)
            else:
                pass

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Hdfc Ergo General Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = processed_df.get('ASP Practice', '')
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(str).apply(
                lambda x: float(str(x).replace(',', '').replace('(', '').replace(')', '')) if x != '' else 0.0
            ).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            # Modification: Take the first value instead of sum
            if len(net_amount_values_numeric) > 0:
                net_amount_value = net_amount_values_numeric.iloc[0]
            else:
                net_amount_value = 0.0
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''

            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')

            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                # Modification: Take the first value instead of sum
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Hdfc Ergo Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Hdfc Ergo insurance: {str(e)}")
        raise

def process_relaince_general_insurance_co(file_path, template_data, risk_code_data, cust_neft_data,
                                         table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Remove rows where any column contains 'Grand Total'
        data = data[~data.apply(lambda row: row.astype(str).str.contains('Grand Total', case=False, na=False).any(), axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        mapped_df[template_col] = section[attachment_col]
                    else:
                        mapped_df[template_col] = ''
            else:
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # ---- Data Expansion Logic Starts Here ----
            # Create separate DataFrames for Brokerage1/Premium1, Brokerage2/Premium2, Brokerage3/Premium3
            df_list = []
            # List to maintain the order and insert blank rows
            dataframes = []

            # Helper function to set 'ASP Practice' based on mapping names
            def get_asp_practice(brokerage_num):
                attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{brokerage_num}'), '')
                if 'TP' in attachment_col.upper():
                    return 'Motor TP'
                elif 'TERR' in attachment_col.upper():
                    return 'Terrorism'
                else:
                    return ''

            for i in range(1, 4):
                brokerage_col = f'Brokerage{i}'
                premium_col = f'Premium{i}'
                if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                    df = mapped_df.copy()
                    df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                    if not df.empty:
                        df['Brokerage'] = df[brokerage_col]
                        df['Premium'] = df[premium_col]
                        # Set 'ASP Practice'
                        if i == 1:
                            df['ASP Practice'] = 'Normal'
                        else:
                            asp_practice_value = get_asp_practice(i)
                            df['ASP Practice'] = asp_practice_value
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                        df = df.drop(columns=cols_to_drop, errors='ignore')
                        df_list.append(df)
            # Arrange the DataFrames and insert blank rows
            for idx_df, df in enumerate(df_list):
                if idx_df > 0:
                    # Insert a blank row
                    blank_row = pd.DataFrame(columns=df.columns)
                    dataframes.append(blank_row)
                dataframes.append(df)

            # Concatenate all DataFrames
            if dataframes:
                processed_df = pd.concat(dataframes, ignore_index=True)
            else:
                # If no dataframes were created, use the mapped_df as is
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            # Replace blank values in 'Premium' column with 'No value found'
            if 'Premium' in processed_df.columns:
                processed_df['Premium'] = processed_df['Premium'].apply(lambda x: 'No value found' if pd.isnull(x) or x == '' else x)

            # ---- Move 'P & L JV' assignment after 'Endorsement No.' processing ----

            # ---- Policy No and Endorsement No Processing Starts Here ----
            # Replace single quotes in 'Policy No' and 'Endorsement No'
            for col in ['Policy No.', 'Endorsement No.']:
                if col in processed_df.columns:
                    processed_df[col] = processed_df[col].astype(str).str.replace("’", "'", regex=False).str.replace("‘", "'", regex=False)

            # Make 'Endorsement No.' blank if it contains '00' or '0' and set 'P & L JV' accordingly
            if 'Endorsement No.' in processed_df.columns:
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].apply(
                    lambda x: '' if str(x).strip() in ['00', '0', '0.00', '0.0'] else x
                )
                # Set 'P & L JV' based on 'Endorsement No.'
                processed_df['P & L JV'] = processed_df['Endorsement No.'].apply(
                    lambda x: 'Endorsement' if pd.notna(x) and str(x).strip() != '' else ''
                )
            else:
                processed_df['P & L JV'] = ''

            # ---- Policy No and Endorsement No Processing Ends Here ----

            # Remove empty rows and reset index if necessary
            processed_df = processed_df.dropna(how='all').reset_index(drop=True)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '').replace(')', ''))
                        premium_val = str(row['Premium']).replace(',', '').replace('(', '').replace(')', '')
                        if premium_val.lower() == 'no value found':
                            return 0
                        premium = float(premium_val)
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Reliance General Insurance Co. Ltd.'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = processed_df.get('ASP Practice', '')
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(str).apply(
                lambda x: float(str(x).replace(',', '').replace('(', '').replace(')', '')) if x != '' else 0.0
            ).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            # Modification: Take the first value instead of sum
            if len(net_amount_values_numeric) > 0:
                net_amount_value = net_amount_values_numeric.iloc[0]
            else:
                net_amount_value = 0.0
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''

            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')

            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            try:
                date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
            except:
                date_col_formatted = ''

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                # Modification: Take the first value instead of sum
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': [''] * 2,
                    'Debtor Name': [processed_df['Debtor Name'].iloc[0]] * 2,
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': [processed_df['AccountType'].iloc[0]] * 2,
                    'Debtor Branch Ref': [processed_df['Debtor Branch Ref'].iloc[0]] * 2,
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': [''] * 2,
                    'Risk': [''] * 2,
                    'Endorsement No.': ["", ""],
                    'Policy Type': [''] * 2,
                    'Policy Start Date': [''] * 2,
                    'Policy End Date': [''] * 2,
                    'Premium': ['0.00'] * 2,
                    'Brokerage Rate': ['', ''],
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': [narration, narration],
                    'NPT': ['', ''],
                    'Bank Ledger': [bank_ledger_value] * 2,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'] * 2,
                    'RepDate': [processed_df['RepDate'].iloc[-1]] * 2,
                    'Branch': ['', ''],
                    'Income category': ['', ''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]] * 2,
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': [processed_df['NPT2'].iloc[-1]] * 2
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': [''],
                    'Debtor Name': [processed_df['Debtor Name'].iloc[0]],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': [processed_df['AccountType'].iloc[0]],
                    'Debtor Branch Ref': [processed_df['Debtor Branch Ref'].iloc[0]],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': [''],
                    'Risk': [''],
                    'Endorsement No.': [""],
                    'Policy Type': [''],
                    'Policy Start Date': [''],
                    'Policy End Date': [''],
                    'Premium': ['0.00'],
                    'Brokerage Rate': [''],
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': [narration],
                    'NPT': [''],
                    'Bank Ledger': [bank_ledger_value],
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': [processed_df['RepDate'].iloc[-1]],
                    'Branch': [''],
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': [processed_df['NPT2'].iloc[-1]]
                })

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Relaince General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Relaince General insurance: {str(e)}")
        raise


def process_bajaj_allianz_life_insurance(file_path, template_data, risk_code_data, cust_neft_data,
                                    table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print(data.head(10))

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print(data.head(10))

        # Remove rows where any column contains 'Grand Total'
        data = data[~data.apply(lambda row: row.astype(str).str.contains('Grand Total', case=False, na=False).any(), axis=1)].reset_index(drop=True)

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        mapped_df[template_col] = section[attachment_col]
                    else:
                        mapped_df[template_col] = ''
            else:
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # ---- New Logic: Update 'Client Name' based on 'ASP Practice' using 'state_lookups.xlsx' 'Sheet5' ----
            if 'ASP Practice' in mapped_df.columns:
                state_lookups_sheet5 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet5',
                )
                # Clean column names
                state_lookups_sheet5.columns = state_lookups_sheet5.columns.str.strip()
                # Ensure 'ASP Practice' and 'MASTER_POLICY_NO' are strings
                mapped_df['ASP Practice'] = mapped_df['ASP Practice'].astype(str).str.strip()
                state_lookups_sheet5['MASTER_POLICY_NO'] = state_lookups_sheet5['MASTER_POLICY_NO'].astype(str).str.strip()
                # Merge on 'ASP Practice' and 'MASTER_POLICY_NO'
                mapped_df = mapped_df.merge(
                    state_lookups_sheet5[['MASTER_POLICY_NO', 'Client Name']],
                    how='left',
                    left_on='ASP Practice',
                    right_on='MASTER_POLICY_NO'
                )
                # Update 'Client Name' where match is found
                mapped_df['Client Name'] = mapped_df['Client Name_y'].combine_first(mapped_df['Client Name_x'])
                # Drop the extra columns
                mapped_df = mapped_df.drop(columns=['MASTER_POLICY_NO', 'Client Name_x', 'Client Name_y'])
            else:
                pass  # If 'ASP Practice' not in columns, do nothing
            # ---- New Logic Ends Here ----

            # ---- Data Expansion Logic Starts Here ----
            # Create separate DataFrames for Brokerage1/Premium1, Brokerage2/Premium2, Brokerage3/Premium3
            df_list = []
            # List to maintain the order and insert blank rows
            dataframes = []

            # Helper function to set 'ASP Practice' based on mapping names
            def get_asp_practice(brokerage_num):
                attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{brokerage_num}'), '')
                if 'TP' in attachment_col.upper():
                    return 'Motor TP'
                elif 'TERR' in attachment_col.upper():
                    return 'Terrorism'
                else:
                    return ''

            for i in range(1, 4):
                brokerage_col = f'Brokerage{i}'
                premium_col = f'Premium{i}'
                if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                    df = mapped_df.copy()
                    df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                    if not df.empty:
                        df['Brokerage'] = df[brokerage_col]
                        df['Premium'] = df[premium_col]
                        # Set 'ASP Practice'
                        if i == 1:
                            df['ASP Practice'] = 'Normal'
                        else:
                            asp_practice_value = get_asp_practice(i)
                            df['ASP Practice'] = asp_practice_value
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                        df = df.drop(columns=cols_to_drop, errors='ignore')
                        df_list.append(df)
            # Arrange the DataFrames and insert blank rows
            for idx_df, df in enumerate(df_list):
                if idx_df > 0:
                    # Insert a blank row
                    blank_row = pd.DataFrame(columns=df.columns)
                    dataframes.append(blank_row)
                dataframes.append(df)

            # Concatenate all DataFrames
            if dataframes:
                processed_df = pd.concat(dataframes, ignore_index=True)
            else:
                # If no dataframes were created, use the mapped_df as is
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            processed_df['Income category'] = processed_df['P & L JV']

            # ---- New Logic Starts Here ----
            # If the 'Risk' column has only numbers
            if processed_df['Risk'].apply(lambda x: str(x).strip().isdigit()).all():
                # Open 'Risk code.xlsx' from support files folder, open 'Sheet1'
                risk_code_path = (
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files\Risk code.xlsx'
                )
                risk_code_df = pd.read_excel(risk_code_path, sheet_name='Sheet1')
                # Clean column names
                risk_code_df.columns = risk_code_df.columns.str.strip()
                # Ensure 'Risk' and 'PRODUCT_4DIGIT_CODE' are strings
                processed_df['Risk'] = processed_df['Risk'].astype(str).str.strip()
                risk_code_df['PRODUCT_4DIGIT_CODE'] = risk_code_df['PRODUCT_4DIGIT_CODE'].astype(str).str.strip()
                # Merge
                processed_df = processed_df.merge(
                    risk_code_df[['PRODUCT_4DIGIT_CODE', 'PRODUCT_NAME']],
                    how='left',
                    left_on='Risk',
                    right_on='PRODUCT_4DIGIT_CODE'
                )
                # Update 'Risk' column with 'PRODUCT_NAME' where match found
                processed_df['Risk'] = processed_df['PRODUCT_NAME'].fillna(processed_df['Risk'])
                # Drop the extra columns
                processed_df = processed_df.drop(columns=['PRODUCT_4DIGIT_CODE', 'PRODUCT_NAME'])
            # ---- New Logic Ends Here ----

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '').replace(')', ''))
                        premium = float(str(row['Premium']).replace(',', '').replace('(', '').replace(')', ''))
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'P & L JV' in processed_df.columns:
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                processed_df['P & L JV'] = ''

            # Update 'Endorsement No.' and 'P & L JV' based on 'Policy No.'
            if 'Endorsement No.' in processed_df.columns:
                def process_endorsement(row):
                    policy_no = str(row['Policy No.']).strip()
                    if '-' in policy_no:
                        parts = policy_no.split('-')
                        last_part = parts[-1]
                        if last_part.startswith('E'):
                            row['Endorsement No.'] = last_part
                            row['P & L JV'] = 'Endorsement'
                        else:
                            row['Endorsement No.'] = ''
                            row['P & L JV'] = ''
                    else:
                        row['Endorsement No.'] = ''
                        row['P & L JV'] = ''
                    return row
                processed_df = processed_df.apply(process_endorsement, axis=1)
            else:
                pass

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Bajaj Allianz Life Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = processed_df.get('ASP Practice', '')
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(str).apply(
                lambda x: float(str(x).replace(',', '').replace('(', '').replace(')', '')) if x != '' else 0.0
            ).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            # Modification: Take the first value instead of sum
            if len(net_amount_values_numeric) > 0:
                net_amount_value = net_amount_values_numeric.iloc[0]
            else:
                net_amount_value = 0.0
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''

            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')

            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                # Modification: Take the first value instead of sum
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Bajaj Allianz Life Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Bajaj Allianz Life insurance: {str(e)}")
        raise


def process_care_health_insurance_limited(file_path, template_data, risk_code_data, cust_neft_data,
                                          table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print("Initial Data Preview:")
        print(data.head(10))

        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print("Data after removing empty rows:")
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
        else:
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_col in mappings.items():
                    if attachment_col in section.columns:
                        mapped_df[template_col] = section[attachment_col]
                    else:
                        mapped_df[template_col] = ''
            else:
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # ---- Data Expansion Logic Starts Here ----
            # Create separate DataFrames for Brokerage1/Premium1, Brokerage2/Premium2, Brokerage3/Premium3
            df_list = []
            # List to maintain the order and insert blank rows
            dataframes = []

            # Helper function to set 'ASP Practice' based on mapping names
            def get_asp_practice(brokerage_num):
                attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{brokerage_num}'), '')
                if 'TP' in attachment_col.upper():
                    return 'Motor TP'
                elif 'TERR' in attachment_col.upper():
                    return 'Terrorism'
                else:
                    return ''

            for i in range(1, 4):
                brokerage_col = f'Brokerage{i}'
                premium_col = f'Premium{i}'
                if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                    df = mapped_df.copy()
                    df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                    if not df.empty:
                        df['Brokerage'] = df[brokerage_col]
                        df['Premium'] = df[premium_col]
                        # Set 'ASP Practice'
                        if i == 1:
                            df['ASP Practice'] = 'Normal'
                        else:
                            asp_practice_value = get_asp_practice(i)
                            df['ASP Practice'] = asp_practice_value
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                        df = df.drop(columns=cols_to_drop, errors='ignore')
                        df_list.append(df)
            # Arrange the DataFrames and insert blank rows
            for idx_df, df in enumerate(df_list):
                if idx_df > 0:
                    # Insert a blank row
                    blank_row = pd.DataFrame(columns=df.columns)
                    dataframes.append(blank_row)
                dataframes.append(df)

            # Concatenate all DataFrames
            if dataframes:
                processed_df = pd.concat(dataframes, ignore_index=True)
            else:
                # If no dataframes were created, use the mapped_df as is
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 4)] + [f'Premium{j}' for j in range(1, 4)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            # Replace blank values in 'Premium' column with 'No value found'
            if 'Premium' in processed_df.columns:
                processed_df['Premium'] = processed_df['Premium'].apply(lambda x: 'No value found' if pd.isnull(x) or x == '' else x)

            # ---- Policy No and Endorsement No Processing Starts Here ----
            # Replace single quotes in 'Policy No' and 'Endorsement No'
            for col in ['Policy No.', 'Endorsement No.']:
                if col in processed_df.columns:
                    processed_df[col] = processed_df[col].astype(str).str.replace("’", "'", regex=False).str.replace("‘", "'", regex=False)

            # Handle 'P & L JV' based solely on 'Endorsement No.'
            if 'Endorsement No.' in processed_df.columns:
                def set_p_and_l_jv(endorsement_no):
                    if pd.isna(endorsement_no):
                        return ''
                    endorsement_no_str = str(endorsement_no).strip().lstrip('0')
                    if endorsement_no_str in ['', '0']:
                        return ''
                    else:
                        return 'Endorsement'

                processed_df['P & L JV'] = processed_df['Endorsement No.'].apply(set_p_and_l_jv)
            else:
                processed_df['P & L JV'] = ''

            # ---- Policy No and Endorsement No Processing Ends Here ----

            # Remove empty rows and reset index if necessary
            processed_df = processed_df.dropna(how='all').reset_index(drop=True)

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '').replace(')', ''))
                        premium_val = str(row['Premium']).replace(',', '').replace('(', '').replace(')', '')
                        if premium_val.lower() == 'no value found':
                            return 0
                        premium = float(premium_val)
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))

            # For 'P & L JV' column, no lookup is needed (already handled above)
            # Removed the lookup-based assignment

            # Income category lookup
            if 'Income category' in processed_df.columns:
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups']
                    .astype(str)
                    .str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Care Health Insurance Limited.'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['ASP Practice'] = processed_df.get('ASP Practice', '')
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(str).apply(
                lambda x: float(str(x).replace(',', '').replace('(', '').replace(')', '')) if x != '' else 0.0
            ).sum()

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            # Modification: Take the first value instead of sum
            if len(net_amount_values_numeric) > 0:
                net_amount_value = net_amount_values_numeric.iloc[0]
            else:
                net_amount_value = 0.0
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
            else:
                debtor_branch_ref = ''
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name

            # Convert date to dd/mm/yyyy format
            try:
                date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
            except:
                date_col_formatted = ''

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                # Modification: Take the first value instead of sum
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
            else:
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': ['', ''],
                    'Debtor Name': [processed_df['Debtor Name'].iloc[0]] * 2,
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': [processed_df['AccountType'].iloc[0]] * 2,
                    'Debtor Branch Ref': [processed_df['Debtor Branch Ref'].iloc[0]] * 2,
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': ['', ''],
                    'Risk': ['', ''],
                    'Endorsement No.': ["", ""],
                    'Policy Type': ['', ''],
                    'Policy Start Date': ['', ''],
                    'Policy End Date': ['', ''],
                    'Premium': ['0.00', '0.00'],
                    'Brokerage Rate': ['', ''],
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': [narration, narration],
                    'NPT': ['', ''],
                    'Bank Ledger': [bank_ledger_value, bank_ledger_value],
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [processed_df['Service Tax Ledger'].iloc[0], '2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26', 'TDS Receivable - AY 2025-26'],
                    'RepDate': [processed_df['RepDate'].iloc[-1], processed_df['RepDate'].iloc[-1]],
                    'Branch': ['', ''],
                    'Income category': ['', ''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1], processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': [processed_df['NPT2'].iloc[-1], processed_df['NPT2'].iloc[-1]]
                })
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': [''],
                    'Debtor Name': [processed_df['Debtor Name'].iloc[0]],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': [processed_df['AccountType'].iloc[0]],
                    'Debtor Branch Ref': [processed_df['Debtor Branch Ref'].iloc[0]],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': [''],
                    'Risk': [''],
                    'Endorsement No.': [""],
                    'Policy Type': [''],
                    'Policy Start Date': [''],
                    'Policy End Date': [''],
                    'Premium': ['0.00'],
                    'Brokerage Rate': [''],
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': [narration],
                    'NPT': [''],
                    'Bank Ledger': [bank_ledger_value],
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': [processed_df['RepDate'].iloc[-1]],
                    'Branch': [''],
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': [processed_df['NPT2'].iloc[-1]]
                })

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = ''.join(e for e in safe_narration if e.isalnum() or e == ' ').strip()
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Care Health Insurance Limited Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Care Health Insurance Limited: {str(e)}")
        raise


def process_magma_hdi_general_insurance_company(file_path, template_data, risk_code_data, cust_neft_data,
                                                table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print("Data read from file:")
        print(data.head(10))
        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print("Data after removing empty rows:")
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
            print("No repeating headers found, treating entire data as one section.")
        else:
            print(f"Repeating headers found at indices: {header_indices}")
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
                print(f"Section {idx+1} created from rows {start_idx} to {end_idx}")
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            print(f"Processing section {idx+1}")
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                print("Mappings provided, processing mappings.")
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_cols in mappings.items():
                    if not isinstance(template_cols, list):
                        template_cols = [template_cols]
                    if attachment_col in section.columns:
                        for template_col in template_cols:
                            mapped_df[template_col] = section[attachment_col]
                            print(f"Mapped '{attachment_col}' to '{template_col}'")
                    else:
                        for template_col in template_cols:
                            mapped_df[template_col] = ''
                            print(f"Attachment column '{attachment_col}' not found, setting '{template_col}' to empty.")
            else:
                print("No mappings provided, proceeding without mappings.")
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # Now, as per user's request, 'Income category' needs to be a copy of 'P & L JV'
            # So if 'P & L JV' is in mapped_df, we need to create 'Income category' as a copy
            if 'P & L JV' in mapped_df.columns:
                mapped_df['Income category'] = mapped_df['P & L JV']
                print("'Income category' set as a copy of 'P & L JV'")
            else:
                mapped_df['Income category'] = ''
                print("'P & L JV' not found in mapped data, 'Income category' set to empty.")

            # ---- Data Expansion Logic Starts Here ----
            # Before expanding data, check if sum of 'Brokerage1' matches 'table_3's column 1 and row1
            # If it matches, then 'Brokerage1' becomes 'Brokerage' column, no additional rows need to be there
            # Else, proceed to create separate DataFrames for 'Brokerage1/Premium1', 'Brokerage2/Premium2'
            brokerage1_col = 'Brokerage1'
            if brokerage1_col in mapped_df.columns:
                # Calculate sum of 'Brokerage1'
                brokerage1_sum = pd.to_numeric(mapped_df[brokerage1_col].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '').str.strip(), errors='coerce').fillna(0).sum()
                print(f"Sum of 'Brokerage1' is {brokerage1_sum}")
                # Get 'table_3's column 1 and row1 value
                if not table_3.empty:
                    table_3_col1 = table_3.columns[0]
                    table_3_row1_value = table_3.iloc[0][table_3_col1]
                    table_3_row1_value_numeric = float(str(table_3_row1_value).replace(',', '').replace('(', '-').replace(')', '').strip())
                    print(f"Value from table_3 column 1, row 1 is {table_3_row1_value_numeric}")
                    # Compare
                    if np.isclose(brokerage1_sum, table_3_row1_value_numeric, atol=0.01):
                        print("Sum of 'Brokerage1' matches 'table_3's column 1 and row1, proceeding without additional rows.")
                        # 'Brokerage1' becomes 'Brokerage' column
                        mapped_df['Brokerage'] = mapped_df['Brokerage1']
                        mapped_df['Premium'] = mapped_df['Premium1']
                        # Drop unnecessary columns
                        cols_to_drop = [f'Brokerage{j}' for j in range(1, 3)] + [f'Premium{j}' for j in range(1, 3)]
                        mapped_df = mapped_df.drop(columns=cols_to_drop, errors='ignore')
                        processed_df = mapped_df.copy()
                    else:
                        print("Sum of 'Brokerage1' does not match 'table_3's column 1 and row1, proceeding to expand data.")
                        # Proceed to create separate DataFrames for 'Brokerage1/Premium1', 'Brokerage2/Premium2'
                        df_list = []
                        # List to maintain the order and insert blank rows
                        dataframes = []

                        for i in range(1, 3):
                            brokerage_col = f'Brokerage{i}'
                            premium_col = f'Premium{i}'
                            if brokerage_col in mapped_df.columns and premium_col in mapped_df.columns:
                                df = mapped_df.copy()
                                df = df[df[brokerage_col].notna() & (df[brokerage_col] != '')]
                                if not df.empty:
                                    df['Brokerage'] = df[brokerage_col]
                                    df['Premium'] = df[premium_col]
                                    # Set 'ASP Practice'
                                    if i == 1:
                                        df['ASP Practice'] = 'Normal'
                                    else:
                                        # Need to set 'ASP Practice' based on mapping names
                                        attachment_col = next((k for k, v in mappings.items() if v == f'Brokerage{i}'), '')
                                        if 'TP' in attachment_col.upper():
                                            df['ASP Practice'] = 'Motor TP'
                                        elif 'TERR' in attachment_col.upper():
                                            df['ASP Practice'] = 'Terrorism'
                                        else:
                                            df['ASP Practice'] = ''
                                    # Drop unnecessary columns
                                    cols_to_drop = [f'Brokerage{j}' for j in range(1, 3)] + [f'Premium{j}' for j in range(1, 3)]
                                    df = df.drop(columns=cols_to_drop, errors='ignore')
                                    df_list.append(df)
                        # Arrange the DataFrames and insert blank rows
                        for idx_df, df in enumerate(df_list):
                            if idx_df > 0:
                                # Insert a blank row
                                blank_row = pd.DataFrame(columns=df.columns)
                                dataframes.append(blank_row)
                            dataframes.append(df)

                        # Concatenate all DataFrames
                        if dataframes:
                            processed_df = pd.concat(dataframes, ignore_index=True)
                        else:
                            # If no dataframes were created, use the mapped_df as is
                            processed_df = mapped_df.copy()
                            # Remove unnecessary columns
                            cols_to_drop = [f'Brokerage{j}' for j in range(1, 3)] + [f'Premium{j}' for j in range(1, 3)]
                            processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
                else:
                    print("table_3 is empty, cannot compare 'Brokerage1' sum with 'table_3's column 1 and row1.")
                    # Proceed as per default (could decide what to do in this case)
                    # Let's proceed without additional rows
                    mapped_df['Brokerage'] = mapped_df['Brokerage1']
                    mapped_df['Premium'] = mapped_df['Premium1']
                    # Drop unnecessary columns
                    cols_to_drop = [f'Brokerage{j}' for j in range(1, 3)] + [f'Premium{j}' for j in range(1, 3)]
                    mapped_df = mapped_df.drop(columns=cols_to_drop, errors='ignore')
                    processed_df = mapped_df.copy()
            else:
                print("'Brokerage1' not in mapped data, proceeding without data expansion.")
                processed_df = mapped_df.copy()
                # Remove unnecessary columns
                cols_to_drop = [f'Brokerage{j}' for j in range(1, 3)] + [f'Premium{j}' for j in range(1, 3)]
                processed_df = processed_df.drop(columns=cols_to_drop, errors='ignore')
            # ---- Data Expansion Logic Ends Here ----

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain
                    print(f"Processed date column '{column}'")
                else:
                    print(f"Date column '{column}' not in processed data or empty.")

            # For 'Risk' column, combine 'Risk1' and 'Risk2' into 'Risk', similar to 'raheja'
            if 'Risk1' in processed_df.columns and 'Risk2' in processed_df.columns:
                print("Processing 'Risk' column by combining 'Risk1' and 'Risk2'")
                def get_risk(row):
                    if pd.notnull(row['Risk1']) and str(row['Risk1']).strip() != '':
                        return row['Risk1']
                    elif pd.notnull(row['Risk2']) and str(row['Risk2']).strip() != '':
                        return row['Risk2']
                    else:
                        return ''
                processed_df['Risk'] = processed_df.apply(get_risk, axis=1)
                # Remove 'Risk1', 'Risk2' columns from processed_df
                processed_df.drop(columns=['Risk1', 'Risk2'], inplace=True)
            else:
                print("One of 'Risk1' or 'Risk2' not in processed data, setting 'Risk' to empty.")
                processed_df['Risk'] = ''

            # Process 'Client Name' column by concatenating 'Client Name' and 'Client Name2' when 'Client Name2' is not blank
            if 'Client Name' in processed_df.columns and 'Client Name2' in processed_df.columns:
                print("Processing 'Client Name' by concatenating 'Client Name' and 'Client Name2'")
                def get_client_name(row):
                    if pd.notnull(row['Client Name2']) and str(row['Client Name2']).strip() != '':
                        return str(row['Client Name']) + ' ' + str(row['Client Name2'])
                    else:
                        return str(row['Client Name'])
                processed_df['Client Name'] = processed_df.apply(get_client_name, axis=1)
                # Remove 'Client Name2' column from processed_df
                processed_df.drop(columns=['Client Name2'], inplace=True)
            else:
                print("'Client Name2' not in processed data, proceeding without concatenation.")

            # Remove rows where both 'Premium' and 'Brokerage' are 0
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df[['Premium', 'Brokerage']] = processed_df[['Premium', 'Brokerage']].apply(pd.to_numeric, errors='coerce').fillna(0)
                processed_df = processed_df[~((processed_df['Premium'] == 0) & (processed_df['Brokerage'] == 0))]
                print("Removed rows where both 'Premium' and 'Brokerage' are 0.")
            else:
                print("'Premium' or 'Brokerage' column not in processed data.")

            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '').str.strip()
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
                    print(f"Processed numeric column '{column}'")
                else:
                    print(f"Numeric column '{column}' not in processed data.")

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                print("Calculating 'Brokerage Rate'")
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '-').replace(')', '').strip())
                        premium = float(str(row['Premium']).replace(',', '').replace('(', '-').replace(')', '').strip())
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))
            else:
                print("'Premium' or 'Brokerage' column not in processed data, cannot calculate 'Brokerage Rate'.")

            # For 'Branch' column, map using 'state_lookups.xlsx' 'Sheet2'
            if 'Branch' in processed_df.columns:
                print("Processing 'Branch' column with lookup")
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                print("'Branch' column not in processed data, setting 'Branch' to empty.")
                processed_df['Branch'] = ''

            # For 'P & L JV' and 'Income category', perform lookups, and if match found, get the match, otherwise keep it blank. No other logic needed for these two.
            # Perform lookup on 'P & L JV' and update 'P & L JV'
            if 'P & L JV' in processed_df.columns:
                print("Processing 'P & L JV' with lookup")
                endorsement_type_mapping = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet3',
                )
                endorsement_type_mapping['Endorsement Type'] = (
                    endorsement_type_mapping['Endorsement Type']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                endorsement_type_mapping['lookup value'] = (
                    endorsement_type_mapping['lookup value']
                    .astype(str)
                    .str.strip()
                )
                processed_df['P & L JV'] = (
                    processed_df['P & L JV'].astype(str).str.strip().str.lower()
                )
                endorsement_lookup = (
                    endorsement_type_mapping.set_index('Endorsement Type')['lookup value']
                    .to_dict()
                )
                processed_df['P & L JV'] = processed_df['P & L JV'].map(
                    endorsement_lookup
                ).fillna('')
            else:
                print("'P & L JV' not in processed data, setting to empty.")
                processed_df['P & L JV'] = ''

            # Perform lookup on 'Income category' and update 'Income category'
            if 'Income category' in processed_df.columns:
                print("Processing 'Income category' with lookup")
                state_lookups_sheet4 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet4',
                )
                state_lookups_sheet4['BUSINESS_TYPE'] = (
                    state_lookups_sheet4['BUSINESS_TYPE']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                state_lookups_sheet4['lookups'] = (
                    state_lookups_sheet4['lookups'].astype(str).str.strip()
                )
                processed_df['Income category'] = (
                    processed_df['Income category']
                    .astype(str)
                    .str.strip()
                    .str.lower()
                )
                income_category_lookup = (
                    state_lookups_sheet4.set_index('BUSINESS_TYPE')['lookups'].to_dict()
                )
                processed_df['Income category'] = processed_df['Income category'].map(
                    income_category_lookup
                ).fillna('')
            else:
                print("'Income category' not in processed data, setting to empty.")
                processed_df['Income category'] = ''

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Magma Hdi General Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''
            processed_df['ASP Practice'] = ''
            print("Set default columns in processed data.")

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()
            print(f"Sum of 'Brokerage' is {sum_brokerage}")

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)
            print(f"Net amount from 'table_3' is {net_amount_value}")

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)
            print(f"Does sum of 'Brokerage' equal net amount from 'table_3'? {brokerage_equals_net_amount}")

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)
            print(f"Total amount from 'table_4' is {amount_total}")

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''
            print(f"Narration from 'table_4': {narration_from_table_4}")

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)
            print(f"Is GST present in 'table_3' columns? {gst_present}")

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()
            print(f"Safe narration for file naming: {safe_narration}")

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
                print(f"Found 'Debtor Branch Ref' for insurer '{insurer_name}': {debtor_branch_ref}")
            else:
                debtor_branch_ref = ''
                print(f"No 'Debtor Branch Ref' found for insurer '{insurer_name}', setting to empty.")
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name
            print("Updated 'Debtor Branch Ref', 'Service Tax Ledger', 'Debtor Name' in processed data.")

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
            print(f"Formatted date: {date_col_formatted}")

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break
            print(f"Supplier name from 'table_4': {supplier_name_col}")

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration
            print(f"Narration set in processed data: {narration}")

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value
            print(f"'Bank Ledger' set to: {bank_ledger_value}")

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                print(f"TDS column found in 'table_3': {tds_column}")
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
                print(f"Third new row 'Brokerage' value: {third_new_row_brokerage}")
            else:
                print("No TDS column found in 'table_3', setting 'third_new_row_brokerage' to 0.0")
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                print(f"GST amount calculated: {first_new_row_brokerage}")
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
                print("Created new rows for GST present case.")
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
                print("Created new row for GST not present case.")

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)
            print("Concatenated new rows to processed data.")

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]
            print("Rearranged columns in processed data.")

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            print("Removed empty rows and updated 'Entry No.'")

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
                print(f"Appended processed section {idx+1} to processed sections.")
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
            print("Concatenated all processed sections.")
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")
        print(f"Generated file name components: {safe_narration}, {date_str}")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Magma Hdi General Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)
        print(f"Ensured directories exist: {excel_dir}, {csv_dir}")

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Magma Hdi General Insurance Company: {str(e)}")
        raise



def process_generali_india_insurance_company(file_path, template_data, risk_code_data, cust_neft_data,
                                             table_3, table_4, table_5, subject, mappings):
    try:
        # Read the file based on its extension, including xlsb files
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            data = pd.read_excel(file_path, header=0)
        elif file_extension == '.xlsb':
            data = pd.read_excel(file_path, engine='pyxlsb', header=0)
        elif file_extension == '.csv':
            data = pd.read_csv(file_path, header=0)
        elif file_extension == '.ods':
            data = pd.read_excel(file_path, engine='odf', header=0)
        elif file_extension == '.txt':
            data = pd.read_csv(file_path, delimiter='\t', header=0)
        elif file_extension == '.xls':
            data = pd.read_excel(file_path, engine='xlrd', header=0)
        else:
            raise ValueError("Unsupported file format")

        print("Data read from file:")
        print(data.head(10))
        # Remove empty rows to avoid empty dataframes
        data = data.dropna(how='all').reset_index(drop=True)
        data = data[data.apply(lambda row: row.count() > 4, axis=1)].reset_index(drop=True)
        print("Data after removing empty rows:")
        print(data.head(10))

        # Handle repeating header rows and split data into sections
        columns = data.columns.tolist()
        columns_cleaned = [str(col).strip().lower() for col in columns]  # Cleaned column names for comparison

        header_indices = []
        for i, row in data.iterrows():
            # Convert all row values to string, strip whitespace, and lowercase
            row_values = row.astype(str).str.strip().str.lower().tolist()
            if row_values == columns_cleaned:
                header_indices.append(i)

        if not header_indices:
            # If no repeating headers are found, treat entire data as one section
            sections = [data]
            print("No repeating headers found, treating entire data as one section.")
        else:
            print(f"Repeating headers found at indices: {header_indices}")
            header_indices.append(len(data))  # Add the end index
            sections = []
            for idx in range(len(header_indices) - 1):
                start_idx = header_indices[idx] + 1
                end_idx = header_indices[idx + 1]
                section_df = data.iloc[start_idx:end_idx].reset_index(drop=True)
                # Assign the original column names
                section_df.columns = columns
                sections.append(section_df)
                print(f"Section {idx+1} created from rows {start_idx} to {end_idx}")
        # ---- Updated Splitting Logic Ends Here ----

        processed_sections = []
        for idx, section in enumerate(sections):
            print(f"Processing section {idx+1}")
            # Clean column names and data
            section.columns = section.columns.str.strip()
            section = section.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            # Process mappings from frontend (attachment columns on left, template columns on right)
            if mappings:
                print("Mappings provided, processing mappings.")
                # Create a DataFrame to hold mapped data
                mapped_df = pd.DataFrame()
                for attachment_col, template_cols in mappings.items():
                    if not isinstance(template_cols, list):
                        template_cols = [template_cols]
                    if attachment_col in section.columns:
                        for template_col in template_cols:
                            mapped_df[template_col] = section[attachment_col]
                            print(f"Mapped '{attachment_col}' to '{template_col}'")
                    else:
                        for template_col in template_cols:
                            mapped_df[template_col] = ''
                            print(f"Attachment column '{attachment_col}' not found, setting '{template_col}' to empty.")
            else:
                print("No mappings provided, proceeding without mappings.")
                mapped_df = section.copy()  # Proceed without mappings if not provided

            # After mapping, keep the data as is
            processed_df = mapped_df.copy()

            # Process 'Brokerage' columns according to your instructions
            if 'Brokerage1' in processed_df.columns:
                if (('Brokerage2' in processed_df.columns and processed_df['Brokerage2'].isnull().all()) or ('Brokerage2' not in processed_df.columns)) and \
                   (('Brokerage3' in processed_df.columns and processed_df['Brokerage3'].isnull().all()) or ('Brokerage3' not in processed_df.columns)):
                    # If Brokerage2 and Brokerage3 are blank or not present, then Brokerage1 is your Brokerage
                    processed_df['Brokerage'] = processed_df['Brokerage1']
                    print("Brokerage2 and Brokerage3 are blank or not present, using Brokerage1 as Brokerage.")
                else:
                    # Sum Brokerage1, Brokerage2, Brokerage3 into Brokerage
                    # Handle missing columns
                    brokerage_columns = [col for col in ['Brokerage1', 'Brokerage2', 'Brokerage3'] if col in processed_df.columns]
                    processed_df['Brokerage'] = processed_df[brokerage_columns].sum(axis=1, min_count=1)
                    print("Summed Brokerage1, Brokerage2, Brokerage3 into Brokerage.")
            else:
                print("'Brokerage1' column not in processed data, setting 'Brokerage' to empty.")
                processed_df['Brokerage'] = ''

            # Process 'Premium' columns similarly if needed
            if 'Premium1' in processed_df.columns:
                if (('Premium2' in processed_df.columns and processed_df['Premium2'].isnull().all()) or ('Premium2' not in processed_df.columns)) and \
                   (('Premium3' in processed_df.columns and processed_df['Premium3'].isnull().all()) or ('Premium3' not in processed_df.columns)):
                    # If Premium2 and Premium3 are blank or not present, then Premium1 is your Premium
                    processed_df['Premium'] = processed_df['Premium1']
                    print("Premium2 and Premium3 are blank or not present, using Premium1 as Premium.")
                else:
                    # Sum Premium1, Premium2, Premium3 into Premium
                    # Handle missing columns
                    premium_columns = [col for col in ['Premium1', 'Premium2', 'Premium3'] if col in processed_df.columns]
                    processed_df['Premium'] = processed_df[premium_columns].sum(axis=1, min_count=1)
                    print("Summed Premium1, Premium2, Premium3 into Premium.")
            else:
                print("'Premium1' column not in processed data, setting 'Premium' to empty.")
                processed_df['Premium'] = ''

            # For 'Risk' column, it's coming in, no further processing needed

            # 'Branch' needs lookup
            if 'Branch' in processed_df.columns:
                print("Processing 'Branch' column with lookup")
                state_lookups_sheet2 = pd.read_excel(
                    r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
                    r'\Common folder AP & AR\Brokerage Statement Automation\support files'
                    r'\state_lookups.xlsx',
                    sheet_name='Sheet2',
                )
                state_lookups_sheet2['state'] = (
                    state_lookups_sheet2['state'].astype(str).str.strip().str.lower()
                )
                state_lookups_sheet2['shortform'] = (
                    state_lookups_sheet2['shortform'].astype(str).str.strip()
                )
                processed_df['Branch'] = (
                    processed_df['Branch'].astype(str).str.strip().str.lower()
                )
                branch_lookup = state_lookups_sheet2.set_index('state')[
                    'shortform'
                ].to_dict()
                processed_df['Branch'] = processed_df['Branch'].map(branch_lookup).fillna('')
            else:
                print("'Branch' column not in processed data, setting 'Branch' to empty.")
                processed_df['Branch'] = ''

            # Handle dates in 'Policy Start Date' and 'Policy End Date' columns after mappings
            date_columns = ['Policy Start Date', 'Policy End Date']
            for column in date_columns:
                if column in processed_df.columns and not processed_df[column].empty:
                    processed_df[column] = processed_df[column].apply(parse_date_flexible)
                    processed_df[column] = processed_df[column].apply(lambda x: x.strftime('%d/%m/%Y') if isinstance(x, datetime) else '')
                    processed_df[column] = processed_df[column].fillna('')  # Ensure no nulls remain
                    print(f"Processed date column '{column}'")
                else:
                    print(f"Date column '{column}' not in processed data or empty.")

            # For 'Endorsement No.', if 'Policy No.' and 'Endorsement No.' are exactly the same, set 'Endorsement No.' to blank
            if 'Policy No.' in processed_df.columns and 'Endorsement No.' in processed_df.columns:
                processed_df['Policy No.'] = processed_df['Policy No.'].astype(str).str.strip()
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].astype(str).str.strip()
                processed_df.loc[processed_df['Policy No.'] == processed_df['Endorsement No.'], 'Endorsement No.'] = ''
                print("Set 'Endorsement No.' to blank where it equals 'Policy No.'")

            # Also, if 'Endorsement No.' is '0', '00', '0.0', '0.00', '1', '1.0', etc., set it to blank
            endorsement_no_values_to_blank = ['0', '00', '0.0', '0.00', '1', '1.0']
            if 'Endorsement No.' in processed_df.columns:
                processed_df['Endorsement No.'] = processed_df['Endorsement No.'].replace(endorsement_no_values_to_blank, '')
                print("Set 'Endorsement No.' to blank for specific values")

            # 'P & L JV' logic
            if 'Endorsement No.' in processed_df.columns:
                processed_df['P & L JV'] = processed_df.apply(
                    lambda row: 'Endorsement' if row['Endorsement No.'] != '' else '', axis=1
                )
                print("Processed 'P & L JV' based on 'Endorsement No.'")
            else:
                processed_df['P & L JV'] = ''
                print("'Endorsement No.' not in processed data, setting 'P & L JV' to empty.")

            # Remove rows where both 'Premium' and 'Brokerage' are 0
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                processed_df[['Premium', 'Brokerage']] = processed_df[['Premium', 'Brokerage']].apply(pd.to_numeric, errors='coerce').fillna(0)
                processed_df = processed_df[~((processed_df['Premium'] == 0) & (processed_df['Brokerage'] == 0))]
                print("Removed rows where both 'Premium' and 'Brokerage' are 0.")
            else:
                print("'Premium' or 'Brokerage' column not in processed data.")

            # Ensure numeric columns are handled correctly after mappings
            numeric_columns = ['Premium', 'Brokerage']
            for column in numeric_columns:
                if column in processed_df.columns:
                    processed_df[column] = processed_df[column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '').str.strip()
                    processed_df[column] = pd.to_numeric(processed_df[column], errors='coerce').fillna(0)
                    print(f"Processed numeric column '{column}'")
                else:
                    print(f"Numeric column '{column}' not in processed data.")

            # Calculate 'Brokerage Rate' as (Brokerage / Premium) * 100, rounded to 2 decimals
            if 'Premium' in processed_df.columns and 'Brokerage' in processed_df.columns:
                print("Calculating 'Brokerage Rate'")
                def calc_brokerage_rate(row):
                    try:
                        brokerage = float(str(row['Brokerage']).replace(',', '').replace('(', '-').replace(')', '').strip())
                        premium = float(str(row['Premium']).replace(',', '').replace('(', '-').replace(')', '').strip())
                        if premium != 0:
                            return round((brokerage / premium) * 100, 2)
                        else:
                            return 0
                    except:
                        return 0
                processed_df['Brokerage Rate'] = processed_df.apply(calc_brokerage_rate, axis=1)
                processed_df['Brokerage Rate'] = processed_df['Brokerage Rate'].apply(lambda x: "{0:.2f}".format(x))
            else:
                print("'Premium' or 'Brokerage' column not in processed data, cannot calculate 'Brokerage Rate'.")

            # 'Income category' comes from mappings
            if 'Income category' not in processed_df.columns:
                processed_df['Income category'] = ''
                print("'Income category' not in processed data, setting to empty.")

            # Set 'Entry No.' and other columns
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            processed_df['Debtor Name'] = 'Future Generali India Insurance Company Limited'
            processed_df['AccountType'] = "Customer"
            processed_df['AccountTypeDuplicate'] = processed_df['AccountType']
            processed_df['Nature of Transaction'] = "Brokerage Statement"
            processed_df['TDS Ledger'] = processed_df['Debtor Name']
            processed_df['RepDate'] = datetime.today().strftime('%d-%b-%y')
            processed_df['NPT2'] = subject.replace('FW:', '').replace('RE:', '').strip()
            processed_df['Debtor Branch Ref'] = ''
            processed_df['NPT'] = ''
            processed_df['Bank Ledger'] = ''
            processed_df['Service Tax Ledger'] = ''
            processed_df['Narration'] = ''
            processed_df['Policy Type'] = ''
            processed_df['ASP Practice'] = ''
            print("Set default columns in processed data.")

            # Calculate sum of 'Brokerage'
            sum_brokerage = processed_df['Brokerage'].astype(float).sum()
            print(f"Sum of 'Brokerage' is {sum_brokerage}")

            # Get 'Net Amount' from 'table_3'
            net_amount_column = table_3.columns[-1]
            net_amount_values_cleaned = table_3[net_amount_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            net_amount_values_numeric = pd.to_numeric(net_amount_values_cleaned, errors='coerce').fillna(0)
            net_amount_value = net_amount_values_numeric.sum()
            net_amount_value_formatted = "{:,.2f}".format(net_amount_value)
            print(f"Net amount from 'table_3' is {net_amount_value}")

            # Check if sum_brokerage is approximately equal to net_amount_value
            brokerage_equals_net_amount = np.isclose(sum_brokerage, net_amount_value, atol=0.01)
            print(f"Does sum of 'Brokerage' equal net amount from 'table_3'? {brokerage_equals_net_amount}")

            # Get details from 'table_4'
            amount_values_cleaned = table_4['Amount'].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
            amount_values_numeric = pd.to_numeric(amount_values_cleaned, errors='coerce').fillna(0)
            amount_total = amount_values_numeric.sum()
            narration_value_original = "{:,.2f}".format(amount_total)
            print(f"Total amount from 'table_4' is {amount_total}")

            bank_value = table_4['Bank'].iloc[0] if 'Bank' in table_4.columns else ''
            date_col = table_4['Date'].iloc[0] if 'Date' in table_4.columns else datetime.today().strftime('%d/%m/%Y')
            insurer_name = table_4['Insurer Name'].iloc[0] if 'Insurer Name' in table_4.columns else ''
            if 'Narration' in table_4.columns and not table_4['Narration'].empty:
                narration_from_table_4 = table_4['Narration'].iloc[0]
            elif 'Narration (Ref)' in table_4.columns and not table_4['Narration (Ref)'].empty:
                narration_from_table_4 = table_4['Narration (Ref)'].iloc[0]
            else:
                narration_from_table_4 = ''
            print(f"Narration from 'table_4': {narration_from_table_4}")

            # Get 'GST' presence in 'table_3' columns
            gst_present = any('GST' in col or 'GST @18%' in col for col in table_3.columns)
            print(f"Is GST present in 'table_3' columns? {gst_present}")

            # Remove special characters from 'Narration' for file naming
            safe_narration = ''.join(e for e in narration_from_table_4 if e.isalnum() or e == ' ').strip()
            print(f"Safe narration for file naming: {safe_narration}")

            # Get 'Debtor Branch Ref' from 'cust_neft_data' using 'Insurer Name'
            debtor_branch_ref_row = cust_neft_data[cust_neft_data['Name'].str.lower() == insurer_name.lower()]
            if not debtor_branch_ref_row.empty:
                debtor_branch_ref = debtor_branch_ref_row['No.2'].iloc[0]
                print(f"Found 'Debtor Branch Ref' for insurer '{insurer_name}': {debtor_branch_ref}")
            else:
                debtor_branch_ref = ''
                print(f"No 'Debtor Branch Ref' found for insurer '{insurer_name}', setting to empty.")
            processed_df['Debtor Branch Ref'] = debtor_branch_ref
            processed_df['Service Tax Ledger'] = processed_df['Debtor Branch Ref'].str.replace('CUST_NEFT_', '')
            processed_df['Debtor Name'] = insurer_name
            print("Updated 'Debtor Branch Ref', 'Service Tax Ledger', 'Debtor Name' in processed data.")

            # Convert date to dd/mm/yyyy format
            date_col_formatted = pd.to_datetime(date_col).strftime('%d/%m/%Y')
            print(f"Formatted date: {date_col_formatted}")

            # Get 'supplier_name_col' from 'table_4'
            supplier_name_col = ''
            for col in ['Insurer Name', 'Insurer', 'SupplierName']:
                if col in table_4.columns and not table_4[col].empty:
                    supplier_name_col = table_4[col].iloc[0]
                    break
            print(f"Supplier name from 'table_4': {supplier_name_col}")

            # Create narration considering GST and value in brackets
            if gst_present:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} with GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} with GST 18%"
            else:
                if not np.isclose(float(narration_value_original.replace(',', '')), net_amount_value, atol=0.01):
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} ({net_amount_value_formatted}) from {supplier_name_col} without GST 18%"
                else:
                    narration = f"BNG NEFT DT-{date_col_formatted} rcvd towrds brkg Rs.{narration_value_original} from {supplier_name_col} without GST 18%"
            processed_df['Narration'] = narration
            print(f"Narration set in processed data: {narration}")

            # Map 'Bank Ledger' similar to others
            bank_ledger_lookup = {
                'CITI_005_2600004': 'CITIBANK 340214005 ACCOUNT',
                'HSBC_001_2600014': 'HSBC A/C-030-618375-001',
                'HSBC': 'HSBC A/C-030-618375-001'
            }
            bank_ledger_value = bank_value
            for key, value in bank_ledger_lookup.items():
                if bank_value == key:
                    bank_ledger_value = value
                    break
            processed_df['Bank Ledger'] = bank_ledger_value
            print(f"'Bank Ledger' set to: {bank_ledger_value}")

            # Calculate Brokerage values for the new rows
            tds_column = None
            for col in table_3.columns:
                if 'TDS' in col or 'TDS @10%' in col:
                    tds_column = col
                    break
            if tds_column is not None:
                print(f"TDS column found in 'table_3': {tds_column}")
                tds_values_cleaned = table_3[tds_column].astype(str).str.replace(',', '').str.replace('(', '-').str.replace(')', '')
                tds_values_numeric = pd.to_numeric(tds_values_cleaned, errors='coerce').fillna(0)
                invoice_nos = ', '.join(table_4['Invoice No'].dropna().astype(str).unique()) if 'Invoice No' in table_4.columns else ''
                if len(tds_values_numeric) > 0:
                    third_new_row_brokerage = tds_values_numeric.iloc[0]
                else:
                    third_new_row_brokerage = 0.0
                third_new_row_brokerage = -abs(third_new_row_brokerage)
                print(f"Third new row 'Brokerage' value: {third_new_row_brokerage}")
            else:
                print("No TDS column found in 'table_3', setting 'third_new_row_brokerage' to 0.0")
                third_new_row_brokerage = 0.0

            if gst_present:
                gst_amount = sum_brokerage * 0.18  # Assuming GST is 18%
                first_new_row_brokerage = gst_amount
                print(f"GST amount calculated: {first_new_row_brokerage}")
                # Create additional rows
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["GST Receipts", "Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["GST @ 18%", "TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': ["", ""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{first_new_row_brokerage:.2f}", f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': [processed_df['AccountTypeDuplicate'].iloc[0], 'G/L Account'],
                    'Service Tax Ledger': [
                        processed_df['Service Tax Ledger'].iloc[0],
                        '2300022'
                    ],
                    'TDS Ledger': [processed_df['TDS Ledger'].iloc[0], 'TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': ['', ''],
                    'ASP Practice': processed_df['ASP Practice'].iloc[-1],
                    'P & L JV': [invoice_nos, invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
                print("Created new rows for GST present case.")
            else:
                # Create additional row
                new_rows = pd.DataFrame({
                    'Entry No.': '',
                    'Debtor Name': processed_df['Debtor Name'].iloc[0],
                    'Nature of Transaction': ["Brokerage Statement"],
                    'AccountType': processed_df['AccountType'].iloc[0],
                    'Debtor Branch Ref': processed_df['Debtor Branch Ref'].iloc[0],
                    'Client Name': ["TDS Receivable - AY 2025-26"],
                    'Policy No.': '',
                    'Risk': '',
                    'Endorsement No.': [""],
                    'Policy Type': '',
                    'Policy Start Date': '',
                    'Policy End Date': '',
                    'Premium': '0.00',
                    'Brokerage Rate': '',
                    'Brokerage': [f"{third_new_row_brokerage:.2f}"],
                    'Narration': narration,
                    'NPT': '',
                    'Bank Ledger': bank_ledger_value,
                    'AccountTypeDuplicate': ['G/L Account'],
                    'Service Tax Ledger': ['2300022'],
                    'TDS Ledger': ['TDS Receivable - AY 2025-26'],
                    'RepDate': processed_df['RepDate'].iloc[-1],
                    'Branch': '',
                    'Income category': [''],
                    'ASP Practice': [processed_df['ASP Practice'].iloc[-1]],
                    'P & L JV': [invoice_nos],
                    'NPT2': processed_df['NPT2'].iloc[-1]
                })
                print("Created new row for GST not present case.")

            # Concatenate new_rows to processed_df
            processed_df = pd.concat([processed_df, new_rows], ignore_index=True)
            print("Concatenated new rows to processed data.")

            # Update 'Entry No.'
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)

            # Rearranging columns to desired order
            desired_columns = [
                'Entry No.', 'Debtor Name', 'Nature of Transaction', 'AccountType',
                'Debtor Branch Ref', 'Client Name', 'Policy No.', 'Risk',
                'Endorsement No.', 'Policy Type', 'Policy Start Date',
                'Policy End Date', 'Premium', 'Brokerage Rate', 'Brokerage',
                'Narration', 'NPT', 'Bank Ledger', 'AccountTypeDuplicate',
                'Service Tax Ledger', 'TDS Ledger', 'RepDate', 'Branch',
                'Income category', 'ASP Practice', 'P & L JV', 'NPT2',
            ]
            for col in desired_columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            processed_df = processed_df[desired_columns]
            print("Rearranged columns in processed data.")

            # Remove empty rows and update 'Entry No.'
            processed_df = processed_df.dropna(
                how='all',
                subset=processed_df.columns.difference(['Entry No.'])
            ).reset_index(drop=True)
            processed_df['Entry No.'] = range(1, len(processed_df) + 1)
            print("Removed empty rows and updated 'Entry No.'")

            # Only append non-empty processed_df
            if not processed_df.empty:
                processed_sections.append(processed_df)
                print(f"Appended processed section {idx+1} to processed sections.")
            else:
                print(f"Processed section {idx + 1} is empty after processing.")

        if processed_sections:
            # Concatenate all processed sections
            final_processed_df = pd.concat(processed_sections, ignore_index=True)
            print("Concatenated all processed sections.")
        else:
            raise ValueError("No valid processed sections to concatenate.")

        # Generate the shortened subject and date for the filename
        safe_narration = safe_narration.replace(' ', '_')[:50]
        date_str = datetime.now().strftime("%Y%m%d")
        print(f"Generated file name components: {safe_narration}, {date_str}")

        # Define output directories
        base_dir = (
            r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller'
            r'\Common folder AP & AR\Brokerage Statement Automation\Future Generali India Insurance Template Files'
        )
        excel_dir = os.path.join(base_dir, 'excel_file')
        csv_dir = os.path.join(base_dir, 'csv_file')

        # Ensure directories exist
        os.makedirs(excel_dir, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)
        print(f"Ensured directories exist: {excel_dir}, {csv_dir}")

        # Save the processed dataframe
        excel_file_name = f'{safe_narration}_{date_str}.xlsx'
        csv_file_name = f'{safe_narration}_{date_str}.csv'
        excel_file_path = os.path.join(excel_dir, excel_file_name)
        csv_file_path = os.path.join(csv_dir, csv_file_name)
        final_processed_df.to_excel(excel_file_path, index=False)
        final_processed_df.to_csv(csv_file_path, index=False)
        print(f"Saved Excel file: {excel_file_path}")
        print(f"Saved CSV file: {csv_file_path}")

        # Return the processed dataframe and the path to the Excel file
        return final_processed_df, excel_file_path

    except Exception as e:
        print(f"Error processing Future Generali India Insurance Company: {str(e)}")
        raise
