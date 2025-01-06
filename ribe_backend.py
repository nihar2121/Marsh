import pandas as pd
import re
import os

def process_file(filepath):
    """
    Reads the uploaded file (CSV or Excel),
    adds a 'Category' column based on conditions,
    and returns the path to the processed output file.
    """

    # Read the file into a DataFrame
    # Decide CSV vs Excel based on file extension
    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(filepath)
    else:
        df = pd.read_excel(filepath)

    # Define a helper function to assign category
    def assign_category(row):
        desc = str(row.get('DESCRIPTION', '')).lower()
        debit_str = str(row.get('DEBIT AMT', '')).strip()
        credit_str = str(row.get('CREDIT AMT', '')).strip()

        # 1) If credit_amt is present (non-empty/positive) and debit is empty => "Receipt"
        #    (We do a simple check for '(' to handle negative amounts with parentheses as well.)
        if credit_str and not credit_str.startswith('(') and credit_str != '0' and credit_str != '0.0':
            # Debit is presumably empty or zero => means we have a credit entry
            return "Receipt"
        
        # 2) Check keywords in DESCRIPTION for other categories.
        #    We'll check them in descending priority if needed,
        #    but from your specification, it seems each condition is distinct.
        if re.search(r'brokerage transfer', desc):
            return "Brokerage Transfer"
        if re.search(r'taxes and cess', desc):
            return "Bank Charges"
        if re.search(r'billing invoice paid', desc):
            return "Bank Charges"
        if re.search(r'outgoing', desc):
            return "Payment"

        # Default (no match)
        return ""

    # Apply category assignment
    df["Category"] = df.apply(assign_category, axis=1)

    # Create an output file path
    # You can choose to always output CSV or match the input file extension.
    # Here, we'll just create a new CSV regardless.
    base, _ = os.path.splitext(filepath)
    output_path = base + "_processed.csv"
    df.to_csv(output_path, index=False)

    return output_path
