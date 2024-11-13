from flask import Flask, render_template_string, request, redirect, url_for, jsonify, send_file
import threading
import webbrowser
import os
import pandas as pd
import io
import extract_msg  # To read .msg files
from bs4 import BeautifulSoup
import openpyxl
# Importing the backend functions
from poc_backend import process_bajaj_allianz_life_insurance,process_relaince_general_insurance_co, process_hdfc_ergo_insurance, process_bajaj_allianz_insurance,process_tata_aig_insurance,process_royal_sundaram_general_insurance,process_raheja_general_insurance,process_godigit_general_insurance,proess_acko_general_insurance,process_sbi_general_insurance,process_liberty_general_insurance,process_cholamandalam_general_insurance,process_icici_prudential_life_insurance,process_zuna_general_insurance, process_universal_sampo_insurance,process_kotak_mahindra_insurance,process_shriram_general_insurance,process_hdfc_life_insurance_co,process_star_health_insurer,read_lookup_files,process_icici_lombard_insurance, process_new_india_assurance,process_oriental_insurance_co, process_united_india_insurance,process_tata_aia_insurance
from datetime import datetime
from urllib.parse import unquote_plus

app = Flask(__name__)

# Global variable to hold the attachment data and mappings between requests
file_attachment = None
default_mappings = {}

login_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>Login</title>
    <style>
      .login-container {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100vh;
        background: linear-gradient(180deg, #004080, #c8c8ff);
      }
      .login-form {
        background: white;
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        width: 100%;
        max-width: 400px;
      }
      .logo {
        width: 150px;
        display: block;
        margin: 0 auto 1rem;
      }
      input {
        width: 100%;
      }
      .alert {
        margin-top: 1rem;
      }
    </style>
  </head>
  <body>
    <div class="login-container">
      <form class="login-form" method="post">
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Marsh McLennan Logo" class="logo">
        <h2 class="text-center">Login Page</h2>
        {% if error %}
          <div class="alert alert-danger" role="alert">
            {{ error }}
          </div>
        {% endif %}
        <div class="form-group">
          <label for="username">Username:</label>
          <input type="text" id="username" name="username" class="form-control">
        </div>
        <div class="form-group">
          <label for="password">Password:</label>
          <input type="password" id="password" name="password" class="form-control">
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary">Login</button>
        </div>
      </form>
    </div>
  </body>
</html>
"""

search_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css" rel="stylesheet" />
    <title>Select Email File</title>
    <style>
      .search-container {
        display: flex;
        justify-content: center;
        align-items: center; /* Center alignment */
        height: 100vh;
        background: linear-gradient(180deg, #004080, #c8c8ff);
      }
      .search-form {
        background: white;
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        width: 100%;
        max-width: 600px;
        text-align: center;
      }
      .logo {
        width: 150px;
        margin: 0 auto 1rem;
      }
      .select2-container .select2-selection--single {
        height: 40px;
      }
      .select2-selection__rendered {
        line-height: 38px;
      }
      .select2-selection__arrow {
        height: 38px;
      }
    </style>
  </head>
  <body>
    <div class="search-container">
      <form class="search-form" method="post">
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Marsh McLennan Logo" class="logo">
        <h2 class="text-center">Select Email File</h2>
        <div class="form-group">
          <label for="email_file">Select Email:</label>
          <select id="email_file" name="email_file" class="form-control">
            {% for file in email_files %}
              <option value="{{ file }}">{{ file }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary">Process Email</button>
        </div>
      </form>
    </div>
    <script src="https://code.jquery.com/jquery-3.5.1.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/js/select2.min.js"></script>
    <script>
      $(document).ready(function() {
        $('#email_file').select2({
          placeholder: 'Select an email',
          width: '100%',
          dropdownAutoWidth: true,
          dropdownCssClass: 'bigdrop',
          maximumSelectionLength: 1,
        });
      });
    </script>
  </body>
</html>
"""

email_preview_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>Email Preview</title>
    <style>
      .preview-container {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100vh;
        background: linear-gradient(180deg, #004080, #c8c8ff);
        overflow: auto;
      }
      .preview-form {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        width: 100%;
        max-width: 1200px;  /* Increased width */
        max-height: 80vh;
        overflow: auto;
      }
      .email-body-container {
        max-height: 60vh;
        overflow-y: auto;
        margin-bottom: 1rem;
      }
      table {
        width: 100%;
        border-collapse: collapse;
      }
      table, th, td {
        border: 1px solid black;
      }
      th, td {
        padding: 8px;
        text-align: left;
      }
      .alert {
        margin-bottom: 1rem;
      }
    </style>
  </head>
  <body>
    <div class="preview-container">
      <div class="preview-form">
        <h2 class="text-center">Email Preview</h2>
        {% if already_processed == 'yes' %}
          <div class="alert alert-warning text-center" role="alert">
            This email has already been processed. Are you sure you want to reprocess it?
          </div>
        {% endif %}
        <div class="email-body-container">
          <div>{{ email_body|safe }}</div>
        </div>
        <div class="text-center">
          <button class="btn btn-success" id="yesButton">Yes</button>
          <button class="btn btn-danger" id="noButton">No</button>
          <button class="btn btn-secondary" onclick="window.history.back()">Cancel</button>
        </div>
      </div>
    </div>
    <script>
      document.getElementById("yesButton").onclick = function() {
        window.location.href = '/select_insurer?file_name={{ file_name }}&subject={{ subject }}';
      };
      document.getElementById("noButton").onclick = function() {
        alert('Please search for another email.');
        window.history.back();
      };
    </script>
  </body>
</html>
"""

dropdown_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css" rel="stylesheet" />
    <title>Select Insurer</title>
    <style>
      .dropdown-container {
        display: flex;
        justify-content: center;
        align-items: center;  /* Center alignment */
        height: 100vh;
        background: linear-gradient(180deg, #004080, #c8c8ff);
      }
      .dropdown-form {
        background: white;
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        width: 100%;
        max-width: 600px;
      }
      .select2-container .select2-selection--single {
        height: 40px;
      }
      .select2-selection__rendered {
        line-height: 38px;
      }
      .select2-selection__arrow {
        height: 38px;
      }
    </style>
  </head>
  <body>
    <div class="dropdown-container">
      <form class="dropdown-form" method="post">
        <h2 class="text-center">Select Insurer</h2>
        <div class="form-group">
          <label for="insurer">Insurer Name:</label>
          <select id="insurer" name="insurer" class="form-control">
            {% for insurer in insurers %}
              <option value="{{ insurer }}">{{ insurer }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary">Process</button>
        </div>
      </form>
    </div>
    <script src="https://code.jquery.com/jquery-3.5.1.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/js/select2.min.js"></script>
    <script>
      $(document).ready(function() {
        $('#insurer').select2({
          placeholder: 'Select an insurer',
          width: '100%',
          dropdownAutoWidth: true,
          dropdownCssClass: 'bigdrop',
          maximumSelectionLength: 1,
        });
      });
    </script>
  </body>
</html>
"""

mapping_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link
      href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"
      rel="stylesheet"
    >
    <title>Column Mapping</title>
    <style>
      .mapping-container {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100vh;
        background: linear-gradient(180deg, #004080, #c8c8ff);
      }
      .mapping-form {
        background: white;
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
        width: 100%;
        max-width: 600px;
      }
      .mapping-form-content {
        max-height: 60vh; /* Adjust as needed */
        overflow-y: auto; /* Add vertical scrollbar */
      }
      .constant-label {
        background-color: #e9ecef;
        padding: 0.5rem;
        border-radius: 5px;
        display: inline-block;
        width: 100%;
        text-align: left;
      }
    </style>
  </head>
  <body>
    <div class="mapping-container">
      <form class="mapping-form" method="post">
        <h2 class="text-center">Edit Column Mapping</h2>
        <div class="mapping-form-content">
          <div class="form-group">
            {% for mapping in mappings %}
            <div class="form-group">
              <label>{{ mapping.source_col }}:</label>
              {% if mapping.editable %}
                <select id="mapping_{{ loop.index }}" name="mapping_{{ loop.index }}" class="form-control">
                  <option value="" disabled selected>Select an option</option>
                  {% for col in all_columns %}
                  <option value="{{ col }}" {% if col == mapping.dest_col %}selected{% endif %}>{{ col }}</option>
                  {% endfor %}
                </select>
              {% else %}
                <span class="constant-label">{{ mapping.dest_col }}</span>
                <input type="hidden" name="mapping_{{ loop.index }}" value="{{ mapping.dest_col }}">
              {% endif %}
            </div>
            {% endfor %}
          </div>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary">Process</button>
        </div>
      </form>
    </div>
  </body>
</html>
"""

@app.route('/', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username == 'priya' and password == 'nihar21':
            return redirect(url_for('search_email'))
        elif username == 'ramkrishna' and password == 'nihar21':
            return redirect(url_for('search_email'))
        else:
            error = "Are you an intruder? If not, enter the correct password."
    return render_template_string(login_page, error=error)

@app.route('/search', methods=['GET', 'POST'])
def search_email():
    # Paths to the email folders
    email_folder_path = r'\\?\UNC\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Email Received'
    processed_folder_path = r'\\?\UNC\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\processed_files'
    os.makedirs(processed_folder_path, exist_ok=True)

    # Path to the log file
    log_file_path = os.path.join(processed_folder_path, 'processed_emails.csv')

    # Get list of .msg files in the email folder
    email_files = [f for f in os.listdir(email_folder_path) if f.endswith('.msg')]

    # Read the log file to get list of processed emails
    if os.path.exists(log_file_path):
        processed_emails_df = pd.read_csv(log_file_path)
        processed_emails = processed_emails_df['email_name'].tolist()
    else:
        processed_emails = []

    if request.method == 'POST':
        selected_file = request.form['email_file']

        # Check if the selected email has been processed
        if selected_file in processed_emails:
            # Redirect to preview with a warning
            return redirect(url_for('preview_email', file_name=selected_file, already_processed='yes'))
        else:
            return redirect(url_for('preview_email', file_name=selected_file))

    return render_template_string(search_page, email_files=email_files)

@app.route('/preview_email', methods=['GET'])
def preview_email():
    file_name = request.args.get('file_name')
    file_name = unquote_plus(file_name)  # Decode the URL-encoded file name
    already_processed = request.args.get('already_processed', 'no')
    email_folder_path = r'\\?\UNC\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Email Received'
    file_path = os.path.join(email_folder_path, file_name)


    try:
        # Open and read the .msg file
        msg = extract_msg.Message(file_path)
        email_subject = msg.subject  # Extract the subject

        global email_body
        email_body = msg.htmlBody  # Use the raw HTML body if available

        if not email_body:
            # Fall back to plain text if HTML body isn't available
            email_body = msg.body.replace('\n', '<br>')  # Use line breaks for plain text

        # Extract attachment if it exists
        global file_attachment
        file_attachment = None  # Reset the file attachment
        global file_path_n
        file_path_n = None  # Reset the file attachment

        for attachment in msg.attachments:
            # Save the attachment file
            attachment_filename = os.path.join(email_folder_path, attachment.longFilename)
            with open(attachment_filename, 'wb') as f:
                f.write(attachment.data)
            file_attachment = attachment_filename  # Set the file_attachment to the saved file path
            file_path_n = attachment_filename

            # Break after saving the first attachment
            break

        # If no attachment is found, proceed to display the email
        if file_attachment is None:
            print("No valid attachment found.")

        # Render the email preview with the raw HTML body (no parsing/modification)
        return render_template_string(
            email_preview_page,
            email_body=email_body,
            file_name=file_name,
            subject=email_subject,
            already_processed=already_processed  # Pass the flag to the template
        )

    except Exception as e:
        return jsonify({'message': f"Error processing .msg file: {str(e)}"}), 400

@app.route('/select_insurer', methods=['GET', 'POST'])
def select_insurer():
    global default_mappings, file_attachment

    subject = request.args.get('subject')  # Get the subject
    file_name = request.args.get('file_name')

    # Read the Insurer_Names.xlsx file
    insurer_file_path = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files\Insurer_Names.xlsx'
    insurer_df = pd.read_excel(insurer_file_path)

    # Extract the insurer names for the dropdown
    insurers = insurer_df['Insurer_Name'].tolist()

    if request.method == 'POST':
        selected_insurer = request.form['insurer'].strip()
        function_name = insurer_df.loc[insurer_df['Insurer_Name'] == selected_insurer, 'Function to process'].values[0]

        # Define default mappings based on the selected insurer
        if selected_insurer == 'The New India Assurance Co':
            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function

            default_mappings = {
                'Policy Number': 'Policy No.',
                'Endorsement Number': 'Endorsement No.',
                'LOB Code': 'Risk',
                'Insured Name': 'Client Name',
                'Policy Inception Date': 'Policy Start Date',
                'Policy Expiry Date': 'Policy End Date',
                'Premium': 'Premium',
                'Brokerage': 'Brokerage',
                'Insured Type': 'Policy Type'
            }
        elif selected_insurer == 'The Oriental Insurance Co':
            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function

            default_mappings = {
                'Insured Name': 'Client Name',
                'POLICY NO': 'Policy No.',
                'ENDT NO': 'Endorsement No.',
                'Start Date': 'Policy Start Date',
                'Expiry Date': 'Policy End Date',
                'Premium': 'Premium',
                'Comm': 'Brokerage'
            }
        elif selected_insurer == 'United India Insurance':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {

                'Risk name': 'Risk',
                'POLICY_NUMBER': 'Policy No.',
                'INSURED_NAME': 'Client Name',
                'POLICY_EXPIRY_DATE': 'Policy End Date',
                'TO_DATE': 'Policy Start Date',
                'ELG_PREMIUM_AMOUNT': 'Premium',
                'Commission': 'Brokerage'

            }
        elif selected_insurer == 'Tata AIA Insurance':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {

                'Segment': 'Risk',
                'Reference key 1': 'Policy No.',
                'INSURED_NAME': 'Client Name',
                'Entry Date': 'Policy End Date',
                'Start Date': 'Policy Start Date',
                'Premium': 'Premium',
                'Comm': 'Brokerage'

            }
        elif selected_insurer == 'ICICI Lombard General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {

                'PREMIA PRODUCT CODE': 'Risk',
                'POLICY_NO': 'Policy No.',
                'INSURED_NAME': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'POLICY_START_DATE': 'Policy Start Date',
                'ACTUAL PREMIUM': 'Premium',
                'BROKERAGE AS PER COST': 'Brokerage',
                'POLICY_TYPE': 'Income category'
            }
        elif selected_insurer == 'Star Health Insurance':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {

                'PRODUCT_NAME': 'Risk',
                'POL_NUMBER_TXT': 'Policy No.',
                'INSURED_CUSTOMER_NAME': 'Client Name',
                'POLICY_TO_DATE': 'Policy End Date',
                'POLICY_FROM_DATE': 'Policy Start Date',
                'PREMIUM_FOR_PAYOUTS': 'Premium',
                'ACTUAL_COMMISSION': 'Brokerage',
                'BUSINESS_TYPE': 'Income category',
                '':'Branch'
            }
        elif selected_insurer == 'Hdfc Life Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {

                'riskn': 'Risk',
                'Policy No': 'Policy No.',
                'Client Name ': 'Client Name',
                'POLICY_TO_DATE': 'Policy End Date',
                'RISK_COMMENCEMENT_DATE': 'Policy Start Date',
                'Premium amt': 'Premium',
                ' ORIGAMT ': 'Brokerage',
                'Branchn': 'Branch',
                'Commission Type': 'Income category'
            }
        elif selected_insurer == 'Shriram General Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {

                'S Proddesc': 'Risk',
                'D_POLL': 'Policy No.',
                'S Insuredname': 'Client Name',
                'S Doe': 'Policy End Date',
                'S Doi': 'Policy Start Date',
                'GROSS': 'Premium',
                'TOTAL COMM': 'Brokerage',
                'S Divisionname': 'Branch',
                'S Fresh Renewal': 'Income category'
            }
        elif selected_insurer == 'Kotak Mahindra General Insurance Company':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'PRODUCT NAME': 'Risk',
                'POLICY NO': 'Policy No.',
                'CUSTOMER NAME': 'Client Name',
                'POLICY EXPIRY DATE': 'Policy End Date',
                'POLICY INCEPTION DATE': 'Policy Start Date',
                'OUR SHARE OF PREMIUM GWP': 'Premium',
                'TOTAL COMM': 'Brokerage',
                'BRANCH NAME': 'Branch',
                'BUSINESS TYPE': 'Income category',
                'Transaction Type':'P & L JV',
                'Endorsement No':'Endorsement No.'
            }
        elif selected_insurer == 'Universal Sampo Insurance':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'PRODUCTNAME': 'Risk',
                'TXT_POLICY_NO_CHAR': 'Policy No.',
                'TXT_CUSTOMER_NAME': 'Client Name',
                'POLICY END DATE': 'Policy End Date',
                'RISK START DATE': 'Policy Start Date',
                'Commissionable premium': 'Premium',
                'Payable ': 'Brokerage',
                'BRANCH NAME': 'Branch',
                'TXT_BUSINESS_TYPE': 'Income category'
            }
        elif selected_insurer == 'Zuno General Insurance Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'PRCTNAME': 'Risk',
                'Policy No.': 'Policy No.',
                'Insured Name': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'Transaction Type':'P & L JV',
                'Commissionable premium': 'Premium',
                'Commission/Brokerage': 'Brokerage',
                'Servicing Branch': 'Branch',
                'Business Type': 'Income category',
                'Endorsement No.':'Endorsement No.'

            }
        elif selected_insurer == 'ICICI Prudential Life Insurance Co Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Code': 'Risk',
                'Policy No': 'Policy No.',
                'Policy Holder Name': 'Client Name',
                'Commission Cycle': 'Policy End Date',
                'Issuance Date': 'Policy Start Date',
                'Premium': 'Premium',
                'Remuneration Amount': 'Brokerage',
                'From State': 'Branch',
                'Nature': 'Income category'
            }

        elif selected_insurer == 'Cholamandalam General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Description': 'Risk',
                'POLICY_NO': 'Policy No.',
                'CLIENT NAME': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'POLICY_START_DATE': 'Policy Start Date',
                ' GWP': 'Premium',
                'TOTAL IRDA ': 'Brokerage',
                'Branch Name': 'Branch',
                'Business Type': 'Income category',
                'Transaction Type':'P & L JV',
                'Endorsement No.':'Endorsement No.'
                    }
        elif selected_insurer == 'Liberty Videocon General Insurance Co. Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Description': 'Risk',
                'PolicyNo From System pasted from system': 'Policy No.',
                'CLIENT NAME': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'POLICY_START_DATE': 'Policy Start Date',
                'GWP': 'Premium',
                'TOTAL IRDA': 'Brokerage',
                'Transaction Type':'P & L JV',                
                'BUSINESS TYPE': 'Income category',
                'Branch Name': 'Branch',
                'Endorsement No.': 'Endorsement No.'
                    }
        elif selected_insurer == 'SBI General Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Description': 'Risk',
                'Policy No': 'Policy No.',
                'Insured Name': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'Gross Written Premium': 'Premium',
                'Total Commission': 'Brokerage',
                'SBIGICLBRANCHNAME': 'Branch',
                'Endorsement No': 'Endorsement No.'
                    }
        elif selected_insurer == 'Acko General Insurance Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product': 'Risk',
                'Invoice/Endorsement Number - July 24': 'Policy No.',
                'Partner': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'RSD': 'Policy Start Date',
                'Premium': 'Premium',
                'Brokerage': 'Brokerage',
                'Fresh/Renewal': 'Income category',
                'Endorsement No.': 'Endorsement No.',
                'Master Policy Number': 'ASP Practice'
                    }

        elif selected_insurer == 'GoDigit General Insurance Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'product name': 'Risk',
                'policy number': 'Policy No.',
                'policy holder': 'Client Name',
                'risk exp date': 'Policy End Date',
                'risk inC date': 'Policy Start Date',
                'net premium coll': 'Premium',
                'office name': 'Branch',
                'state': 'Branch2',
                'IRDA_AMT': 'Brokerage',
                'policy type': 'Income category',
                'endorsement ind': 'Endorsement No.'
                                                        }
        elif selected_insurer == 'Raheja Qbe General Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Contract type': 'Risk1',
                'LoB': 'Risk2',
                'Business ': 'Risk3',
                'PolicyNumber': 'Policy No.',
                'SURNAME': 'Client Name',
                'Givname': 'Client Name2',
                'Expiry date': 'Policy End Date',
                'Start Date': 'Policy Start Date',
                'Premium': 'Premium',
                'City': 'Branch',
                'Commission': 'Brokerage',
                'TypeofTra nsaction': 'Income category',
                'TypeofTransaction':'P & L JV',                
                'Endorsement No.': 'Endorsement No.'
                                                        }
        elif selected_insurer == 'Royal Sundaram General Insurance Co Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'PRODUCT sds2': 'Risk',
                'POLICY ID': 'Policy No.',
                'CLIENT NAME': 'Client Name',
                'risk exp date': 'Policy End Date',
                'PREMIUM EFFECTIVE DATE': 'Policy Start Date',
                'GROSS WRITTEN PREMIUM': 'Premium',
                'POLICY BRANCH NAME': 'Branch',
                'COMMISSION AMOUNT': 'Brokerage',
                'TYPE OF BUSINESS': 'Income category',
                'Endorsement No.': 'Endorsement No.'
                                                        }
        elif selected_insurer == 'Tata AIG General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product name': 'Risk',
                'Policy no': 'Policy No.',
                'ClientName': 'Client Name',
                'Policy_end_date': 'Policy End Date',
                'Policy_start_date': 'Policy Start Date',
                'Branch': 'Branch',
                'Business_type': 'Income category',
                'Endorsement No.': 'Endorsement No.',
                'Premium': 'Premium1',
                'TP_premium': 'Premium2',
                'Terror commision': 'Premium3',
                'reward': 'Brokerage1',
                'TP_commission': 'Brokerage2',
                'Terrorism AMOUNT': 'Brokerage3'

                                                        }
        elif selected_insurer == 'Bajaj Allianz General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product name': 'Risk',
                'POLICY_REFERENCE': 'Policy No.',
                'CUSTOMER NAME': 'Client Name',
                '': 'Policy End Date',
                'Policy Date': 'Policy Start Date',
                'Branch': 'Branch',
                'MOVEMENT': 'Income category',
                'MOVEMENT': 'P & L JV',
                'Endorsement No.': 'Endorsement No.',
                'Premium': 'Premium1',
                'TP_premium': 'Premium2',
                'Terror commision': 'Premium3',
                'reward': 'Brokerage1',
                'TP_commission': 'Brokerage2',
                'Terrorism AMOUNT': 'Brokerage3'
                                                        }
        elif selected_insurer == 'Bajaj Allianz Life Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product name': 'Risk',
                'POLICY_REFERENCE': 'Policy No.',
                'CUSTOMER NAME': 'Client Name',
                '': 'Policy End Date',
                'Policy Date': 'Policy Start Date',
                'Branch': 'Branch',
                'MOVEMENT': 'Income category',
                'MOVEMENT': 'P & L JV',
                'Endorsement No.': 'Endorsement No.',
                'Premium': 'Premium1',
                'TP_premium': 'Premium2',
                'Terror commision': 'Premium3',
                'reward': 'Brokerage1',
                'TP_commission': 'Brokerage2',
                'Terrorism AMOUNT': 'Brokerage3',
                'MASTER_POLICY_NO': 'ASP Practice'
                                                        }
        elif selected_insurer == 'Hdfc Ergo General Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Type_of_Policy': 'Risk',
                'Policy_Num': 'Policy No.',
                'Customer_Name': 'Client Name',
                'Start_Dt': 'Policy End Date',
                'Expiry_Dt': 'Policy Start Date',
                'SRC_State': 'Branch',
                'Business_Type': 'Income category',
                'Endorsement_Type': 'P & L JV',
                'Endorsement_Num': 'Endorsement No.',
                'OD': 'Premium1',
                'TP': 'Premium2',
                'Terror commision': 'Premium3',
                'COMMISSION_OD_AMT': 'Brokerage1',
                'COMMISSION_TP_AMT': 'Brokerage2',
                'Terrorism AMOUNT': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Reliance General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'ProductCode': 'Risk',
                'PolicyNumber': 'Policy No.',
                'InsuredName': 'Client Name',
                'Policy Start Date': 'Policy End Date',
                'Policy End Date': 'Policy Start Date',
                'SRC_State': 'Branch',
                'Endorsement No.': 'Endorsement No.',
                'PremiumAmount': 'Premium1',
                'TPPremiumAmount': 'Premium2',
                'TerrorismPremiumAmount': 'Premium3',
                'FinalIRDAComm': 'Brokerage1',
                'FinalTPComm': 'Brokerage2',
                'FinalTerrorism_': 'Brokerage3'
                                                      }
                                
        else:
            # Ensure there's a fallback for other insurers
            default_mappings = {
                'Column1': 'Value1',
                'Column2': 'Value2'
            }

        # Pass selected_insurer to edit_mappings via URL parameters
        return redirect(url_for('edit_mappings', function_name=function_name, subject=subject, selected_insurer=selected_insurer, file_name=file_name))

    return render_template_string(dropdown_page, insurers=insurers)

@app.route('/edit_mappings', methods=['GET', 'POST'])
def edit_mappings():
    global default_mappings, file_attachment

    function_name = request.args.get('function_name')
    subject = request.args.get('subject')
    selected_insurer = request.args.get('selected_insurer')
    file_name = request.args.get('file_name')

    # Map insurers to header rows
    header_rows = {
        'The New India Assurance Co': 14,
        'The Oriental Insurance Co': 0,
        'United India Insurance':0,
        'Tata AIA Insurance':0,
        'ICICI Lombard General Insurance Co. Ltd.':0,
        'Star Health Insurance':0,
        'Hdfc Life Insurance Co. Ltd.':0,
        'Shriram General Insurance Company Limited':0,
        'Kotak Mahindra General Insurance Company':0,
        'Universal Sampo Insurance':0,
        'Zuno General Insurance Limited':0,
        'ICICI Prudential Life Insurance Co Ltd':0,
        'Cholamandalam General Insurance Co. Ltd.':0,
        'Liberty Videocon General Insurance Co. Ltd':0,
        'SBI General Insurance Company Limited':0,
        'Acko General Insurance Limited':3,
        'GoDigit General Insurance Limited':0,
        'Royal Sundaram General Insurance Co Ltd':0,
        'Raheja Qbe General Insurance Company Limited':0,
        'Tata AIG General Insurance Co. Ltd.':0,
        'Bajaj Allianz General Insurance Co. Ltd.':0,
        'Hdfc Ergo General Insurance Company Limited':0,
        'Reliance General Insurance Co. Ltd.':0,
        'Bajaj Allianz Life Insurance Company Limited':0
        # Add more insurers as needed
    }

    header_row = header_rows.get(selected_insurer, 0)  # Default to 0 if not specified

    # Read the file attachment using the appropriate header
# Read the file attachment using the appropriate header
    if isinstance(file_attachment, str) and os.path.exists(file_attachment):
        try:
            if file_attachment.endswith('.xlsx'):
                try:
                    # Try reading with default engine first
                    file_df = pd.read_excel(file_attachment, header=header_row)
                except Exception as e:
                    # If any error occurs, try reading with calamine engine
                    file_df = pd.read_excel(file_attachment, header=header_row, engine='calamine')
            elif file_attachment.endswith('.xls'):
                file_df = pd.read_excel(file_attachment, engine='xlrd', header=header_row)
            elif file_attachment.endswith('.xlsb'):
                file_df = pd.read_excel(file_attachment, engine='pyxlsb', header=header_row)
            elif file_attachment.endswith('.csv'):
                file_df = pd.read_csv(file_attachment, header=header_row)
            elif file_attachment.endswith('.ods'):
                file_df = pd.read_excel(file_attachment, engine='odf', header=header_row)
            elif file_attachment.endswith('.txt'):
                file_df = pd.read_csv(file_attachment, delimiter='\t', header=header_row)
            else:
                return jsonify({'message': 'Unsupported file type or no valid attachment found.'}), 400
        except Exception as e:
            return jsonify({'message': f'Error reading the file: {str(e)}'}), 400
    else:
        return jsonify({'message': 'No valid file attachment found.'}), 400

    # Extract column names from the file
    # Extract column names from the file
    all_columns = [str(col).strip() for col in file_df.columns.tolist() if not str(col).startswith('Unnamed')]
    all_columns.append('blank')
    all_columns.append('blank1')
    all_columns.append('blank2')
    all_columns.append('blank3')
    all_columns.append('blank4')
    all_columns.append('blank5')

    if request.method == 'POST':
        # Build the mappings in the format:
        # {'Dropdown Selected Column': 'Fixed Label'}
        edited_mappings = {request.form.get(f'mapping_{i+1}'): fixed_col for i, fixed_col in enumerate(default_mappings.values())}

        # Ensure no missing or None mappings are sent
        if not all(edited_mappings.keys()):
            return jsonify({'message': 'Some mappings are missing. Please select all mappings.'}), 400

        # Pass the mappings and selected_insurer as part of the URL parameters
        return redirect(url_for('process_data', function_name=function_name, subject=subject, mappings=edited_mappings, selected_insurer=selected_insurer, file_name=file_name))

    # Prepare the mappings for form rendering
    mappings_for_form = []
    for fixed_col, source_col in default_mappings.items():
        mappings_for_form.append({
            'source_col': source_col,
            'dest_col': fixed_col,
            'editable': fixed_col not in all_columns  # Make uneditable if the left-side value exists in the file
        })

    return render_template_string(mapping_page, mappings=mappings_for_form, all_columns=all_columns)

@app.route('/process_data', methods=['GET', 'POST'])
def process_data():
    global file_path_n

    function_name = request.args.get('function_name')
    subject = request.args.get('subject')  # Get the subject
    selected_insurer = request.args.get('selected_insurer')
    file_name = request.args.get('file_name')  # Get the email file name

    # Retrieve mappings from URL parameters and ensure they are formatted correctly
    mappings = request.args.get('mappings')

    # Debug: Print the mappings to see how they are received from the frontend
    print("Mappings Received:", mappings)

    try:
        mappings = eval(mappings)  # Convert the string representation back into a dictionary

        # Debug: Print the evaluated mappings
        print("Mappings After Eval:", mappings)
    except Exception as e:
        return jsonify({'message': f"Error parsing mappings: {str(e)}"}), 400

    print(file_path_n)

    # Check if file_attachment is a valid file path
    if isinstance(file_path_n, str) and os.path.exists(file_path_n):
        data = file_path_n  # Pass the file path directly
    else:
        return jsonify({'message': 'No valid file attachment found or the file does not exist.'}), 400

    if function_name in globals():
        process_function = globals()[function_name]

        # Read lookup files
        template_data, risk_code_data, cust_neft_data, table_3, table_4, table_5 = read_lookup_files()

        # Debug: Print before calling the backend processing function
        print(f"Calling backend function: {function_name} with mappings: {mappings}")

        # Call the backend processing function
        try:
            final_data, final_csv_path = process_function(
                data, template_data, risk_code_data, cust_neft_data,
                table_3, table_4, table_5, subject, mappings
            )
        except Exception as e:
            return jsonify({'message': f"Error processing file: {str(e)}"}), 400

        if isinstance(final_data, str):
            return jsonify({'message': f'Error processing file: {final_data}'}), 400

        # After processing, move the email and log the processing
        email_folder_path = r'\\?\UNC\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\Email Received'
        processed_folder_path = r'\\?\UNC\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\processed_files'
        os.makedirs(processed_folder_path, exist_ok=True)

        email_file_path = os.path.join(email_folder_path, file_name)
        processed_email_file_path = os.path.join(processed_folder_path, file_name)

        # Move the email file to processed_files folder
        if os.path.exists(email_file_path):
            os.replace(email_file_path, processed_email_file_path)

        # Write to log file
        log_file_path = os.path.join(processed_folder_path, 'processed_emails.csv')
        output_files = os.path.basename(final_csv_path)
        date_processed = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Create a DataFrame for the log entry
        log_entry = pd.DataFrame([{
            'email_name': file_name,
            'date_processed': date_processed,
            'output_files': output_files
        }])

        # Append to the log file
        if os.path.exists(log_file_path):
            log_df = pd.read_csv(log_file_path)
            log_df = pd.concat([log_df, log_entry], ignore_index=True)
        else:
            log_df = log_entry

        log_df.to_csv(log_file_path, index=False)

        return send_file(final_csv_path, as_attachment=True)

    return jsonify({'message': 'Invalid processing function.'}), 400

def read_tables_from_email(email_body, selected_insurer):
    print(f"read_tables_from_email was called for {selected_insurer}")

    # Define default expected columns
    default_expected_columns = {
        'table_3': {'Total', 'TotalTaxAmt', 'GST', 'TDS@10%', 'GST TDS @2%', 'NET', 'Net', 'Rem Amt', 'TDS', 'Net Amount'},
        'table_4': {'Date', 'Month', 'Year', 'Bank', 'Description of Remittance', 'Invoice No', 'Amount'},
        'table_5': {'Invoice No', 'InvoiceDate', 'Description', 'TotalTaxAmt', 'SupplierName'}
    }

    # Define overrides for specific insurers
    insurer_overrides = {
        'The New India Assurance Co': {
            'table_3': {'Total', 'GST', 'TDS@10%', 'GST TDS @2%', 'NET'},
            'table_4': {'Date', 'Month', 'Year', 'Bank'},
            'table_5': {'Invoice NO', 'InvoiceDate', 'Description', 'TotalTaxAmt', 'SupplierName', 'SupplierCode', 'SupplierState'}
        },
        'The Oriental Insurance Co': {
            'table_3': {'TotalTaxAmt', 'GST', 'TDS', 'GST TDS', 'NET'},
            'table_4': {'Date', 'Month', 'Year', 'Bank', 'Description of Remittance', 'Invoice No', 'Amount'},
            'table_5': {'Invoice NO', 'InvoiceDate', 'Description', 'TotalTaxAmt', 'SupplierName', 'SupplierCode', 'SupplierState'}
        },
        # Add other insurers with specific overrides here
    }

    supported_insurers = [
        'The New India Assurance Co', 'The Oriental Insurance Co', 'United India Insurance',
        'ICICI Lombard General Insurance Co. Ltd.', 'Star Health Insurance', 'Hdfc Life Insurance Co. Ltd.',
        'Shriram General Insurance Company Limited', 'Kotak Mahindra General Insurance Company',
        'Universal Sampo Insurance', 'Zuno General Insurance Limited', 'ICICI Prudential Life Insurance Co Ltd',
        'Bajaj Allianz Life Insurance Company Limited','Reliance General Insurance Co. Ltd.','Hdfc Ergo General Insurance Company Limited','Bajaj Allianz General Insurance Co. Ltd.','Tata AIG General Insurance Co. Ltd.','Royal Sundaram General Insurance Co Ltd','Raheja Qbe General Insurance Company Limited','GoDigit General Insurance Limited','Acko General Insurance Limited','SBI General Insurance Company Limited','Cholamandalam General Insurance Co. Ltd.', 'Tata AIA Insurance','Liberty Videocon General Insurance Co. Ltd'
    ]

    if selected_insurer not in supported_insurers:
        print(f"Processing for {selected_insurer} is not supported.")
        return f"Processing for {selected_insurer} is not supported."

    # Get expected columns for the selected insurer
    expected_columns = default_expected_columns.copy()
    if selected_insurer in insurer_overrides:
        for table_key, columns in insurer_overrides[selected_insurer].items():
            expected_columns[table_key] = columns

    # Parse the email body to find tables
    soup = BeautifulSoup(email_body, 'html.parser')
    tables = soup.find_all('table')

    # Initialize DataFrames for the tables
    table_3_df = pd.DataFrame()
    table_4_df = pd.DataFrame()
    table_5_df = pd.DataFrame()

    def extract_table(table_html):
        rows = []
        for row in table_html.find_all('tr'):
            cols = row.find_all(['th', 'td'])
            cols = [ele.get_text(strip=True) for ele in cols]
            if cols:
                rows.append(cols)
        if not rows:
            return pd.DataFrame()
        # Find the maximum number of columns
        max_cols = max(len(r) for r in rows)
        # Pad rows to have the same number of columns
        rows_padded = [r + [''] * (max_cols - len(r)) for r in rows]
        headers = rows_padded[0]
        data_rows = rows_padded[1:]
        return pd.DataFrame(data_rows, columns=headers)

    # Debug: Print the number of tables found
    print(f"Found {len(tables)} tables in the email")

    # For each table, try to match to table_3_df, table_4_df, table_5_df
    for i, table in enumerate(tables):
        df = extract_table(table)
        if df.empty:
            continue
        df.columns = [col.strip() for col in df.columns]

        # Identify table_3_df
        if set(df.columns).intersection(expected_columns['table_3']):
            if table_3_df.empty:
                table_3_df = df
                print(f"Extracted table_3_df from table {i+1}")
                continue

        # Identify table_4_df
        if set(df.columns).intersection(expected_columns['table_4']):
            if table_4_df.empty:
                table_4_df = df
                print(f"Extracted table_4_df from table {i+1}")
                continue

        # Identify table_5_df
        if set(df.columns).intersection(expected_columns['table_5']):
            if table_5_df.empty:
                table_5_df = df
                print(f"Extracted table_5_df from table {i+1}")
                continue

    # Define the save directory
    save_dir = r'\\Mgd.mrshmc.com\ap_data\MBI2\Shared\Common - FPA\Common Controller\Common folder AP & AR\Brokerage Statement Automation\support files'
    os.makedirs(save_dir, exist_ok=True)

    # Save the tables
    if not table_3_df.empty:
        table_3_df.to_csv(os.path.join(save_dir, 'table_3.csv'), index=False)
        print("Saved table_3.csv")
    else:
        print("table_3.csv is empty. Table not found or extracted.")

    if not table_4_df.empty:
        table_4_df.to_csv(os.path.join(save_dir, 'table_4.csv'), index=False)
        print("Saved table_4.csv")
    else:
        print("table_4.csv is empty. Table not found or extracted.")

    if not table_5_df.empty:
        table_5_df.to_csv(os.path.join(save_dir, 'table_5.csv'), index=False)
        print("Saved table_5.csv")
    else:
        print("table_5.csv is empty. Table not found or extracted.")

    return f"Tables from email read successfully for {selected_insurer}"



@app.route('/download', methods=['GET'])
def download_file():
    file_path = request.args.get('file_path')
    if file_path and os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return jsonify({'message': 'File not found or path incorrect'}), 404
def run_app():
    app.run(port=5000)

threading.Thread(target=run_app).start()
webbrowser.open('http://127.0.0.1:5000')
