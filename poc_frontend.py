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
from poc_backend import  process_aviva_insurance_co,process_national_insurance_limited,kotak_life_insurance_co,process_indialife_first_insurance, proces_aegon_life_insurance_co,process_shriram_life_insurance_co, process_edelweiss_tokio_life_insurance,process_niva_bupa_health_insurance,process_go_digit_life_insurance,process_pnb_metlife_insurance,process_pramerica_life_insurance,process_max_life_insurance,process_aditya_birla_sun_life ,process_sbi_life_insurance_co,process_iffco_tokyo_insurer,process_star_india_diachi,process_future_generalli_life_insurance,process_aditya_insurance_co,process_manipal_health_insurance_company,process_generali_india_insurance_company,process_magma_hdi_general_insurance_company,process_care_health_insurance_limited,process_bajaj_allianz_life_insurance,process_relaince_general_insurance_co, process_hdfc_ergo_insurance, process_bajaj_allianz_insurance,process_tata_aig_insurance,process_royal_sundaram_general_insurance,process_raheja_general_insurance,process_godigit_general_insurance,proess_acko_general_insurance,process_sbi_general_insurance,process_liberty_general_insurance,process_cholamandalam_general_insurance,process_icici_prudential_life_insurance,process_zuna_general_insurance, process_universal_sampo_insurance,process_kotak_mahindra_insurance,process_shriram_general_insurance,process_hdfc_life_insurance_co,process_star_health_insurer,read_lookup_files,process_icici_lombard_insurance, process_new_india_assurance,process_oriental_insurance_co, process_united_india_insurance,process_tata_aia_insurance
from datetime import datetime
from urllib.parse import unquote_plus
import xlrd
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
    <!-- Bootstrap CSS -->
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>StatementFlow - Login</title>
    <style>
      body {
        background: linear-gradient(135deg, #004080, #c8c8ff);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #333;
        display: flex;
        flex-direction: column;
        min-height: 100vh;
      }
      .content-wrapper {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
      }
      .login-form {
        background: rgba(255, 255, 255, 0.95);
        padding: 4rem 5rem;
        border-radius: 16px;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 600px;
        transition: transform 0.3s ease;
      }
      .login-form:hover {
        transform: translateY(-5px);
      }
      .logo {
        width: 140px;
        display: block;
        margin: 0 auto 1.5rem;
        border-radius: 50%;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
      }
      .app-title {
        text-align: center;
        font-size: 2rem;
        margin-bottom: 1.5rem;
        color: #004080;
        font-weight: bold;
        letter-spacing: 2px;
      }
      .footer {
        background-color: #003366;
        color: #fff;
        text-align: center;
        padding: 1rem 0;
        font-size: 1rem;
      }
      .alert {
        margin-top: 1rem;
        font-size: 1.1rem;
      }
      label {
        font-size: 1.1rem;
        font-weight: 600;
      }
      input:focus {
        box-shadow: none;
        border-color: #004080;
      }
      .btn-primary {
        background-color: #004080;
        border: none;
        transition: background-color 0.3s;
        letter-spacing: 1px;
        font-size: 1.1rem;
        padding: 0.75rem;
      }
      .btn-primary:hover {
        background-color: #003366;
      }
    </style>
  </head>
  <body>
    <div class="content-wrapper">
      <form class="login-form" method="post">
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Marsh McLennan Logo" class="logo">
        <div class="app-title">StatementFlow</div>
        <h2 class="text-center mb-4">Login</h2>
        {% if error %}
          <div class="alert alert-danger" role="alert">
            {{ error }}
          </div>
        {% endif %}
        <div class="form-group">
          <label for="username">Username:</label>
          <input type="text" id="username" name="username" class="form-control" required>
        </div>
        <div class="form-group">
          <label for="password">Password:</label>
          <input type="password" id="password" name="password" class="form-control" required>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary btn-block">Login</button>
        </div>
      </form>
    </div>
    <footer class="footer">
      &copy; {{ current_year }} StatementFlow. All rights reserved. Powered by Relentless.AI
    </footer>
    <!-- Optional JavaScript -->
    <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js"></script>
  </body>
</html>
"""

search_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <!-- Bootstrap CSS -->
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <!-- Select2 CSS -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css" rel="stylesheet" />
    <title>StatementFlow - Select Email</title>
    <style>
      body {
        background: linear-gradient(135deg, #004080, #c8c8ff);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #333;
        display: flex;
        flex-direction: column;
        min-height: 100vh;
      }
      .content-wrapper {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
      }
      .search-form {
        background: rgba(255, 255, 255, 0.95);
        padding: 4rem 5rem;
        border-radius: 16px;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 800px;
        transition: transform 0.3s ease;
      }
      .search-form:hover {
        transform: translateY(-5px);
      }
      .logo {
        width: 140px;
        display: block;
        margin: 0 auto 1.5rem;
        border-radius: 50%;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
      }
      .app-title {
        text-align: center;
        font-size: 2rem;
        margin-bottom: 1.5rem;
        color: #004080;
        font-weight: bold;
        letter-spacing: 2px;
      }
      .footer {
        background-color: #003366;
        color: #fff;
        text-align: center;
        padding: 1rem 0;
        font-size: 1rem;
      }
      .select2-container--default .select2-selection--single {
        height: 50px;
        border-radius: 8px;
        border: 1px solid #ced4da;
      }
      .select2-selection__rendered {
        line-height: 50px;
        font-size: 1.1rem;
      }
      .select2-selection__arrow {
        height: 50px;
      }
      .btn-primary {
        background-color: #004080;
        border: none;
        transition: background-color 0.3s;
        letter-spacing: 1px;
        font-size: 1.1rem;
        padding: 0.75rem;
      }
      .btn-primary:hover {
        background-color: #003366;
      }
      label {
        font-size: 1.1rem;
        font-weight: 600;
      }
    </style>
  </head>
  <body>
    <div class="content-wrapper">
      <form class="search-form" method="post">
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Marsh McLennan Logo" class="logo">
        <div class="app-title">StatementFlow</div>
        <h2 class="text-center mb-4">Select Email File</h2>
        <div class="form-group">
          <label for="email_file">Select Email:</label>
          <select id="email_file" name="email_file" class="form-control" required>
            {% for file in email_files %}
              <option value="{{ file }}">{{ file }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary btn-block">Process Email</button>
        </div>
      </form>
    </div>
    <footer class="footer">
      &copy; {{ current_year }} StatementFlow. All rights reserved. Powered by Relentless.AI
    </footer>
    <!-- jQuery -->
    <script src="https://code.jquery.com/jquery-3.5.1.min.js"></script>
    <!-- Select2 JS -->
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
    <!-- Bootstrap CSS -->
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>StatementFlow - Email Preview</title>
    <style>
      body {
        background: linear-gradient(135deg, #004080, #c8c8ff);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #333;
        display: flex;
        flex-direction: column;
        min-height: 100vh;
      }
      .content-wrapper {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
      }
      .preview-form {
        background: rgba(255, 255, 255, 0.95);
        padding: 4rem 5rem;
        border-radius: 16px;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 1200px;
        transition: transform 0.3s ease;
      }
      .preview-form:hover {
        transform: translateY(-5px);
      }
      .app-title {
        text-align: center;
        font-size: 2rem;
        margin-bottom: 1rem;
        color: #004080;
        font-weight: bold;
        letter-spacing: 2px;
      }
      .footer {
        background-color: #003366;
        color: #fff;
        text-align: center;
        padding: 1rem 0;
        font-size: 1rem;
      }
      .email-body-container {
        max-height: 40vh; /* Reduced height for compactness */
        overflow-y: auto;
        margin-bottom: 1.5rem;
        padding: 1rem;
        border: 1px solid #ced4da;
        border-radius: 8px;
        background-color: #f8f9fa;
        font-size: 1.1rem; /* Increased font size */
      }
      table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 1rem;
      }
      table, th, td {
        border: 1px solid #dee2e6;
      }
      th, td {
        padding: 14px;
        text-align: left;
        font-size: 1.1rem; /* Increased font size */
      }
      th {
        background-color: #e9ecef;
      }
      .btn-success, .btn-danger, .btn-secondary {
        min-width: 120px;
        margin: 0 0.5rem;
        padding: 0.75rem 1rem;
        font-size: 1.1rem;
        border-radius: 8px;
        transition: background-color 0.3s, transform 0.2s;
      }
      .btn-success {
        background-color: #28a745;
        border: none;
        color: #fff;
      }
      .btn-success:hover {
        background-color: #218838;
        transform: scale(1.05);
      }
      .btn-danger {
        background-color: #dc3545;
        border: none;
        color: #fff;
      }
      .btn-danger:hover {
        background-color: #c82333;
        transform: scale(1.05);
      }
      .btn-secondary {
        background-color: #6c757d;
        border: none;
        color: #fff;
      }
      .btn-secondary:hover {
        background-color: #5a6268;
        transform: scale(1.05);
      }
      label {
        font-size: 1.1rem;
        font-weight: 600;
      }
    </style>
  </head>
  <body>
    <div class="content-wrapper">
      <div class="preview-form">
        <div class="app-title">StatementFlow</div>
        <h2 class="text-center mb-4">Email Preview</h2>
        {% if already_processed == 'yes' %}
          <div class="alert alert-warning text-center" role="alert">
            This email has already been processed. Are you sure you want to reprocess it?
          </div>
        {% endif %}
        <div class="email-body-container">
          <div>{{ email_body|safe }}</div>
        </div>
        <div class="text-center mb-4">
          <button class="btn btn-success" id="yesButton">Yes</button>
          <button class="btn btn-danger" id="noButton">No</button>
          <button class="btn btn-secondary" onclick="window.history.back()">Cancel</button>
        </div>
      </div>
    </div>
    <footer class="footer">
      &copy; {{ current_year }} StatementFlow. All rights reserved. Powered by Relentless.AI
    </footer>
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
    <!-- Bootstrap CSS -->
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <!-- Select2 CSS -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/select2/4.0.13/css/select2.min.css" rel="stylesheet" />
    <title>StatementFlow - Select Insurer</title>
    <style>
      body {
        background: linear-gradient(135deg, #004080, #c8c8ff);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #333;
        display: flex;
        flex-direction: column;
        min-height: 100vh;
      }
      .content-wrapper {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
      }
      .dropdown-form {
        background: rgba(255, 255, 255, 0.95);
        padding: 4rem 5rem;
        border-radius: 16px;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 800px;
        transition: transform 0.3s ease;
      }
      .dropdown-form:hover {
        transform: translateY(-5px);
      }
      .logo {
        width: 140px;
        display: block;
        margin: 0 auto 1.5rem;
        border-radius: 50%;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
      }
      .app-title {
        text-align: center;
        font-size: 2rem;
        margin-bottom: 1.5rem;
        color: #004080;
        font-weight: bold;
        letter-spacing: 2px;
      }
      .footer {
        background-color: #003366;
        color: #fff;
        text-align: center;
        padding: 1rem 0;
        font-size: 1rem;
      }
      .select2-container--default .select2-selection--single {
        height: 50px;
        border-radius: 8px;
        border: 1px solid #ced4da;
      }
      .select2-selection__rendered {
        line-height: 50px;
        font-size: 1.1rem;
      }
      .select2-selection__arrow {
        height: 50px;
      }
      .btn-primary {
        background-color: #004080;
        border: none;
        transition: background-color 0.3s;
        letter-spacing: 1px;
        font-size: 1.1rem;
        padding: 0.75rem;
      }
      .btn-primary:hover {
        background-color: #003366;
      }
      label {
        font-size: 1.1rem;
        font-weight: 600;
      }
    </style>
  </head>
  <body>
    <div class="content-wrapper">
      <form class="dropdown-form" method="post">
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Marsh McLennan Logo" class="logo">
        <div class="app-title">StatementFlow</div>
        <h2 class="text-center mb-4">Select Insurer</h2>
        <div class="form-group">
          <label for="insurer">Insurer Name:</label>
          <select id="insurer" name="insurer" class="form-control" required>
            {% for insurer in insurers %}
              <option value="{{ insurer }}">{{ insurer }}</option>
            {% endfor %}
          </select>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary btn-block">Process</button>
        </div>
      </form>
    </div>
    <footer class="footer">
      &copy; {{ current_year }} StatementFlow. All rights reserved. Powered by Relentless.AI
    </footer>
    <!-- jQuery -->
    <script src="https://code.jquery.com/jquery-3.5.1.min.js"></script>
    <!-- Select2 JS -->
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
    <!-- Bootstrap CSS -->
    <link
      href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"
      rel="stylesheet"
    >
    <title>StatementFlow - Column Mapping</title>
    <style>
      body {
        background: linear-gradient(135deg, #004080, #c8c8ff);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #333;
        display: flex;
        flex-direction: column;
        min-height: 100vh;
      }
      .content-wrapper {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
      }
      .mapping-form {
        background: rgba(255, 255, 255, 0.95);
        padding: 4rem 5rem;
        border-radius: 16px;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 900px;
        transition: transform 0.3s ease;
      }
      .mapping-form:hover {
        transform: translateY(-5px);
      }
      .app-title {
        text-align: center;
        font-size: 2rem;
        margin-bottom: 1rem;
        color: #004080;
        font-weight: bold;
        letter-spacing: 2px;
      }
      .footer {
        background-color: #003366;
        color: #fff;
        text-align: center;
        padding: 1rem 0;
        font-size: 1rem;
      }
      .mapping-form-content {
        max-height: 70vh;
        overflow-y: auto;
        margin-bottom: 2rem;
        padding: 1rem;
        border: 1px solid #ced4da;
        border-radius: 8px;
        background-color: #f8f9fa;
        font-size: 1.1rem;
      }
      .constant-label {
        background-color: #e9ecef;
        padding: 0.75rem;
        border-radius: 8px;
        display: inline-block;
        width: 100%;
        text-align: left;
        font-weight: bold;
        box-shadow: inset 0 0 5px rgba(0,0,0,0.1);
      }
      .btn-primary {
        background-color: #004080;
        border: none;
        transition: background-color 0.3s;
        letter-spacing: 1px;
        font-size: 1.1rem;
        padding: 0.75rem;
      }
      .btn-primary:hover {
        background-color: #003366;
      }
      label {
        font-size: 1.1rem;
        font-weight: 600;
      }
      select.form-control {
        border-radius: 8px;
        border: 1px solid #ced4da;
      }
      .form-group.row {
        align-items: center;
      }
    </style>
  </head>
  <body>
    <div class="content-wrapper">
      <form class="mapping-form" method="post">
        <div class="app-title">StatementFlow</div>
        <h2 class="text-center mb-4">Edit Column Mapping</h2>
        <div class="mapping-form-content">
          <div class="form-group">
            {% for mapping in mappings %}
            <div class="form-group row">
              <label class="col-sm-4 col-form-label">{{ mapping.source_col }}:</label>
              <div class="col-sm-8">
                {% if mapping.editable %}
                  <select id="mapping_{{ loop.index }}" name="mapping_{{ loop.index }}" class="form-control" required>
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
            </div>
            {% endfor %}
          </div>
        </div>
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary btn-block">Process</button>
        </div>
      </form>
    </div>
    <footer class="footer">
      &copy; {{ current_year }} StatementFlow. All rights reserved. Powered by Relentless.AI
    </footer>
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
    insurer_df = pd.read_excel(insurer_file_path, engine = 'openpyxl')

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
                'dfs': 'Policy No.',
                'S Insuredname': 'Client Name',
                'S Doe': 'Policy End Date',
                'S Doi': 'Policy Start Date',
                'GROSS': 'Premium',
                'TOTAL COMM': 'Brokerage',
                'S Divisionname': 'Branch',
                'S Fresh Renewal': 'Income category',
                'fsdf':'Endorsement No.'

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
                '': 'Policy No.',
                'Partner': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'RSD': 'Policy Start Date',
                'Premium': 'Premium',
                'Brokerage': 'Brokerage',
                'Fresh/Renewal': 'Income category',
                '': 'Endorsement No.',
                '': 'ASP Practice'
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
                'Polie': 'Policy Start Date',
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
                'ewrer': 'Premium1',
                'jfa': 'Premium2',
                'nananan': 'Premium3',
                'fdmms': 'Brokerage1',
                'sdaaaa': 'Brokerage2',
                'vbk': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Reliance General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'ProductCode': 'Risk',
                'PolicyNumber': 'Policy No.',
                'InsuredName': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'SRC_State': 'Branch',
                'Endorsement No.': 'Endorsement No.',
                'dk;jfla': 'Premium1',
                'safdf': 'Premium2',
                'dasfewr': 'Premium3',
                'asdfdas': 'Brokerage1',
                'sadfa': 'Brokerage2',
                'saffdsa': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Care Health Insurance Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Type': 'Risk',
                'Policy No': 'Policy No.',
                'Customer Name': 'Client Name',
                'End date': 'Policy End Date',
                'Effective Date/Policy Start date': 'Policy Start Date',
                'State': 'Branch',
                'Business Type': 'Income Category',
                'Endorsement No': 'Endorsement No.',
                'Premium': 'Premium1',
                'TPPremiumAmount': 'Premium2',
                'TerrorismPremiumAmount': 'Premium3',
                'FinalIRDAComm': 'Brokerage1',
                'FinalTPComm': 'Brokerage2',
                'FinalTerrorism_': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Magma Hdi General Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product': 'Risk1',
                'Product LOB': 'Risk2',
                'Policy No.': 'Policy No.',
                'Insured Name': 'Client Name',
                'End date': 'Policy End Date',
                'Risk Start Date': 'Policy Start Date',
                'Location Name': 'Branch',
                'Policy+Enddt': 'Income Category',
                'Policy+Enddt': 'P & L JV',
                'Endorsement No': 'Endorsement No.',
                'Gross Written Premium': 'Premium1',
                'Base Premium(OD Premium)': 'Premium2',
                'TP/Terrorism Premium': 'Premium3',
                'Actual Commission Payable Amt': 'Brokerage1',
                'Comm/Brokerage Amount(OD)': 'Brokerage2',
                'Comm/Brokerage Amount(TP/Terr)': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Future Generali India Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'LONGDESC02': 'Risk',
                'POLICY_NO': 'Policy No.',
                'COMBINE_CLIENT_NAME': 'Client Name',
                'POL_END_DT': 'Policy End Date',
                'POL_STR_DT': 'Policy Start Date',
                'BRANCH_NAME': 'Branch',
                'BOOKING_TYPE': 'Income Category',
                'POLICY_ENDT_NO': 'Endorsement No.',
                'GWP': 'Premium1',
                'Base Premium(OD Premium)': 'Premium2',
                'TP/Terrorism Premium': 'Premium3',
                'COMMISSION': 'Brokerage1',
                'Comm/Brokerage Amount(OD)': 'Brokerage2',
                'Comm/Brokerage Amount(TP/Terr)': 'Brokerage3'
                                                      }

        elif selected_insurer == 'Manipal Cigna Health Insurance Company Limited (Prev. Cigna Ttk Health Insurance':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Line': 'Risk',
                'Policy Number': 'Policy No.',
                'Proposer Name': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'Branch': 'Branch',
                'Business Type': 'Income Category',
                'POLICY-TYPE': 'Policy Type',
                'Endorsement No.': 'Endorsement No.',
                'Base Premium': 'Premium1',
                'Base Premium(OD Premium)': 'Premium2',
                'TP/Terrorism Premium': 'Premium3',
                'Commission': 'Brokerage1',
                'Comm/Brokerage Amount(OD)': 'Brokerage2',
                'Comm/Brokerage Amount(TP/Terr)': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Aditya Birla Health Insurance Co.Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product': 'Risk',
                'Policy Number': 'Policy No.',
                'Master Holder Name': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'POLICY_START_DATE': 'Policy Start Date',
                'TO ': 'Branch',
                'BUSINESS_TYPE': 'Income Category',
                'Business Type': 'ASP Practice',
                'Endoresement': 'Endorsement No.',
                'GWP': 'Premium1',
                'Base Premium(OD Premium)': 'Premium2',
                'TP/Terrorism Premium': 'Premium3',
                'Comm Amt': 'Brokerage1',
                'Comm/Brokerage Amount(OD)': 'Brokerage2',
                'Comm/Brokerage Amount(TP/Terr)': 'Brokerage3'
                                                      }
        elif selected_insurer == 'Star Union Dai-Ichi Life Insurance Company Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'PRODNAME': 'Risk',
                'Master Mpolicy 3': 'Policy No.',
                'Client Name': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'POLICY_START_DATE': 'Policy Start Date',
                'TO ': 'Branch',
                'TRANDESC': 'Income Category',
                'TRANNO': 'Endorsement No.',
                'Premium': 'Premium',
                'Commisison': 'Brokerage'
                                                      }
        elif selected_insurer == 'Future Generali India Life Insurance Co Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product': 'Risk',
                'Policy No.': 'Policy No.',
                'Subsidiary': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'POLICY_START_DATE': 'Policy Start Date',
                'MPH State': 'Branch',
                'Type of Transaction': 'Income Category',
                'Policy Owner ID': 'ASP Practice',
                'Endoresement': 'Endorsement No.',
                'Premium': 'Premium',
                'Commisison': 'Brokerage'
                                                      }
        elif selected_insurer == 'Sbi Life Insurance Co. Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'Policy Number': 'Policy No.',
                'Policy Holder': 'Client Name',
                'POLICY_END_DATE': 'Policy End Date',
                'Risk Date': 'Policy Start Date',
                'Policy Holder Permenant State': 'Branch',
                'Premium Type ': 'Income Category',
                'Endoresement': 'Endorsement No.',
                'Premium': 'Premium',
                'Payable Gross commission': 'Brokerage'
                                                      }
        elif selected_insurer == 'Max Life Insurance Co. Ltd. ( Prev. Known As Max New York Life Insurance Co. Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Detailed Product Name': 'Risk',
                'Policy No.': 'Policy No.',
                'Client Names': 'Client Name',
                'EDC To': 'Policy End Date',
                'EDC From': 'Policy Start Date',
                'Nature of Premium (SP / FY / RW)': 'Income Category',
                'Commission Premium Amt. (Gross)': 'Premium',
                'Comm. Amt. (Gross)': 'Brokerage'
                                                      }
        elif selected_insurer == 'Aditya Birla Sun Life Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Plan': 'Risk',
                'Policy No.': 'Policy No.',
                'Client name': 'Client Name',
                'BSLI GST "TO" State':'Branch',
                'EDC To': 'Policy End Date',
                'Policy Inception Dt': 'Policy Start Date',
                'Commission Business Type': 'Income Category',
                ' Total Premium / Contribution ': 'Premium',
                ' Net Commission': 'Brokerage'
                                                      }            
        elif selected_insurer == 'IFFCO TOKIO General Insurance Co. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'Policy number': 'Policy No.',
                'Client Name': 'Client Name',
                'Risk exp date': 'Policy End Date',
                'Risk inc date': 'Policy Start Date',
                'Geographical State': 'Branch',
                'Type of Transaction': 'Income Category',
                'Endorsement ind': 'Endorsement No.',
                'sf': 'Premium1',
                's234': 'Premium2',
                'sfawe': 'Premium3',
                'cnmdf': 'Brokerage1',
                'adsf': 'Brokerage2',
                'dsfa': 'Brokerage3'
                                                      }                
        elif selected_insurer == 'Pramerica Life Insurance Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'Policy Number': 'Policy No.',
                'Client Name': 'Client Name',
                'Risk exp date': 'Policy End Date',
                'Risk inc date': 'Policy Start Date',
                'Geographical State': 'Branch',
                'Type of Transaction': 'Income Category',
                'Endorsement ind': 'Endorsement No.',
                ' ': 'Premium',
                'Commission': 'Brokerage'
                                                      }         
        elif selected_insurer == 'Pnb Metlife India Insurance Company Ltd (Pre. Met Life India Insurance Company P':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'Policy No.': 'Policy No.',
                'Client Name': 'Client Name',
                'Risk exp date': 'Policy End Date',
                'Risk inc date': 'Policy Start Date',
                'Geographical State': 'Branch',
                'Type of Transaction': 'Income Category1',
                'BILLNO': 'Income Category2',
                'Endorsement ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                'Premium': 'Premium',
                'Commission': 'Brokerage'
                                                      }             
        elif selected_insurer == 'Go Digit Life Insurance Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'Policy No.': 'Policy No.',
                'Policy Holder': 'Client Name',
                'Master Policy Holder Name': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'Imd Branch Name': 'Branch',
                'Type of Transaction': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                'Net Premium': 'Premium',
                'Total Commission Amount': 'Brokerage'
                                                      }
        elif selected_insurer == 'Niva Bupa Health Insurance Company Limited (Previously Known As Max Bupa Health':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product': 'Risk',
                'Policy_Number': 'Policy No.',
                'Customer': 'Client Name',
                'Master Policy Holder Name': 'Client Name1',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'Max_Bupa_State': 'Branch',
                'Business_Type': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                'GWP_AMOUNT': 'Premium',
                'Total_Comm_Amount': 'Brokerage'
                                                      }   
        elif selected_insurer == 'Edelweiss Tokio Life Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'POLICY_ID': 'Policy No.',
                'Name': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'Branch': 'Branch',
                'Type': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                'MODAL_PREM_AMT': 'Premium',
                'Total Gross': 'Brokerage'
                                                      }
        elif selected_insurer == 'Shriram Life Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'MP Number': 'Policy No.',
                'Insured Name': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                '': 'Branch',
                'Type': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                '': 'Premium',
                '': 'Brokerage'
                                                      }   
        elif selected_insurer == 'Aegon Life Insurance Company Private Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Product Name': 'Risk',
                'Policy No.': 'Policy No.',
                'MPH': 'Client Name',
                'Policy End Date': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                '': 'Branch',
                'Type': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                'Charged Premium': 'Premium',
                'Brokerage': 'Brokerage'
                                                      }  
        elif selected_insurer == 'IndiaFirst Life Insurance Company Ltd':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'CNTTYPE': 'Risk',
                'CHDRNUM': 'Policy No.',
                'OWNERNAME': 'Client Name',
                'HISSDTE': 'Policy End Date',
                'Policy Start Date': 'Policy Start Date',
                'BANK BRANCH STATE': 'Branch',
                'Type': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                '': 'ASP Practice',
                '': 'P & L JV',
                'TOTPREM': 'Premium',
                'AGNTCOMM': 'Brokerage'
                                                      }  
        elif selected_insurer == 'Kotak Mahindra Life Insurance Company Limited(Previously Know As Kotak Mahindra':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Plan': 'Risk',
                'GA Pol. No.': 'Policy No.',
                'Name Of Policy Holder': 'Client Name',
                'HISSDTE': 'Policy End Date',
                'DOC': 'Policy Start Date',
                'CRM Branch (TO State)': 'Branch',
                'Type': 'Income Category',
                'Endorsement Ind': 'Endorsement No.',
                'Master Pol. No.': 'ASP Practice',
                '': 'P & L JV',
                'ddsd': 'Premium',
                'dfsfs': 'Brokerage'
                                                      }  
        elif selected_insurer == 'National Insurance Company Limited':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'f': 'Risk',
                's': 'Policy No.',
                'x': 'Client Name',
                'z': 'Policy End Date',
                '  ': 'Policy Start Date',
                'dsaf': 'Branch',
                'safwe': 'Income Category',
                'ajoajd': 'Endorsement No.',
                'djaifljdail;': 'ASP Practice',
                'dsafqwera': 'P & L JV',
                'vmmvmvmvmvmv': 'Premium',
                'dsajkfl;ajkl;dfjaskl;': 'Brokerage'
                                                      } 
        elif selected_insurer == 'Aviva Life Insurance Co. India Pvt. Ltd.':

            read_tables_from_email(email_body, selected_insurer)  # Pass the selected insurer to the function
            default_mappings = {
                'Risk Code': 'Risk',
                'Policy No': 'Policy No.',
                'Policy Name': 'Client Name',
                'POLICY_EXP_DATE': 'Policy End Date',
                'DOC': 'Policy Start Date',
                'OFFICE_CODE': 'Branch',
                'Type': 'Income Category',
                'ENDORSEMENT_NUMBER': 'Endorsement No.',
                'Master Pol. No.': 'ASP Practice',
                '': 'P & L JV',
                'Premium Collected': 'Premium',
                'Commission Amount': 'Brokerage'
                                                      }
            # **PDF to Excel Conversion for Aviva Life Insurance Co. India Pvt. Ltd.**
            try:
                # Ensure the attachment is a PDF
                if file_attachment and file_attachment.lower().endswith('.pdf'):
                    pdf_file_path = file_attachment
                    base_name = os.path.splitext(os.path.basename(pdf_file_path))[0]
                    output_cleaned_excel_path = os.path.join(os.path.dirname(pdf_file_path), f"{base_name}_cleaned.xlsx")

                    # Extract tables from the PDF using Camelot
                    tables = camelot.read_pdf(pdf_file_path, pages="1", flavor="stream")  # Use 'stream' for bordered tables

                    # Check if any tables were extracted
                    if tables.n > 0:
                        # Save the raw extracted table
                        raw_df = tables[0].df
                        print("Raw Extracted Table:\n", raw_df.head())  # Debug: Print the extracted table structure
                        # raw_df.to_excel(output_raw_excel_path, index=False, header=False)  # Not saving raw as per user instruction

                        # Clean the DataFrame
                        df = raw_df[2:]  # Skip the first two rows (header and extra row)
                        df = df.reset_index(drop=True)  # Reset the index

                        # Explicitly use column D (index 3) for Policy Name
                        # Ensure there are enough columns
                        if df.shape[1] >= 4:
                            df = df.iloc[:, [0, 3, -3, -2, -1]]  # Policy No (0), Policy Name (3), Month (-3), Premium Collected (-2), Commission Amount (-1)
                            df.columns = ["Policy No", "Policy Name", "Month", "Premium Collected", "Commission Amount"]

                            # Remove rows with invalid numeric data (e.g., headers that were not properly removed)
                            df = df[~df["Premium Collected"].str.contains("Premium Collected", na=False)]  # Drop rows containing column headers

                            # Forward-fill missing Policy Name values if any
                            df["Policy Name"] = df["Policy Name"].replace("", None).fillna(method="ffill")

                            # Remove invalid rows with empty Policy No
                            df = df[df["Policy No"].str.strip().astype(bool)]

                            # Clean numeric columns (remove commas, handle empty cells, and convert to float)
                            df["Premium Collected"] = (
                                df["Premium Collected"]
                                .str.replace(",", "", regex=False)
                                .replace("", "0")  # Replace empty strings with 0
                                .astype(float)
                            )
                            df["Commission Amount"] = (
                                df["Commission Amount"]
                                .str.replace(",", "", regex=False)
                                .replace("", "0")  # Replace empty strings with 0
                                .astype(float)
                            )

                            # Save the cleaned DataFrame to an Excel file
                            df.to_excel(output_cleaned_excel_path, index=False)
                            print(f"Cleaned data extracted and saved to {output_cleaned_excel_path}")

                            # Update the attachment to the cleaned Excel file
                            file_attachment = output_cleaned_excel_path
                            file_path_n = output_cleaned_excel_path
                        else:
                            print("Not enough columns to perform cleaning.")
                    else:
                        print("No tables found in the PDF.")
                else:
                    print("Attachment is not a PDF. Skipping conversion.")
            except Exception as e:
                print(f"Error during PDF to Excel conversion: {str(e)}")
                return jsonify({'message': f"Error during PDF to Excel conversion: {str(e)}"}), 400

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
        'National Insurance Company Limited':0,
        'Kotak Mahindra Life Insurance Company Limited(Previously Know As Kotak Mahindra':0,
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
        'Bajaj Allianz Life Insurance Company Limited':0,
        'Magma Hdi General Insurance Company Limited':0,
        'Care Health Insurance Limited':0,
        'Future Generali India Insurance Company Limited':0,
        'Manipal Cigna Health Insurance Company Limited (Prev. Cigna Ttk Health Insurance':0,
        'Aditya Birla Health Insurance Co.Ltd.':0,
        'Future Generali India Life Insurance Co Ltd':0,
        'Star Union Dai-Ichi Life Insurance Company Ltd':0,
        'IFFCO TOKIO General Insurance Co. Ltd.':0,
        'Sbi Life Insurance Co. Ltd':0,
        'Max Life Insurance Co. Ltd. ( Prev. Known As Max New York Life Insurance Co. Ltd':0,
        'Aditya Birla Sun Life Insurance Company Limited':0,
        'Pramerica Life Insurance Limited':0,
        'Pnb Metlife India Insurance Company Ltd (Pre. Met Life India Insurance Company P':0,
        'Go Digit Life Insurance Limited':0,
        'Niva Bupa Health Insurance Company Limited (Previously Known As Max Bupa Health':0,
        'Edelweiss Tokio Life Insurance Company Limited':0,
        'Shriram Life Insurance Company Limited':0,
        'Aegon Life Insurance Company Private Limited':0,
        'IndiaFirst Life Insurance Company Ltd':0,
        'Aviva Life Insurance Co. India Pvt. Ltd.':0 # Add more insurers as needed
    }

    header_row = header_rows.get(selected_insurer, 0)  # Default to 0 if not specified

    # Read the file attachment using the appropriate header
# Read the file attachment using the appropriate header
    if isinstance(file_attachment, str) and os.path.exists(file_attachment):
        try:
            if file_attachment.endswith('.xlsx'):
                try:
                    # Try reading with default engine first
                    file_df = pd.read_excel(file_attachment, header=header_row, engine='openpyxl')
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
        'Aviva Life Insurance Co. India Pvt. Ltd.','National Insurance Company Limited','ICICI Lombard General Insurance Co. Ltd.', 'Star Health Insurance', 'Hdfc Life Insurance Co. Ltd.',
        'Kotak Mahindra Life Insurance Company Limited(Previously Know As Kotak Mahindra','IndiaFirst Life Insurance Company Ltd','Shriram General Insurance Company Limited', 'Kotak Mahindra General Insurance Company',
        'Shriram Life Insurance Company Limited','Edelweiss Tokio Life Insurance Company Limited','Universal Sampo Insurance', 'Zuno General Insurance Limited', 'ICICI Prudential Life Insurance Co Ltd','Niva Bupa Health Insurance Company Limited (Previously Known As Max Bupa Health',
        'Aegon Life Insurance Company Private Limited','Go Digit Life Insurance Limited','Pnb Metlife India Insurance Company Ltd (Pre. Met Life India Insurance Company P','Pramerica Life Insurance Limited','Max Life Insurance Co. Ltd. ( Prev. Known As Max New York Life Insurance Co. Ltd','Aditya Birla Sun Life Insurance Company Limited','Sbi Life Insurance Co. Ltd','IFFCO TOKIO General Insurance Co. Ltd.','Star Union Dai-Ichi Life Insurance Company Ltd','Future Generali India Life Insurance Co Ltd','Aditya Birla Health Insurance Co.Ltd.','Manipal Cigna Health Insurance Company Limited (Prev. Cigna Ttk Health Insurance','Future Generali India Insurance Company Limited','Magma Hdi General Insurance Company Limited','Care Health Insurance Limited','Bajaj Allianz Life Insurance Company Limited','Reliance General Insurance Co. Ltd.','Hdfc Ergo General Insurance Company Limited','Bajaj Allianz General Insurance Co. Ltd.','Tata AIG General Insurance Co. Ltd.','Royal Sundaram General Insurance Co Ltd','Raheja Qbe General Insurance Company Limited','GoDigit General Insurance Limited','Acko General Insurance Limited','SBI General Insurance Company Limited','Cholamandalam General Insurance Co. Ltd.', 'Tata AIA Insurance','Liberty Videocon General Insurance Co. Ltd'
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
