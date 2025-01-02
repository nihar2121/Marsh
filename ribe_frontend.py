from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, session
import os
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime
import threading
import webbrowser
from jinja2 import DictLoader

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Replace with a secure secret key

# Configuration for file uploads
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# HTML Templates

base_template = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>RI Bank Entries - {{ title }}</title>
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <style>
      body {
        background: linear-gradient(180deg, #004080, #c8c8ff);
        min-height: 100vh;
        display: flex;
        flex-direction: column;
      }
      .navbar {
        background-color: #003366;
      }
      .navbar-brand {
        color: #ffffff !important;
        font-weight: bold;
        font-size: 1.5rem;
      }
      .container-content {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
      }
      .form-container {
        background: white;
        padding: 2rem;
        border-radius: 8px;
        box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 500px;
      }
      .logo {
        width: 150px;
        display: block;
        margin: 0 auto 1rem;
      }
      .footer {
        background-color: #003366;
        color: #ffffff;
        text-align: center;
        padding: 1rem 0;
      }
      .btn-primary {
        background-color: #0059b3;
        border-color: #0059b3;
      }
      .btn-primary:hover {
        background-color: #004080;
        border-color: #003366;
      }
      .alert {
        margin-bottom: 1.5rem;
      }
      .form-group {
        margin-bottom: 1.5rem;
      }
      #file {
        width: 100%;
      }
      .btn-block {
        width: 100%;
      }
    </style>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg">
      <a class="navbar-brand" href="#">RI Bank Entries</a>
      {% if session.username %}
      <div class="ml-auto">
        <a href="{{ url_for('logout') }}" class="btn btn-outline-light">Logout</a>
      </div>
      {% endif %}
    </nav>
    <div class="container-content">
      {% block content %}{% endblock %}
    </div>
    <footer class="footer">
      &copy; {{ current_year }} RI Bank Entries. All rights reserved.
    </footer>
  </body>
</html>
"""

login_page = """
{% extends "base.html" %}
{% block content %}
  <form class="form-container" method="post">
    <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Logo" class="logo">
    <h2 class="text-center mb-4">Login</h2>
    {% if error %}
      <div class="alert alert-danger" role="alert">
        {{ error }}
      </div>
    {% endif %}
    <div class="form-group">
      <label for="username">Username:</label>
      <input type="text" id="username" name="username" class="form-control" placeholder="Enter username" required>
    </div>
    <div class="form-group">
      <label for="password">Password:</label>
      <input type="password" id="password" name="password" class="form-control" placeholder="Enter password" required>
    </div>
    <button type="submit" class="btn btn-primary btn-block">Login</button>
  </form>
{% endblock %}
"""

browse_page = """
{% extends "base.html" %}
{% block content %}
  <form class="form-container" method="post" enctype="multipart/form-data">
    <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Logo" class="logo">
    <h2 class="text-center mb-4">Upload File</h2>
    {% if message %}
      <div class="alert alert-info" role="alert">
        {{ message }}
      </div>
    {% endif %}
    {% if error %}
      <div class="alert alert-danger" role="alert">
        {{ error }}
      </div>
    {% endif %}
    <div class="form-group">
      <label for="file">Select a CSV or Excel file:</label>
      <input type="file" id="file" name="file" class="form-control-file mt-2" accept=".csv, .xlsx, .xls" required>
    </div>
    <button type="submit" class="btn btn-primary btn-block">Upload and Process</button>
  </form>
{% endblock %}
"""

# Setting up the Jinja2 DictLoader with all templates
template_dict = {
    'base.html': base_template,
    'login.html': login_page,
    'browse.html': browse_page
}

app.jinja_loader = DictLoader(template_dict)

# Helper Functions

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Routes

@app.route('/', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        # Updated password to 'nihar21'
        if (username == 'piyush' and password == 'nihar21'):
            session['username'] = username
            return redirect(url_for('browse_files'))
        else:
            error = "Invalid credentials. Please try again."
    return render_template('login.html', error=error, title="Login", current_year=datetime.now().year)

@app.route('/browse', methods=['GET', 'POST'])
def browse_files():
    if 'username' not in session:
        return redirect(url_for('login'))

    message = None
    error = None

    if request.method == 'POST':
        if 'file' not in request.files:
            error = 'No file part in the request.'
            return render_template('browse.html', message=message, error=error, title="Browse Files", current_year=datetime.now().year)
        
        file = request.files['file']

        if file.filename == '':
            error = 'No file selected.'
            return render_template('browse.html', message=message, error=error, title="Browse Files", current_year=datetime.now().year)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(save_path)

            # Placeholder for backend processing
            # You can call your backend processing function here
            # For now, we'll just return the same file

            message = f'File "{filename}" uploaded successfully.'
            return send_file(save_path, as_attachment=True)
        else:
            error = 'Unsupported file type. Please upload a CSV or Excel file.'
    
    return render_template('browse.html', message=message, error=error, title="Browse Files", current_year=datetime.now().year)

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

def run_app():
    app.run(port=5001, debug=False)  # Changed port to avoid conflict if original app is running

if __name__ == '__main__':
    threading.Thread(target=run_app).start()
    webbrowser.open('http://127.0.0.1:5001')
