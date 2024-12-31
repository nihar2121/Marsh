from flask import Flask, render_template_string, request, redirect, url_for, send_file, jsonify, session
import os
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime
import threading
import webbrowser

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Replace with a secure secret key

# Configuration for file uploads
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# HTML Templates

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
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Logo" class="logo">
        <h2 class="text-center">Login Page</h2>
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
          <button type="submit" class="btn btn-primary">Login</button>
        </div>
      </form>
    </div>
  </body>
</html>
"""

browse_page = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" rel="stylesheet">
    <title>Browse Files</title>
    <style>
      .browse-container {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100vh;
        background: linear-gradient(180deg, #004080, #c8c8ff);
      }
      .browse-form {
        background: white;
        padding: 3rem; /* Increased padding for more whitespace */
        border-radius: 8px;
        box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
        width: 100%;
        max-width: 800px; /* Increased max-width for a larger form area */
        text-align: center;
      }
      .logo {
        width: 150px;
        margin: 0 auto 1.5rem;
      }
      .alert {
        margin-bottom: 1.5rem; /* Increased margin for better spacing */
      }
      .form-group {
        margin-bottom: 1.5rem; /* Increased spacing between form groups */
      }
      #file {
        width: 100%; /* Ensure file input takes full width */
      }
      .btn-primary {
        width: 100%; /* Make button full width */
        padding: 0.75rem;
        font-size: 1.1rem;
      }
    </style>
  </head>
  <body>
    <div class="browse-container">
      <form class="browse-form" method="post" enctype="multipart/form-data">
        <img src="https://i.pinimg.com/736x/b1/ba/ab/b1baab2ab9b18dc74d8a925f036dd598.jpg" alt="Logo" class="logo">
        <h2 class="text-center mb-4">Browse and Upload File</h2>
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
        <div class="form-group text-center">
          <button type="submit" class="btn btn-primary">Upload and Process</button>
        </div>
      </form>
    </div>
  </body>
</html>
"""

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
        # Replace 'your_password' with the actual password you want to set
        if (username == 'piyush' and password == 'your_password'):
            session['username'] = username
            return redirect(url_for('browse_files'))
        else:
            error = "Invalid credentials. Please try again."
    return render_template_string(login_page, error=error)

@app.route('/browse', methods=['GET', 'POST'])
def browse_files():
    if 'username' not in session:
        return redirect(url_for('login'))

    message = None
    error = None

    if request.method == 'POST':
        if 'file' not in request.files:
            error = 'No file part in the request.'
            return render_template_string(browse_page, message=message, error=error)
        
        file = request.files['file']

        if file.filename == '':
            error = 'No file selected.'
            return render_template_string(browse_page, message=message, error=error)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(save_path)

            # Placeholder for backend processing
            # You can call your backend processing function here
            # For now, we'll just return the same file

            return send_file(save_path, as_attachment=True)
        else:
            error = 'Unsupported file type. Please upload a CSV or Excel file.'
    
    return render_template_string(browse_page, message=message, error=error)

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

def run_app():
    app.run(port=5001, debug=False)  # Changed port to avoid conflict if original app is running

if __name__ == '__main__':
    threading.Thread(target=run_app).start()
    webbrowser.open('http://127.0.0.1:5001')
