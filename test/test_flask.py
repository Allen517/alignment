# -*- coding:utf8 -*-

from flask import Flask, url_for, render_template, Markup, request, session, escape, redirect

app = Flask(__name__)
print __name__

class Config(object):
    DEBUG = True
    DEVELOPMENT = True
    SECRET_KEY = 'do-i-really-need-this'
    FLASK_HTPASSWD_PATH = '/secret/.htpasswd'
    FLASK_SECRET = SECRET_KEY
    DB_HOST = 'database' # a docker link

class ProductionConfig(Config):
    DEVELOPMENT = False
    DEBUG = False
    DB_HOST = 'my.production.database' # not a docker link

app.config.from_object('test_flask.ProductionConfig')
print app.config

@app.route('/user/<username>')
def show_user_profile(username):
	# show the user profile for that user
	return 'User %s' % username

@app.route('/post/<int:post_id>')
def show_post(post_id):
	# show the post with the given id, the id is an integer
	return 'Post %d' % post_id

@app.route('/hello/')
@app.route('/hello/<name>')
def hello(name=None):
	return render_template('hello.html', name=name)

@app.route('/')
def index():
	if 'username' in session:
		return 'Logged in as %s' % escape(session['username'])
	return 'You are not logged in'

@app.route('/login', methods=['GET', 'POST'])
def login():
	if request.method == 'POST':
		session['username'] = request.form['username']
		return redirect(url_for('index'))
	return '''
		<form method="post">
		<p><input type=text name=username>
		<p><input type=submit value=Login>
		</form>
		'''

@app.route('/logout')
def logout():
	# remove the username from the session if it's there
	session.pop('username', None)
	return redirect(url_for('index'))

# set the secret key. keep this really secret:
app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

with app.test_request_context():
	print url_for('show_user_profile', username='w')
	# print url_for('hello_world', next="hello")
	print url_for('static', filename='style.css')
