# -*- coding:utf8 -*-

from flask import Flask, url_for, render_template

app = Flask(__name__)

@app.route('/')
def hello_world():
	return 'Hello, World!'

@app.route('/login')
def login(): pass

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

with app.test_request_context():
	print url_for('show_user_profile', username='w')
	print url_for('hello_world', next="hello")
	print url_for('static', filename='style.css')