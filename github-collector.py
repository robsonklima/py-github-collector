#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Title: github-collector 
Created on Sat Mar 31 23:21:40 2018 
@author: robsonlima
"""

import requests
import sqlite3

headers = {"Authorization": "Bearer 0f0f023e00b7b889130f8fedc17bad30690651aa"}
db = '/Users/robsonlima/Documents/Workspace/python/github-collector/github-collector.db'

def get_repositories(last_id): 
    url = 'https://api.github.com/repositories?since={0}'.format(str(last_id))
    response  = requests.get(url, headers=headers)
    #response.raise_for_status()    
    return response.json()

def get_commits(owner, repo):
    url = 'https://api.github.com/repos/{0}/{1}/commits'.format(owner, repo)
    response  = requests.get(url, headers=headers)
    #response.raise_for_status()    
    return response.json()

def get_last_repo_id(): 
    last_id = 1
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    for row in cursor.execute('SELECT * FROM repositories ORDER BY id DESC LIMIT 1'):
        last_id = row[0]
    conn.close()
    return last_id

def save_repo(id, name, full_name, description, private, fork, url, html_url, owner_id): 
    try:
       conn = sqlite3.connect(db)
       cursor = conn.cursor()
       cursor.execute("""INSERT INTO repositories (id, name, full_name, 
                      description, private, fork, url, html_url, owner_id)
                      VALUES (?,?,?,?,?,?,?,?,?)""", (id, name, full_name, 
                      description, private, fork, url, html_url, owner_id))
       conn.commit()
       conn.close()
    except sqlite3.Error as e:
       print("An error occurred:", e.args[0])
       conn.rollback()
       
def save_owner(id, login, type, site_admin): 
    try:
       conn = sqlite3.connect(db)
       cursor = conn.cursor()
       cursor.execute("""INSERT INTO owners (id, login, type, site_admin)
                      VALUES (?,?,?,?)""", (id, login, type, site_admin))
       conn.commit()
       conn.close()
    except sqlite3.Error as e:
       print("An error occurred:", e.args[0])
       conn.rollback()
       
def save_commit(sha, message, date, author_email, repository_id): 
    try:
       conn = sqlite3.connect(db)
       cursor = conn.cursor()
       cursor.execute("""INSERT INTO commits (sha, message, date, author_email, repository_id)
                      VALUES (?,?,?,?,?)""", (sha, message, date, author_email, repository_id))
       conn.commit()
       conn.close()
    except sqlite3.Error as e:
       print("An error occurred:", e.args[0])
       conn.rollback()
       
def save_author(email, name): 
    try:
       conn = sqlite3.connect(db)
       cursor = conn.cursor()
       cursor.execute("""INSERT INTO authors (email, name)
                      VALUES (?,?)""", (email, name))
       conn.commit()
       conn.close()
    except sqlite3.Error as e:
       print("An error occurred:", e.args[0])
       conn.rollback()

last_repo_id = get_last_repo_id()

while(True):    
    repos = get_repositories(last_repo_id)
    for repo in repos:
        save_owner(repo['owner']['id'], repo['owner']['login'], 
                   repo['owner']['type'], repo['owner']['site_admin'])
        save_repo(repo['id'], repo['name'], repo['full_name'], 
                  repo['description'], repo['private'], repo['fork'], 
                  repo['url'], repo['html_url'], repo['owner']['id'])
        commits = get_commits(repo['owner']['login'], repo['name'])
        for commit in commits:
            try:
                save_author(commit['commit']['author']['email'], 
                            commit['commit']['author']['name'])
            except:
                print('Err')
            
            try:
                save_commit(commit['sha'], commit['commit']['message'], 
                            commit['commit']['author']['date'], 
                            commit['commit']['author']['email'], repo['id'])    
            except:
                print('Err')
                
        last_repo_id=repo['id']