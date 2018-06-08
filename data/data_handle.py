# We have to convert the raw reddit comments
# They must be stored in a question reply format to be used by the bot

# Import sqlite3 as our database
import sqlite3
import json 
from datetime import datetime

# Time our dataset was collected
timeframe = '2011-09'
# 2011-09 approx has 12M comments

# Instead of storing one transcation we will store multiple transactions for better speed
sql_transaction = []


connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def transaction_bldr(sql):
	global sql_transaction
	sql_transaction.append(sql)
	# Do transcations together after 1000 query so its faster
	if len(sql_transaction) > 1000:
		c.execute('BEGIN TRANSACTION')
		for s in sql_transaction:
			try:
				c.execute(s)
			except:
				pass
		connection.commit()
		sql_transaction = []


def create_table():
	c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
		(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE,
		 parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)""")

def format_data(text):

	# Replac newline and \r to custom char and all double quotes to single quotes
	text = text.replace("\n", " <newlinechar> ")
	text = text.replace("\r", " <returnchar> ")
	text = text.replace('"', "'")

	return text

def find_parent(pid):
	try:
		sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
		c.execute(sql)
		result = c.fetchone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
		print("Error finding parent:", e)

def find_existing_score(pid):
	try:
		sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
		c.execute(sql)
		result = c.fetchone()
		if result != None:
			return result[0]
		else:
			return False
	except Exception as e:
		print("Error finding parent:", e)

def acceptable_comment(data):
	if len(data.split(' ')) > 50 or len(data) < 1:
		return False
	elif len(data) > 1000:
		return False
	elif data == '[removed]' or data == '[deleted]':
		return False
	else:
		return True 

def sql_update_comment(commentid, parentid, parent, comment, subreddit, time, score):
	try: 
		sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
		transaction_bldr(sql)
	except Exception as e:
		# print("Unable to update parent-comment", e)
		pass

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
	try:
		sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
		transaction_bldr(sql)
	except Exception as e:
		# print('Unable to comment to parent',e)
		pass

def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
	try:
		sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
		transaction_bldr(sql)
	except Exception as e:
		# print('Unable to add comment',e)
		pass

if __name__ == "__main__":
	create_table()
	# Number of rows 
	row_cnt = 0

	# Number of actual rows to store in database
	paired_cnt = 0

	with open('./RC_{}'.format(timeframe),"r", 1000) as f:
		for row in f:
			# Fetch row
			row_cnt += 1
			if row_cnt < 12000000:
				continue

			try:
				row = json.loads(row)
			except Exception as e:
				print("Error loading JSON",e)
				continue
			parent_id = row['parent_id']
			body = format_data(row['body'])
			# Convert to utf-8
			body = unicode(body).encode("utf-8")
			comment_id = row['name']
			created_utc = row['created_utc']
			score = row['score']
			subreddit = row['subreddit']

			parent_data = find_parent(parent_id)

			# Check atleast it is a good commrnt
			if score >= 2:
				if not acceptable_comment(body):
					# print("Bad comment",body, len(body), len(body.split(' ')))
					continue

				# Check if the parent already has a commen
				exising_comment_score = find_existing_score(parent_id)
				# If yes check if we need to update it
				if exising_comment_score:
					if score > exising_comment_score:
						# print("Better score",score,exising_comment_score)
						sql_update_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
					else:
						pass
				# If no add a new row to the table
				else:
					# print("New score:",score)	
					if parent_data:
						# Add comment to a parent
						sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
						paired_cnt += 1
					else:
						# Add it so later it can be a parent itself
						sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

			else:
				pass

			if row_cnt % 100000 == 0:
				print("Time:",datetime.now())
				print("Total read: {}/12150412",row_cnt)
				print("Total paired rowes added to db:", paired_cnt)

	print("Cleanin up!")
	sql = "DELETE FROM parent_reply WHERE parent IS NULL"
	c.execute(sql)
	connection.commit()
	c.execute("VACUUM")
	connection.commit()