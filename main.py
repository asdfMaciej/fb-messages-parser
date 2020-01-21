import os  # directory traversal
import json  # used by FB as the format
import time  # handle dates in messages
import sqlite3  # lightweight choice for storing data

def getMessagePaths(directory):
	def isMessageFile(filename):  # atm they follow message_<n>.json format, this is enough
		return '.json' in filename and 'message' in filename

	all_files = os.listdir(directory)  # get all files in directory
	message_files = list(filter(isMessageFile, all_files))  # take only the ones with messages
	return list(map(lambda x: directory+'\\'+x, message_files))  # add the directory prefix

def getSubdirectories(root_directory):  # each person thread is in a different directory
	return [f.path for f in os.scandir(root_directory) if f.is_dir()]  # this gets all

def parseJsonObject(obj):  # FB json encoding is really, really funky
	for key in obj:
		if isinstance(obj[key], str):
			obj[key] = obj[key].encode('latin_1').decode('utf-8')
		elif isinstance(obj[key], list):
			obj[key] = list(map(lambda x: x if type(x) != str else x.encode('latin_1').decode('utf-8'), obj[key]))
	return obj

def loadJson(path):  # loads json with proper encoding
	with open(path, "r", encoding="utf-8") as file:
		data = json.load(file, object_hook=parseJsonObject)
	return data

class Conversation:
	def __init__(self, directory):
		self.directory = directory
		self.metadata = {}
		self.participants = {}  # {name: id}
		self.chunks_loaded = 0  # files loaded
		self.messages = []
		self.id = 0  # needs to be set before saving

	def loadMessages(self, json):
		metadata = {
			'title': json['title'],
			'thread_type': json['thread_type'],
			'thread_path': json['thread_path']
		}

		if not self.metadata:  # first chunk being loaded
			self.metadata = metadata
			for index, participant in enumerate(json['participants']):
				self.participants[participant['name']] = None
		else:
			assert self.metadata == metadata  # ensure conversation integrity

		self.chunks_loaded += 1
		self.messages += json['messages']

	def __str__(self):
		desc = f"<Conversation> stored at {self.directory} "
		desc += f"with {self.chunks_loaded} loaded files and "
		desc += f"{len(self.participants)} participants."
		return desc

class Database:
	def __init__(self, path):
		self.path = path
		self.db = sqlite3.connect(path)

	def save(self, item):
		cursor, result = self.db.cursor(), False

		if isinstance(item, Conversation):
			result = self._saveConversation(item, cursor)

		self.db.commit()
		return result

	def removeAll(self, item_type):
		cursor, result = self.db.cursor(), False
		if item_type == Conversation:
			result, tables = True, [
				"conversations", "participants", "messages", "photos",
				"shares", "files", "gifs", "videos", "audio_files", 
				"reactions", "stickers"]

			for table in tables: 
				result = result and self._removeAll(table, cursor)

		self.db.commit()
		return result

	def _getParticipantId(self, conversation, name):
		# sometimes a conversation participant was removed since downloading data
		# we need to assign him an ID, even tho he isn't participating right now
		try:
			participant_id = conversation.participants[name]
		except KeyError:
			participant_id = self._saveParticipant(conversation.id, name, False, self.db.cursor())
			conversation.participants[name] = participant_id

		return participant_id

	def _saveConversation(self, item, cursor):
		# saves conversation along with all participants & messages
		result = cursor.execute("""
			INSERT INTO conversations (id, title, type, path)
			VALUES (?, ?, ?, ?)""",
			(item.id, item.metadata['title'],
				item.metadata['thread_type'], item.metadata['thread_path']))

		for participant in item.participants.keys():
			item.participants[participant] = self._saveParticipant(item.id, participant, True, cursor)

		for message in item.messages:
			result = result and self._saveMessage(message, item, cursor)

		return result

	def _saveParticipant(self, convo_id, participant, current, cursor):
		# current is whether he's currently in the conversation
		cursor.execute("""INSERT INTO participants (convo_id, name, current) VALUES (?, ?, ?)""",
				(convo_id, participant, 1 if current else 0))
		return cursor.lastrowid

	def _saveMessage(self, item, conversation, cursor):
		content = item['content'] if 'content' in item else None
		epoch = item['timestamp_ms'] // 1000
		sender_id = self._getParticipantId(conversation, item['sender_name'])

		result = cursor.execute("""
			INSERT INTO messages (timestamp_s, type, sender_id, conversation_id, content)
			VALUES (?, ?, ?, ?, ?)
			""",
			(epoch, item['type'], sender_id, conversation.id, content))

		message_id = cursor.lastrowid
		if "reactions" in item:
			for reaction in item['reactions']:
				user_id = self._getParticipantId(conversation, reaction['actor'])
				self._saveReaction(message_id, user_id, reaction['reaction'], cursor)

		attachements = ["photos", "videos", "files", "audio_files", "gifs"]

		# They all follow the same data format in JSON 
		# tables in sqlite db follow the same terminology
		for table in attachements:
			if table in item:
				for data in item[table]:
					self._saveAttachement(table, message_id, data, cursor)

		if "share" in item:
			self._saveShare(message_id, item['share'], cursor)

		if "sticker" in item:
			self._saveSticker(message_id, item['sticker'], cursor)

		return message_id

	def _saveReaction(self, message_id, user_id, reaction, cursor):
		return cursor.execute("""
			INSERT INTO reactions (message_id, user_id, reaction)
			VALUES (?, ?, ?)""",
			(message_id, user_id, reaction))

	def _saveAttachement(self, table, message_id, item, cursor):
		filename = item['uri'].split('/')[-1]
		return cursor.execute(
			f"INSERT INTO {table} (message_id, filename) VALUES (?, ?)",
			(message_id, filename))

	def _saveSticker(self, message_id, sticker, cursor):
		path = sticker["uri"]  # sometimes they're stored in a different directory
		return cursor.execute("""
			INSERT INTO stickers (message_id, path)
			VALUES (?, ?)""",
			(message_id, path))

	def _saveShare(self, message_id, share, cursor):
		link = share["link"] if "link" in share else None
		share_text = share["share_text"] if "share_text" in share else None
		return cursor.execute("""
			INSERT INTO shares (message_id, link, share_text)
			VALUES (?, ?, ?)
			""", (message_id, link, share_text))

	def _removeAll(self, table, cursor):
		return cursor.execute(f"DELETE FROM {table}")  # internal function, so it's safe


# establish connection and wipe data
database = Database("database.db")
database.removeAll(Conversation)

# get all conversations in the messages/ directory
conversations = getSubdirectories("messages")
for convo_id, convo_directory in enumerate(conversations):
	print(f"[*] Conversation #{convo_id+1} out of {len(conversations)}: {convo_directory}")
	conversation = Conversation(convo_directory)
	conversation.id = convo_id  # auto increments

	# get all messages in the conversation directory
	messagePaths = getMessagePaths(convo_directory)
	for n, messages_path in enumerate(messagePaths):
		print(f"Loading files: {n+1}/{len(messagePaths)}")
		conversation.loadMessages(loadJson(messages_path))

	print("[*] Saving to database... ", end='')
	# save the updated conversation object
	database.save(conversation)
	print("done!\n")  # extra new line for clarity

print("The script has finished. You may access the database now.")