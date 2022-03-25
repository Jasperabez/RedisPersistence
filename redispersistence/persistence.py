import pickle
from collections import defaultdict
from copy import deepcopy
from typing import Any, DefaultDict, Dict, Optional, Tuple
from redis import Redis

from telegram.ext import BasePersistence


class RedisPersistence(BasePersistence):
	'''Using Redis to make the bot persistent'''

	def __init__(self,redis: Redis,on_flush: bool = False):
		super().__init__(store_user_data=True,store_chat_data=True,store_bot_data=True)
		self.redis: Redis = redis
		self.on_flush = on_flush
		self.user_data: Optional[DefaultDict[int, Dict]] = None
		self.chat_data: Optional[DefaultDict[int, Dict]] = None
		self.bot_data: Optional[Dict] = None

	def load_redis(self) -> None:
		try:
			data_bytes = self.redis.get('TelegramBotPersistence')
			if data_bytes:
				data = pickle.loads(data_bytes)
				self.user_data = defaultdict(dict, data['user_data'])
				self.chat_data = defaultdict(dict, data['chat_data'])
				# For backwards compatibility with files not containing bot data
				self.bot_data = data.get('bot_data', {})
			else:
				self.user_data = defaultdict(dict)
				self.chat_data = defaultdict(dict)
				self.bot_data = {}
		except Exception as exc:
			raise TypeError(f"Something went wrong unpickling from Redis") from exc

	def dump_redis(self) -> None:
		data = {
			'user_data': self.user_data,
			'chat_data': self.chat_data,
			'bot_data': self.bot_data,
		}
		data_bytes = pickle.dumps(data)
		self.redis.set('TelegramBotPersistence',data_bytes)

	def get_user_data(self) -> DefaultDict[int, Dict[Any, Any]]:
		'''Returns the user_data from the pickle on Redis if it exists or an empty :obj:`defaultdict`.'''
		if self.user_data:
			pass
		else:
			self.load_redis()
		return deepcopy(self.user_data)  # type: ignore[arg-type]

	def get_chat_data(self) -> DefaultDict[int, Dict[Any, Any]]:
		'''Returns the chat_data from the pickle on Redis if it exists or an empty :obj:`defaultdict`.'''
		if self.chat_data:
			pass
		else:
			self.load_redis()
		return deepcopy(self.chat_data)  # type: ignore[arg-type]

	def get_bot_data(self) -> Dict[Any, Any]:
		'''Returns the bot_data from the pickle on Redis if it exists or an empty :obj:`dict`.'''
		if self.bot_data:
			pass
		else:
			self.load_redis()
		return deepcopy(self.bot_data)  # type: ignore[arg-type]

	def update_user_data(self, user_id: int, data: Dict) -> None:
		'''Will update the user_data and depending on :attr:`on_flush` save the pickle on Redis.'''
		if self.user_data is None:
			self.user_data = defaultdict(dict)
		if self.user_data.get(user_id) == data:
			return
		self.user_data[user_id] = data
		if not self.on_flush:
			self.dump_redis()

	def update_chat_data(self, chat_id: int, data: Dict) -> None:
		'''Will update the chat_data and depending on :attr:`on_flush` save the pickle on Redis.'''
		if self.chat_data is None:
			self.chat_data = defaultdict(dict)
		if self.chat_data.get(chat_id) == data:
			return
		self.chat_data[chat_id] = data
		if not self.on_flush:
			self.dump_redis()

	def update_bot_data(self, data: Dict) -> None:
		'''Will update the bot_data and depending on :attr:`on_flush` save the pickle on Redis.'''
		if self.bot_data == data:
			return
		self.bot_data = data.copy()
		if not self.on_flush:
			self.dump_redis()

	def flush(self) -> None:
		'''Will save all data in memory to pickle on Redis.'''
		self.dump_redis()