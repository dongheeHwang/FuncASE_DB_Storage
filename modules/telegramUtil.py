import telegram
import time

from telegram import constants
from modules import common
from modules.resource import conf


def send_message(text):
    suc_yn = 'Y'
    msg = ''
    try:
        telegram_config = conf.telegram_config
        
        bot = telegram.Bot(token = telegram_config['TOKEN'])
        # updates = bot.getUpdates()

        msg = text[:constants.MAX_MESSAGE_LENGTH]
        bot.sendMessage(chat_id=telegram_config['CHAT_ID'], text=msg)

        time.sleep(2)
    except Exception as e:
        suc_yn = 'N'
    
    # 텔레그램 메시지 DB 등록
    common.insertOtMsg({'msg_cnt':msg, 'suc_yn':suc_yn})

    return True if suc_yn == 'Y' else False

