import logging

from kafkaConsumer import kafkaConsumer
from kafkaProducer import kafkaProducer

import numpy as np
from aiogram import Bot, Dispatcher, executor, types
from emoji import emojize

import config

# init random seed
np.random.seed(42)

# logs
logging.basicConfig(level=logging.INFO)

# Initialization
bot = Bot(token=config.TOKEN)
dp = Dispatcher(bot)

con = kafkaConsumer('topic2', 1, 'group1', 'localhost:9092')
prod = kafkaProducer('localhost:9092', 'topic1')


# Processing
@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    await message.answer(f"Привет! {emojize(':waving_hand:')}\n"
                         f"Это бот, позволяющий генерировать гороскоп по твоему знаку зодиака. {emojize(':shooting_star:')}\n"
                         f"Для генерации выполни команду /generate <знак зодиака>\n"
                         f"Например:\n"
                         f"/generate лев\n"
                         f"/generate Весы\n"
                         f"/generate бЛиЗнЕцЫ")


@dp.message_handler(commands=['generate'])
async def generate_horo(message: types.Message):
    horoscope = ''
    arguments = message.get_args().split(' ')
    if len(arguments) != 1:
        await message.reply('Напиши /generate <свой знак зодиака>')
    else:
        sign = arguments[0].lower()
        if sign not in ['овен', 'близнецы', 'телец', 'рак', 'лев', 'дева',
                        'весы', 'скорпион', 'стрелец', 'козерог', 'водолей', 'рыбы']:
            await message.answer(f'Ты ввел несуществующий знак зодиака.')
        else:
            await message.answer(f'Твой знак зодиака: {sign}.'
                                 f'\nГeнepиpyю гороскоп (это может занять некоторое время)')
            logging.info(f'Pushing sign to kafka: {sign}')
            prod.push(sign)
            while True:
                msg = con.read_from_topic()
                if msg == 0:
                    continue
                else:
                    print('Going to decode message:: ', msg)
                    horoscope = msg.decode('utf-8')
                    break
            await message.answer(f'Твой гороскоп:\n\n{horoscope}')


# run long-polling
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=False)
