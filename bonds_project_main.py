from tinkoff.invest import MarketDataRequest,AsyncClient, SubscriptionAction, LastPriceInstrument, SubscribeLastPriceRequest, Client, CandleInstrument, CandleInterval, SubscribeCandlesRequest, SubscriptionInterval
from tokenAPI import token
from datetime import date, datetime, timedelta
import time
import telebot
from tokenAPI import bottoken
import threading
import requests
from bs4 import BeautifulSoup as bs
import asyncio
from tqdm import tqdm

bot = telebot.TeleBot(bottoken)

chat_id = ''
chat_ids = []

user_subscriptions = {}
user_edits = {}

def set_chat_id(message):
    global chat_id
    global chat_ids
    chat_id = message.chat.id
    chat_ids.append(chat_id)
    if chat_id not in user_subscriptions:
        user_subscriptions[chat_id] = {'Consumer': False, 'Energy': False, 'Financial': False, 'Government': False, 'Health Care': False, 'Industrials': False, 'IT': False, 'Materials': False, 'Municipal': False, 'Other': False, 'Real Estate': False, 'Telecom': False, 'Utilities': False}
        user_edits[chat_id] = {'consumer': 0, 'energy': 0, 'financial': 0, 'government': 0, 'health_care': 0, 'industrials': 0, 'it': 0, 'materials': 0, 'municipal': 0, 'other': 0, 'real_estate': 0, 'telecom': 0, 'utilities': 0}

@bot.message_handler(commands=['subscriptions'])
def subscriptions(message):
    set_chat_id(message)
    subscription_message = ''
    for subscription in user_subscriptions[chat_id]:
        if user_subscriptions[chat_id][subscription]:
            subscription_message += f'{subscription} -- &#9989\n'
        else:
            subscription_message += f'{subscription} -- &#10060\n'
    bot.send_message(chat_id, f'<b>Your Subscriptions:</b>\n'
                              f'{subscription_message}', parse_mode='html', disable_web_page_preview=True)

start_message_sent, consumer_message_sent, energy_message_sent, financial_message_sent, government_message_sent, health_care_message_sent, industrials_message_sent, it_message_sent, materials_message_sent, municipal_message_sent, other_message_sent, real_estate_message_sent, telecom_message_sent, utilities_message_sent = '', '', '', '', '', '', '', '', '', '', '', '', '', ''
messages = {'consumer_message': '', 'energy_message': '', 'financial_message': '', 'government_message': '', 'health_care_message': '', 'industrials_message': '', 'it_message': '', 'materials_message': '', 'municipal_message': '', 'other_message': '', 'real_estate_message': '', 'telecom_message': '', 'utilities_message': ''}

@bot.message_handler(commands=['start','all'])
def start(message):
    set_chat_id(message)
    global start_message_sent
    if messages['utilities_message'] != '':
        bot.send_message(chat_id, f'You subscribed to all sectors\n'
                                  f'<b>Current Top:</b>', parse_mode='html')
        user_edits[chat_id]['consumer'] = bot.send_message(chat_id, f'<b>Sector: Consumer &#x1F6D2</b> \n'
                                                                  f'{messages["consumer_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['energy'] = bot.send_message(chat_id, f'<b>Sector: Energy &#x1F50B</b> \n'
                                                                f'{messages["energy_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['financial'] = bot.send_message(chat_id, f'<b>Sector: Financial &#x1F4B5</b> \n'
                                                                   f'{messages["financial_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['government'] = bot.send_message(chat_id, f'<b>Sector: Government &#x1F3DB</b> \n'
                                                                    f'{messages["government_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['health_care'] = bot.send_message(chat_id, f'<b>Sector: Health Care &#x1F48A</b> \n'
                                                                     f'{messages["health_care_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['industrials'] = bot.send_message(chat_id, f'<b>Sector: Industrials &#x1F3D7</b> \n'
                                                                     f'{messages["industrials_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['it'] = bot.send_message(chat_id, f'<b>Sector: IT &#x1F4BB</b> \n'
                                                            f'{messages["it_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['materials'] = bot.send_message(chat_id, f'<b>Sector: Materials &#x1F9F1</b> \n'
                                                                   f'{messages["materials_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['municipal'] = bot.send_message(chat_id, f'<b>Sector: Municipal &#x1F306</b> \n'
                                                                   f'{messages["municipal_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['other'] = bot.send_message(chat_id, f'<b>Sector: Other &#x1F441</b> \n'
                                                               f'{messages["other_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['real_estate'] = bot.send_message(chat_id, f'<b>Sector: Real Estate &#x1F3E0</b> \n'
                                                                     f'{messages["real_estate_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['telecom'] = bot.send_message(chat_id, f'<b>Sector: Telecom &#x1F4E1</b> \n'
                                                                 f'{messages["telecom_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        user_edits[chat_id]['utilities'] = bot.send_message(chat_id, f'<b>Sector: Utilities &#x1F4A1</b> \n'
                                                                   f'{messages["utilities_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
    else:
        bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
        while messages['utilities_message'] == '':
            time.sleep(5)
        if start_message_sent != messages['utilities_message']:
            bot.send_message(chat_id, f'You subscribed to all sectors\n'
                                      f'<b>Current Top:</b>', parse_mode='html')
            user_edits[chat_id]['consumer'] = bot.send_message(chat_id, f'<b>Sector: Consumer &#x1F6D2</b> \n'
                                                                      f'{messages["consumer_message"]}',
                                                             parse_mode='html',
                                                             disable_web_page_preview=True).message_id
            user_edits[chat_id]['energy'] = bot.send_message(chat_id, f'<b>Sector: Energy &#x1F50B</b> \n'
                                                                    f'{messages["energy_message"]}', parse_mode='html',
                                                           disable_web_page_preview=True).message_id
            user_edits[chat_id]['financial'] = bot.send_message(chat_id, f'<b>Sector: Financial &#x1F4B5</b> \n'
                                                                       f'{messages["financial_message"]}',
                                                              parse_mode='html',
                                                              disable_web_page_preview=True).message_id
            user_edits[chat_id]['government'] = bot.send_message(chat_id, f'<b>Sector: Government &#x1F3DB</b> \n'
                                                                        f'{messages["government_message"]}',
                                                               parse_mode='html',
                                                               disable_web_page_preview=True).message_id
            user_edits[chat_id]['health_care'] = bot.send_message(chat_id, f'<b>Sector: Health Care &#x1F48A</b> \n'
                                                                         f'{messages["health_care_message"]}',
                                                                parse_mode='html',
                                                                disable_web_page_preview=True).message_id
            user_edits[chat_id]['industrials'] = bot.send_message(chat_id, f'<b>Sector: Industrials &#x1F3D7</b> \n'
                                                                         f'{messages["industrials_message"]}',
                                                                parse_mode='html',
                                                                disable_web_page_preview=True).message_id
            user_edits[chat_id]['it'] = bot.send_message(chat_id, f'<b>Sector: IT &#x1F4BB</b> \n'
                                                                f'{messages["it_message"]}', parse_mode='html',
                                                       disable_web_page_preview=True).message_id
            user_edits[chat_id]['materials'] = bot.send_message(chat_id, f'<b>Sector: Materials &#x1F9F1</b> \n'
                                                                       f'{messages["materials_message"]}',
                                                              parse_mode='html',
                                                              disable_web_page_preview=True).message_id
            user_edits[chat_id]['municipal'] = bot.send_message(chat_id, f'<b>Sector: Municipal &#x1F306</b> \n'
                                                                       f'{messages["municipal_message"]}',
                                                              parse_mode='html',
                                                              disable_web_page_preview=True).message_id
            user_edits[chat_id]['other'] = bot.send_message(chat_id, f'<b>Sector: Other &#x1F441</b> \n'
                                                                   f'{messages["other_message"]}', parse_mode='html',
                                                          disable_web_page_preview=True).message_id
            user_edits[chat_id]['real_estate'] = bot.send_message(chat_id, f'<b>Sector: Real Estate &#x1F3E0</b> \n'
                                                                         f'{messages["real_estate_message"]}',
                                                                parse_mode='html',
                                                                disable_web_page_preview=True).message_id
            user_edits[chat_id]['telecom'] = bot.send_message(chat_id, f'<b>Sector: Telecom &#x1F4E1</b> \n'
                                                                     f'{messages["telecom_message"]}',
                                                            parse_mode='html', disable_web_page_preview=True).message_id
            user_edits[chat_id]['utilities'] = bot.send_message(chat_id, f'<b>Sector: Utilities &#x1F4A1</b> \n'
                                                                       f'{messages["utilities_message"]}',
                                                              parse_mode='html',
                                                              disable_web_page_preview=True).message_id
            start_message_sent = messages['utilities_message']
    for key in user_subscriptions[chat_id]:
        user_subscriptions[chat_id][key] = True

@bot.message_handler(commands=['consumer'])
def consumer(message):
    set_chat_id(message)
    global consumer_message_sent
    if not user_subscriptions[chat_id]['Consumer']:
        if messages['consumer_message'] != '':
            user_edits[chat_id]['consumer'] = bot.send_message(chat_id, f'You subscribed to <b>Consumer Sector</b>\n'
                                                                      f'<b>Current Top:</b> \n'
                                                                      f'{messages["consumer_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['consumer_message'] == '':
                time.sleep(5)
            if consumer_message_sent != messages['consumer_message']:
                user_edits[chat_id]['consumer'] = bot.send_message(chat_id, f'You subscribed to <b>Consumer Sector</b>\n'
                                                                          f'<b>Current Top:</b> \n'
                                                                          f'{messages["consumer_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                consumer_message_sent = messages['consumer_message']
        user_subscriptions[chat_id]['Consumer'] = True
    else:
        user_subscriptions[chat_id]['Consumer'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Consumer Sector</b>', parse_mode='html')

@bot.message_handler(commands=['energy'])
def energy(message):
    set_chat_id(message)
    global energy_message_sent
    if not user_subscriptions[chat_id]['Energy']:
        if messages['energy_message'] != '':
            user_edits[chat_id]['energy'] = bot.send_message(chat_id, f'You subscribed to <b>Energy Sector</b>\n'
                                                                    f'<b>Current Top:</b> \n'
                                                                    f'{messages["energy_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['energy_message'] == '':
                time.sleep(5)
            if energy_message_sent != messages['energy_message']:
                user_edits[chat_id]['energy'] = bot.send_message(chat_id, f'You subscribed to <b>Energy Sector</b>\n'
                                                                        f'<b>Current Top:</b> \n'
                                                                        f'{messages["energy_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                energy_message_sent = messages['energy_message']
        user_subscriptions[chat_id]['Energy'] = True
    else:
        user_subscriptions[chat_id]['Energy'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Energy Sector</b>', parse_mode='html')

@bot.message_handler(commands=['financial'])
def financial(message):
    set_chat_id(message)
    global financial_message_sent
    if not user_subscriptions[chat_id]['Financial']:
        if messages['financial_message'] != '':
            user_edits[chat_id]['financial'] = bot.send_message(chat_id, f'You subscribed to <b>Financial Sector</b>\n'
                                                                       f'<b>Current Top:</b> \n'
                                                                       f'{messages["financial_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['financial_message'] == '':
                time.sleep(5)
            if financial_message_sent != messages['financial_message']:
                user_edits[chat_id]['financial'] = bot.send_message(chat_id, f'You subscribed to <b>Financial Sector</b>\n'
                                                                           f'<b>Current Top:</b> \n'
                                                                           f'{messages["financial_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                financial_message_sent = messages['financial_message']
        user_subscriptions[chat_id]['Financial'] = True
    else:
        user_subscriptions[chat_id]['Financial'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Financial Sector</b>', parse_mode='html')

@bot.message_handler(commands=['government'])
def government(message):
    set_chat_id(message)
    global government_message_sent
    if not user_subscriptions[chat_id]['Government']:
        if messages['government_message'] != '':
            user_edits[chat_id]['government'] = bot.send_message(chat_id, f'You subscribed to <b>Government Sector</b>\n'
                                                                        f'<b>Current Top:</b> \n'
                                                                        f'{messages["government_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['government_message'] == '':
                time.sleep(5)
            if government_message_sent != messages['government_message']:
                user_edits[chat_id]['government'] = bot.send_message(chat_id, f'You subscribed to <b>Government Sector</b>\n'
                                                                            f'<b>Current Top:</b> \n'
                                                                            f'{messages["government_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                government_message_sent = messages['government_message']
        user_subscriptions[chat_id]['Government'] = True
    else:
        user_subscriptions[chat_id]['Government'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Government Sector</b>', parse_mode='html')

@bot.message_handler(commands=['health_care'])
def health_care(message):
    set_chat_id(message)
    global health_care_message_sent
    if not user_subscriptions[chat_id]['Health Care']:
        if messages['health_care_message'] != '':
            user_edits[chat_id]['health_care'] = bot.send_message(chat_id, f'You subscribed to <b>Health Care Sector</b>\n'
                                                                         f'<b>Current Top:</b> \n'
                                                                         f'{messages["health_care_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['health_care_message'] == '':
                time.sleep(5)
            if health_care_message_sent != messages['health_care_message']:
                user_edits[chat_id]['health_care'] = bot.send_message(chat_id, f'You subscribed to <b>Health Care Sector</b>\n'
                                                                             f'<b>Current Top:</b> \n'
                                                                             f'{messages["health_care_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                health_care_message_sent = messages['health_care_message']
        user_subscriptions[chat_id]['Health Care'] = True
    else:
        user_subscriptions[chat_id]['Health Care'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Health Care Sector</b>', parse_mode='html')

@bot.message_handler(commands=['industrials'])
def industrials(message):
    set_chat_id(message)
    global industrials_message_sent
    if not user_subscriptions[chat_id]['Industrials']:
        if messages['industrials_message'] != '':
            user_edits[chat_id]['industrials'] = bot.send_message(chat_id, f'You subscribed to <b>Industrials Sector</b>\n'
                                                                         f'<b>Current Top:</b> \n'
                                                                         f'{messages["industrials_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['industrials_message'] == '':
                time.sleep(5)
            if industrials_message_sent != messages['industrials_message']:
                user_edits[chat_id]['industrials'] = bot.send_message(chat_id, f'You subscribed to <b>Industrials Sector</b>\n'
                                                                             f'<b>Current Top:</b> \n'
                                                                             f'{messages["industrials_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                industrials_message_sent = messages['industrials_message']
        user_subscriptions[chat_id]['Industrials'] = True
    else:
        user_subscriptions[chat_id]['Industrials'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Industrials Sector</b>', parse_mode='html')

@bot.message_handler(commands=['it'])
def it(message):
    set_chat_id(message)
    global it_message_sent
    if not user_subscriptions[chat_id]['IT']:
        if messages['it_message'] != '':
            user_edits[chat_id]['it'] = bot.send_message(chat_id, f'You subscribed to <b>IT Sector</b>\n'
                                                                f'<b>Current Top:</b> \n'
                                                                f'{messages["it_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['it_message'] == '':
                time.sleep(5)
            if it_message_sent != messages['it_message']:
                user_edits[chat_id]['it'] = bot.send_message(chat_id, f'You subscribed to <b>IT Sector</b>\n'
                                                                    f'<b>Current Top:</b> \n'
                                                                    f'{messages["it_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                it_message_sent = messages['it_message']
        user_subscriptions[chat_id]['IT'] = True
    else:
        user_subscriptions[chat_id]['IT'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>IT Sector</b>', parse_mode='html')

@bot.message_handler(commands=['materials'])
def materials(message):
    set_chat_id(message)
    global materials_message_sent
    if not user_subscriptions[chat_id]['Materials']:
        if messages['materials_message'] != '':
            user_edits[chat_id]['materials'] = bot.send_message(chat_id, f'You subscribed to <b>Materials Sector</b>\n'
                                                                       f'<b>Current Top:</b> \n'
                                                                       f'{messages["materials_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['materials_message'] == '':
                time.sleep(5)
            if materials_message_sent != messages['materials_message']:
                user_edits[chat_id]['materials'] = bot.send_message(chat_id, f'You subscribed to <b>Materials Sector</b>\n'
                                                                           f'<b>Current Top:</b> \n'
                                                                           f'{messages["materials_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                materials_message_sent = messages['materials_message']
        user_subscriptions[chat_id]['Materials'] = True
    else:
        user_subscriptions[chat_id]['Materials'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Materials Sector</b>', parse_mode='html')

@bot.message_handler(commands=['municipal'])
def municipal(message):
    set_chat_id(message)
    global municipal_message_sent
    if not user_subscriptions[chat_id]['Municipal']:
        if messages['municipal_message'] != '':
            user_edits[chat_id]['municipal'] = bot.send_message(chat_id, f'You subscribed to <b>Municipal Sector</b>\n'
                                                                       f'<b>Current Top:</b> \n'
                                                                       f'{messages["municipal_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['municipal_message'] == '':
                time.sleep(5)
            if municipal_message_sent != messages['municipal_message']:
                user_edits[chat_id]['municipal'] = bot.send_message(chat_id, f'You subscribed to <b>Municipal Sector</b>\n'
                                                                           f'<b>Current Top:</b> \n'
                                                                           f'{messages["municipal_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                municipal_message_sent = messages['municipal_message']
        user_subscriptions[chat_id]['Municipal'] = True
    else:
        user_subscriptions[chat_id]['Municipal'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Municipal Sector</b>', parse_mode='html')

@bot.message_handler(commands=['other'])
def other(message):
    set_chat_id(message)
    global other_message_sent
    if not user_subscriptions[chat_id]['Other']:
        if messages['other_message'] != '':
            user_edits[chat_id]['other'] = bot.send_message(chat_id, f'You subscribed to <b>Other Sector</b>\n'
                                                                   f'<b>Current Top:</b> \n'
                                                                   f'{messages["other_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['other_message'] == '':
                time.sleep(5)
            if other_message_sent != messages['other_message']:
                user_edits[chat_id]['other'] = bot.send_message(chat_id, f'You subscribed to <b>Other Sector</b>\n'
                                                                       f'<b>Current Top:</b> \n'
                                                                       f'{messages["other_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                other_message_sent = messages['other_message']
        user_subscriptions[chat_id]['Other'] = True
    else:
        user_subscriptions[chat_id]['Other'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Other Sector</b>', parse_mode='html')

@bot.message_handler(commands=['real_estate'])
def real_estate(message):
    set_chat_id(message)
    global real_estate_message_sent
    if not user_subscriptions[chat_id]['Real Estate']:
        if messages['real_estate_message'] != '':
            user_edits[chat_id]['real_estate'] = bot.send_message(chat_id, f'You subscribed to <b>Real Estate Sector</b>\n'
                                                                         f'<b>Current Top:</b> \n'
                                                                         f'{messages["real_estate_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['real_estate_message'] == '':
                time.sleep(5)
            if real_estate_message_sent != messages['real_estate_message']:
                user_edits[chat_id]['real_estate'] = bot.send_message(chat_id, f'You subscribed to <b>Real Estate Sector</b>\n'
                                                                             f'<b>Current Top:</b> \n'
                                                                             f'{messages["real_estate_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                real_estate_message_sent = messages['real_estate_message']
        user_subscriptions[chat_id]['Real Estate'] = True
    else:
        user_subscriptions[chat_id]['Real Estate'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Real Estate Sector</b>', parse_mode='html')

@bot.message_handler(commands=['telecom'])
def telecom(message):
    set_chat_id(message)
    global telecom_message_sent
    if not user_subscriptions[chat_id]['Telecom']:
        if messages['telecom_message'] != '':
            user_edits[chat_id]['telecom'] = bot.send_message(chat_id, f'You subscribed to <b>Telecom Sector</b>\n'
                                                                     f'<b>Current Top:</b> \n'
                                                                     f'{messages["telecom_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['telecom_message'] == '':
                time.sleep(5)
            if telecom_message_sent != messages['telecom_message']:
                user_edits[chat_id]['telecom'] = bot.send_message(chat_id, f'You subscribed to <b>Telecom Sector</b>\n'
                                                                         f'<b>Current Top:</b> \n'
                                                                         f'{messages["telecom_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                telecom_message_sent = messages['telecom_message']
        user_subscriptions[chat_id]['Telecom'] = True
    else:
        user_subscriptions[chat_id]['Telecom'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Telecom Sector</b>', parse_mode='html')

@bot.message_handler(commands=['utilities'])
def utilities(message):
    set_chat_id(message)
    global utilities_message_sent
    if not user_subscriptions[chat_id]['Utilities']:
        if messages['utilities_message'] != '':
            user_edits[chat_id]['utilities'] = bot.send_message(chat_id, f'You subscribed to <b>Utilities Sector</b>\n'
                                                                       f'<b>Current Top:</b> \n'
                                                                       f'{messages["utilities_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
        else:
            bot.send_message(chat_id, f'The bot is retrieving the required information. Please, wait for a bit.')
            while messages['utilities_message'] == '':
                time.sleep(5)
            if utilities_message_sent != messages['utilities_message']:
                user_edits[chat_id]['utilities'] = bot.send_message(chat_id, f'You subscribed to <b>Utilities Sector</b>\n'
                                                                           f'<b>Current Top:</b> \n'
                                                                           f'{messages["utilities_message"]}', parse_mode='html', disable_web_page_preview=True).message_id
                utilities_message_sent = messages['utilities_message']
        user_subscriptions[chat_id]['Utilities'] = True
    else:
        user_subscriptions[chat_id]['Utilities'] = False
        bot.send_message(chat_id, f'You unsubscribed from <b>Utilities Sector</b>', parse_mode='html')

@bot.message_handler(commands=['stop'])
def it(message):
    set_chat_id(message)
    for key in user_subscriptions[chat_id]:
        user_subscriptions[chat_id][key] = False
    bot.send_message(chat_id, f'You unsubscribed from all sectors', parse_mode='html', disable_web_page_preview=True)

def bot_polling():
    while True:
        try:
            bot.polling()
            break
        except Exception as e:
            print('Ошибка bot.polling', e)

bot_thread = threading.Thread(target=bot_polling)
bot_thread.start()

while True:
    try:
        with Client(token) as client:
            r = client.instruments.bonds(
                instrument_status=1
            )

        name_bonds, ticker_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, sector_bonds = {}, {}, {}, {}, {}, {}

        print('Получение списка облигаций и данных по ним по критериям…')
        for bond in r.instruments:
            if bond.currency == 'rub' \
                    and not bond.floating_coupon_flag \
                    and not bond.amortization_flag \
                    and not bond.perpetual_flag \
                    and bond.country_of_risk == 'RU' \
                    and bond.nominal.units + (bond.nominal.nano * 10 ** (-9)) == 1000.0 \
                    and bond.nominal.currency == 'rub' \
                    and (bond.risk_level.numerator == 1 or bond.risk_level.numerator == 2):
                instrument_id = bond.uid
                ticker = bond.ticker
                name = bond.name
                maturity_date = bond.maturity_date.strftime('%Y-%m-%d')
                nominal = bond.nominal.units + (bond.nominal.nano * 10 ** (-9))
                aci_value = bond.aci_value.units + (bond.aci_value.nano * 10 ** (-9))
                sector = bond.sector

                name_bonds[instrument_id] = name
                ticker_bonds[instrument_id] = ticker
                maturity_date_bonds[instrument_id] = maturity_date
                nominal_bonds[instrument_id] = nominal
                aci_bonds[instrument_id] = aci_value
                sector_bonds[instrument_id] = sector
        break
    except Exception as e:
        print('Ошибка получения списка облигаций', e)

uid_bonds = []
credit_rating_bonds = {}

while True:
    try:
        print('Парсинг Smart-lab для отфильтровки списка облигаций по кредитному рейтингу и наличию оферт/кол-опционов…')
        for bond_ticker in tqdm(ticker_bonds):
            url = f'https://smart-lab.ru/q/bonds/{ticker_bonds[bond_ticker]}/'
            r = requests.get(url)
            soup = bs(r.text, 'lxml')
            if soup.find(string=lambda text: text and 'Сектор' in text).find_next('div', class_='quotes-simple-table__item').text.strip() in ('ОФЗ', 'Субфедеральные'):
                credit_rating = 'AAA+'
            elif soup.find('div', class_='linear-progress-bar__text') is None:
                credit_rating = '-'
            else:
                credit_rating = soup.find('div', class_='linear-progress-bar__text').text.strip()
            offer_date = soup.find('a', class_='blue-link').find_next('div', class_='quotes-simple-table__item').text.strip()
            if (offer_date == '—' or datetime.strptime(offer_date, '%d-%m-%Y').date() < date.today()) \
                and credit_rating in ('BBB-', 'BBB', 'BBB+', 'A-', 'A', 'A+', 'AA-', 'AA', 'AA+', 'AAA-', 'AAA', 'AAA+'):
                    uid_bonds.append(bond_ticker)
                    credit_rating_bonds[bond_ticker] = credit_rating
        break
    except Exception as e:
        print('Ошибка парсинга Smart-lab', e)

uid_for_bonds = []

while True:
    try:
        print('Подтверждение итогового списка облигаций…')
        for uid in uid_bonds:
            if uid in name_bonds.keys() and uid in ticker_bonds.keys() and uid in maturity_date_bonds.keys() and nominal_bonds.keys() and aci_bonds.keys() and sector_bonds.keys() and datetime.strptime(maturity_date_bonds[uid], '%Y-%m-%d').date() > date.today():
                uid_for_bonds.append(uid)
        break
    except Exception as e:
        print('Ошибка подтверждения итогового списка облигаций', e)

while True:
    try:
        print('Установка временных интервалов…')
        if datetime.today().isoweekday() == 5:
            day = datetime.today() + timedelta(days=3)
        elif datetime.today().isoweekday() == 6:
            day = datetime.today() + timedelta(days=2)
        else:
            day = datetime.today() + timedelta(days=1)
        break
    except Exception as e:
        print('Ошибка установки временных интервалов', e)

coupon_sums = {}

while True:
    try:
        print('Подсчёт сумм купонных выплат за весь период для всех облигаций…')
        for instrument_id in tqdm(uid_for_bonds):
            with Client(token) as client:
                r = client.instruments.get_bond_coupons(
                    instrument_id = instrument_id,
                    from_ = day,
                    to = datetime.today() + timedelta(weeks=49999)
                )

            coupons_quantity = len(r.events)

            if len(r.events) != 0:
                coupon_payment = r.events[0].pay_one_bond.units + (r.events[0].pay_one_bond.nano * 10 ** (-9))
                coupon_sum = coupon_payment * coupons_quantity
            else:
                coupon_sum = 0

            coupon_sums[instrument_id] = coupon_sum
        break
    except Exception as e:
        print('Ошибка подсчёта сумм купонных выплат', e)

last_prices = {}
volume_bonds = {}
profitability_bonds = {}
profitabilityyear_bonds = {}
consumer_bonds, energy_bonds, financial_bonds, government_bonds, health_care_bonds, industrials_bonds, it_bonds, materials_bonds, municipal_bonds, other_bonds, real_estate_bonds, telecom_bonds, utilities_bonds = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

while True:
    try:
        print('Запрос исторических данных по объёмам за сегодня…')
        for uid in tqdm(uid_for_bonds):
            with Client(token) as client:
                r = client.market_data.get_candles(
                    interval=CandleInterval.CANDLE_INTERVAL_DAY,
                    instrument_id=uid,
                    from_=datetime.today(),
                    to=datetime.today() + timedelta(days=1)
                )
                for candle in r.candles:
                    volume = candle.volume
                    volume_bonds[uid] = volume
        break
    except Exception as e:
        print('Ошибка запроса исторических объёмов', e)

while True:
    try:
        print('Запуск стрима объёмов…')
        async def volumes_stream():
            async def request_iterator():
                yield MarketDataRequest(
                    subscribe_candles_request=SubscribeCandlesRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                        waiting_close=False,
                        instruments=[
                            CandleInstrument(
                                instrument_id=uid,
                                interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY
                            ) for uid in uid_for_bonds
                        ]
                    )
                )
                while True:
                    await asyncio.sleep(0)

            async with AsyncClient(token) as client:
                async for r in client.market_data_stream.market_data_stream(
                    request_iterator()
                ):
                    if r.candle != None:
                        instrument_id = r.candle.instrument_uid
                        volume = r.candle.volume
                        volume_bonds[instrument_id] = volume
        break
    except Exception as e:
        print('Ошибка запуска стрима объёмов', e)

while True:
    try:
        print('Запрос исторических последних цен…')
        with Client(token) as client:
            r = client.market_data.get_last_prices(
                instrument_id=uid_for_bonds
            )

        for last_price in r.last_prices:
            instrument_id = last_price.instrument_uid
            price = (last_price.price.units + (last_price.price.nano * 10 ** (-9))) * 10

            if price != 0:
                last_prices[instrument_id] = price

                profitability_bonds[instrument_id] = \
                    (coupon_sums[instrument_id] - aci_bonds[instrument_id] + (nominal_bonds[instrument_id] - last_prices[instrument_id] * 1.0005))/(last_prices[instrument_id] * 1.0005 + aci_bonds[instrument_id])

                profitabilityyear_bonds[instrument_id] = \
                    round(((profitability_bonds[instrument_id] + 1) ** (365/((datetime.strptime(maturity_date_bonds[instrument_id], '%Y-%m-%d').date() - date.today()).days)) - 1) * 100, 2)
        break
    except Exception as e:
        print('Ошибка получения исторических последних цен', e)

while True:
    try:
        print('Запуск стрима последних цен…')
        async def last_prices_stream():
            max_consumer0, max_energy0, max_financial0, max_government0, max_health_care0, max_industrials0, max_it0, max_materials0, max_municipal0, max_other0, max_real_estate0, max_telecom0, max_utilities0 = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            async def request_iterator():
                yield MarketDataRequest(
                    subscribe_last_price_request=SubscribeLastPriceRequest(
                        subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                        instruments=[
                            LastPriceInstrument(
                                instrument_id=uid
                            ) for uid in uid_bonds
                        ]
                    )
                )
                while True:
                    await asyncio.sleep(0)

            async with AsyncClient(token) as client:
                async for r in client.market_data_stream.market_data_stream(
                    request_iterator()
                ):
                    consumer_message, energy_message, financial_message, government_message, health_care_message, industrials_message, it_message, materials_message, municipal_message, other_message, real_estate_message, telecom_message, utilities_message = '', '', '', '', '', '', '', '', '', '', '', '', ''
                    if r.last_price is not None and r.last_price != 0:
                        instrument_id = r.last_price.instrument_uid
                        price = (r.last_price.price.units + (r.last_price.price.nano * 10 ** (-9))) * 10
                        last_prices[instrument_id] = price

                        profitability_bonds[instrument_id] = \
                            (coupon_sums[instrument_id] - aci_bonds[instrument_id] + (
                                        nominal_bonds[instrument_id] - last_prices[instrument_id] * 1.0005)) / (
                                        last_prices[instrument_id] * 1.0005 + aci_bonds[instrument_id])

                        profitabilityyear_bonds[instrument_id] = \
                            round(((profitability_bonds[instrument_id] + 1) ** (365 / ((datetime.strptime(
                                maturity_date_bonds[instrument_id], '%Y-%m-%d').date() - date.today()).days)) - 1) * 100, 2)
                    displayed_profit = {instrument_id: (name_bonds[instrument_id], sector_bonds[instrument_id], profitabilityyear_bonds[instrument_id], last_prices[instrument_id], ticker_bonds[instrument_id], maturity_date_bonds[instrument_id], credit_rating_bonds[instrument_id], volume_bonds.get(instrument_id, 0)) for instrument_id in profitabilityyear_bonds}

                    for dispprofit in displayed_profit.values():
                        if dispprofit[1] == 'consumer':
                            consumer_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'energy':
                            energy_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'financial':
                            financial_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'government':
                            government_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'health_care':
                            health_care_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'industrials':
                            industrials_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'it':
                            it_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'materials':
                            materials_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'municipal':
                            municipal_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'other':
                            other_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'real_estate':
                            real_estate_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'telecom':
                            telecom_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                        elif dispprofit[1] == 'utilities':
                            utilities_bonds[dispprofit[0]] = (dispprofit[2], dispprofit[3], dispprofit[4], dispprofit[5], dispprofit[6], dispprofit[7])

                    if len(consumer_bonds) >= 3:
                        max_consumer1 = sorted(consumer_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_consumer1 = sorted(consumer_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_consumer1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        consumer_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_consumer1] != [item[0] for item in max_consumer0]:
                        max_consumer0 = max_consumer1
                        print('-' * 173)
                        print('Sector: Consumer')
                        for bond in max_consumer1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Consumer'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['consumer'])
                                user_edits[chat_id]['consumer'] = bot.send_message(chat_id, f'<b>Sector: Consumer &#x1F6D2</b> \n'
                                                                                          f'{consumer_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Consumer'] and messages['consumer_message'] != consumer_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['consumer'], text=f'<b>Sector: Consumer &#x1F6D2</b> \n'
                                                                                                                      f'{consumer_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['consumer_message'] = consumer_message

                    if len(energy_bonds) >= 3:
                        max_energy1 = sorted(energy_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_energy1 = sorted(energy_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_energy1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        energy_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_energy1] != [item[0] for item in max_energy0]:
                        max_energy0 = max_energy1
                        print('-' * 173)
                        print('Sector: Energy')
                        for bond in max_energy1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Energy'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['energy'])
                                user_edits[chat_id]['energy'] = bot.send_message(chat_id, f'<b>Sector: Energy &#x1F50B</b> \n'
                                                                                        f'{energy_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Energy'] and messages['energy_message'] != energy_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['energy'], text=f'<b>Sector: Energy &#x1F50B</b> \n'
                                                                                                                    f'{energy_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['energy_message'] = energy_message

                    if len(financial_bonds) >= 3:
                        max_financial1 = sorted(financial_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_financial1 = sorted(financial_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_financial1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        financial_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_financial1] != [item[0] for item in max_financial0]:
                        max_financial0 = max_financial1
                        print('-' * 173)
                        print('Sector: Financial')
                        for bond in max_financial1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Financial'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['financial'])
                                user_edits[chat_id]['financial'] = bot.send_message(chat_id, f'<b>Sector: Financial &#x1F4B5</b> \n'
                                                                                           f'{financial_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Financial'] and messages['financial_message'] != financial_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['financial'], text=f'<b>Sector: Financial &#x1F4B5</b> \n'
                                                                                                                       f'{financial_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['financial_message'] = financial_message

                    if len(government_bonds) >= 3:
                        max_government1 = sorted(government_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_government1 = sorted(government_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_government1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        government_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_government1] != [item[0] for item in max_government0]:
                        max_government0 = max_government1
                        print('-' * 173)
                        print('Sector: Government')
                        for bond in max_government1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Government'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['government'])
                                user_edits[chat_id]['government'] = bot.send_message(chat_id, f'<b>Sector: Government &#x1F3DB</b> \n'
                                                                                            f'{government_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Government') and messages['government_message'] != government_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['government'], text=f'<b>Sector: Government &#x1F3DB</b> \n'
                                                                                            f'{government_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['government_message'] = government_message


                    if len(health_care_bonds) >= 3:
                        max_health_care1 = sorted(health_care_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_health_care1 = sorted(health_care_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_health_care1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        health_care_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_health_care1] != [item[0] for item in max_health_care0]:
                        max_health_care0 = max_health_care1
                        print('-' * 173)
                        print('Sector: Health Care')
                        for bond in max_health_care1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Health Care'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['health_care'])
                                user_edits[chat_id]['health_care'] = bot.send_message(chat_id, f'<b>Sector: Health Care &#x1F48A</b> \n'
                                                                                             f'{health_care_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Health Care'] and messages['health_care_message'] != health_care_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['health_care'], text=f'<b>Sector: Health Care &#x1F48A</b> \n'
                                                                                                                         f'{health_care_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['health_care_message'] = health_care_message

                    if len(industrials_bonds) >= 3:
                        max_industrials1 = sorted(industrials_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_industrials1 = sorted(industrials_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_industrials1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        industrials_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_industrials1] != [item[0] for item in max_industrials0]:
                        max_industrials0 = max_industrials1
                        print('-' * 173)
                        print('Sector: Industrials')
                        for bond in max_industrials1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Industrials'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['industrials'])
                                user_edits[chat_id]['industrials'] = bot.send_message(chat_id, f'<b>Sector: Industrials &#x1F3D7</b> \n'
                                                                                             f'{industrials_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Industrials'] and messages['industrials_message'] != industrials_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['industrials'], text=f'<b>Sector: Industrials &#x1F3D7</b> \n'
                                                                                                                         f'{industrials_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['industrials_message'] = industrials_message

                    if len(it_bonds) >= 3:
                        max_it1 = sorted(it_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_it1 = sorted(it_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_it1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        it_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_it1] != [item[0] for item in max_it0]:
                        max_it0 = max_it1
                        print('-' * 173)
                        print('Sector: IT')
                        for bond in max_it1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('IT'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['it'])
                                user_edits[chat_id]['it'] = bot.send_message(chat_id, f'<b>Sector: IT &#x1F4BB</b> \n'
                                                                                    f'{it_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['IT'] and messages['it_message'] != it_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['it'], text=f'<b>Sector: IT &#x1F4BB</b> \n'
                                                                                                                f'{it_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['it_message'] = it_message

                    if len(materials_bonds) >= 3:
                        max_materials1 = sorted(materials_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_materials1 = sorted(materials_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_materials1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        materials_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_materials1] != [item[0] for item in max_materials0]:
                        max_materials0 = max_materials1
                        print('-' * 173)
                        print('Sector: Materials')
                        for bond in max_materials1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Materials'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['materials'])
                                user_edits[chat_id]['materials'] = bot.send_message(chat_id, f'<b>Sector: Materials &#x1F9F1</b> \n'
                                                                                           f'{materials_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Materials'] and messages['materials_message'] != materials_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['materials'], text=f'<b>Sector: Materials &#x1F9F1</b> \n'
                                                                                                                       f'{materials_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['materials_message'] = materials_message

                    if len(municipal_bonds) >= 3:
                        max_municipal1 = sorted(municipal_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_municipal1 = sorted(municipal_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_municipal1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        municipal_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_municipal1] != [item[0] for item in max_municipal0]:
                        max_municipal0 = max_municipal1
                        print('-' * 173)
                        print('Sector: Municipal')
                        for bond in max_municipal1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Municipal'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['municipal'])
                                user_edits[chat_id]['municipal'] = bot.send_message(chat_id, f'<b>Sector: Municipal &#x1F306</b> \n'
                                                                                           f'{municipal_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Municipal'] and messages['municipal_message'] != municipal_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['municipal'], text=f'<b>Sector: Municipal &#x1F306</b> \n'
                                                                                                                       f'{municipal_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['municipal_message'] = municipal_message

                    if len(other_bonds) >= 3:
                        max_other1 = sorted(other_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_other1 = sorted(other_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_other1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        other_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_other1] != [item[0] for item in max_other0]:
                        max_other0 = max_other1
                        print('-' * 173)
                        print('Sector: Other')
                        for bond in max_other1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Other'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['other'])
                                user_edits[chat_id]['other'] = bot.send_message(chat_id, f'<b>Sector: Other &#x1F441</b> \n'
                                                                                       f'{other_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Other'] and messages['other_message'] != other_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['other'], text=f'<b>Sector: Other &#x1F441</b> \n'
                                                                                                                   f'{other_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['other_message'] = other_message

                    if len(real_estate_bonds) >= 3:
                        max_real_estate1 = sorted(real_estate_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_real_estate1 = sorted(real_estate_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_real_estate1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        real_estate_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_real_estate1] != [item[0] for item in max_real_estate0]:
                        max_real_estate0 = max_real_estate1
                        print('-' * 173)
                        print('Sector: Real Estate')
                        for bond in max_real_estate1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Real Estate'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['real_estate'])
                                user_edits[chat_id]['real_estate'] = bot.send_message(chat_id, f'<b>Sector: Real Estate &#x1F3E0</b> \n'
                                                                                             f'{real_estate_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Real Estate'] and messages['real_estate_message'] != real_estate_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['real_estate'], text=f'<b>Sector: Real Estate &#x1F3E0</b> \n'
                                                                                                                         f'{real_estate_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['real_estate_message'] = real_estate_message

                    if len(telecom_bonds) >= 3:
                        max_telecom1 = sorted(telecom_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_telecom1 = sorted(telecom_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_telecom1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        telecom_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_telecom1] != [item[0] for item in max_telecom0]:
                        max_telecom0 = max_telecom1
                        print('-' * 173)
                        print('Sector: Telecom')
                        for bond in max_telecom1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Telecom'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['telecom'])
                                user_edits[chat_id]['telecom'] = bot.send_message(chat_id, f'<b>Sector: Telecom &#x1F4E1</b> \n'
                                                                                         f'{telecom_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Telecom'] and messages['telecom_message'] != telecom_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['telecom'], text=f'<b>Sector: Telecom &#x1F4E1</b> \n'
                                                                                                                     f'{telecom_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['telecom_message'] = telecom_message

                    if len(utilities_bonds) >= 3:
                        max_utilities1 = sorted(utilities_bonds.items(), key=lambda x: x[1][0], reverse=True)[:3]
                    else:
                        max_utilities1 = sorted(utilities_bonds.items(), key=lambda x: x[1][0], reverse=True)
                    for bond in max_utilities1:
                        if len(bond[0]) > 15:
                            nameshorten = bond[0][:12] + '…'
                        else:
                            nameshorten = bond[0]
                        utilities_message += f'<a href="https://www.tinkoff.ru/invest/bonds/{bond[1][2]}/#({(datetime.strptime(bond[1][3], "%Y-%m-%d").date() - date.today()).days} days, {bond[1][4]} rating, {bond[1][5]} lots)">{nameshorten}</a> -- <b>{bond[1][0]}%</b> -- {round(bond[1][1], 2)}₽\n'
                    if [item[0] for item in max_utilities1] != [item[0] for item in max_utilities0]:
                        max_utilities0 = max_utilities1
                        print('-' * 173)
                        print('Sector: Utilities')
                        for bond in max_utilities1:
                            print(bond[0], ' ', bond[1][0], '%', ' ', round(bond[1][1], 2), sep='')
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id].get('Utilities'):
                                bot.delete_message(chat_id=chat_id, message_id=user_edits[chat_id]['utilities'])
                                user_edits[chat_id]['utilities'] = bot.send_message(chat_id, f'<b>Sector: Utilities &#x1F4A1</b> \n'
                                                                                           f'{utilities_message}', parse_mode='html', disable_web_page_preview=True).message_id
                    else:
                        for chat_id in user_subscriptions:
                            if chat_id in user_subscriptions and user_subscriptions[chat_id]['Utilities'] and messages['utilities_message'] != utilities_message:
                                bot.edit_message_text(chat_id=chat_id, message_id=user_edits[chat_id]['utilities'], text=f'<b>Sector: Utilities &#x1F4A1</b> \n'
                                                                                                                       f'{utilities_message}', parse_mode='html', disable_web_page_preview=True)
                    messages['utilities_message'] = utilities_message
        break
    except Exception as e:
        print('Ошибка запуска стрима подписок', e)

async def streams():
    await asyncio.gather(volumes_stream(), last_prices_stream())

if __name__ == "__main__":
    while True:
        try:
            print('Запуск стримов изменения объёмов и последних цен…')
            asyncio.run(streams())
            break
        except Exception as e:
            print('Ошибка стримов изменения объёмов и последних цен')