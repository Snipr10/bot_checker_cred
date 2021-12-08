from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta

import dateutil.parser

import schedule
import time
import telebot
import requests

# logging.basicConfig(filename='prod.log', level=logging.INFO)

bot = telebot.TeleBot('1893821469:AAEd2nx3KBlvxrUv3orEgy35eoDBJ36o0HE')


URL_TG_API = "http://194.50.24.4:8000/api/"


trouble_exist = []


@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, f'Привет, я буду присылать тебе статистику')


@bot.message_handler(commands=['statistic'])
def send_statistic(message):
    bot.reply_to(message, statistic(), parse_mode='Markdown')


fb_proxy_limit = 200
fb_worker_limit = 30
fb_balance_limit = 100

tg_proxy_limit = 600
tg_bot_limit = 600
tg_balance_limit = 100

vk_bot_limit = 2_500
vk_proxy_limit = 2_500

tw_proxy_limit = 2_500

ig_bot_limit = 600
ig_proxy_limit = 1_000

yt_proxy_limit = 250

smi_proxy_limit = 1_000

proxy_all_limit = 3_000

# proxy_without_smi_limit = 4500
# proxy_smi_limit = 4500


def statistic():
    message = ''
    fb_proxy = 0
    try:
        fb = requests.get('http://194.50.24.4:7999/api/statistic').json()
        fb_proxy = fb['proxy']
        fb_worker = fb['worker']
        fb_balance = fb['balance']
        if fb_proxy < fb_proxy_limit:
            message += f'Недостаточно прокси *fb*: _{fb_proxy}_, минимум _{fb_proxy_limit}_ \n'
        if fb_worker < fb_worker_limit:
            message += f'Недостаточно ботов *fb*: _{fb_worker}_, минимум _{fb_worker_limit}_ \n'
        if fb_balance < fb_balance_limit:
            message += f'Недостаточно денег на активаторе *fb*: _{fb_balance}_, минимум _{fb_balance_limit}_ \n'
    except Exception:
        message += f'Не могу получить данные из *fb* \n'
    try:
        parsing_data = requests.get('http://194.50.24.4:8000/api/statistic').json()
        try:
            tg = parsing_data['tg']
            tg_bots = tg['bots']
            tg_proxy = tg['proxy']
            tg_balance = tg['balance']
            if tg_proxy < tg_proxy_limit:
                message += f'Недостаточно прокси *tg*: _{tg_proxy}_, минимум _{tg_proxy_limit}_ \n'
            if tg_bots < tg_bot_limit:
                message += f'Недостаточно ботов *tg*: _{tg_bots}_, минимум _{tg_bot_limit}_ \n'
            if tg_balance < tg_balance_limit:
                message += f'Недостаточно денег на активаторе *tg*: _{tg_balance}_, минимум _{tg_balance_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные из *tg* \n'
        try:
            vk = parsing_data['vk']
            vk_bots = vk['bots']
            vk_proxy = vk['proxy']
            if vk_bots < vk_bot_limit:
                message += f'Недостаточно ботов *vk*: _{vk_bots}_, минимум _{vk_bot_limit}_ \n'
            if vk_proxy < vk_proxy_limit:
                message += f'Недостаточно прокси *vk*: _{vk_proxy}_, минимум _{vk_proxy_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные из *vk* \n'
        try:
            tw = parsing_data['tw']
            tw_proxy = tw['proxy']
            if tw_proxy < tw_proxy_limit:
                message += f'Недостаточно прокси *tw*: _{tw_proxy}_, минимум _{tw_proxy_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные из *tw* \n'
        try:
            ig = parsing_data['ig']
            ig_bots = ig['bots']
            ig_proxy = ig['proxy']
            if ig_bots < ig_bot_limit:
                message += f'Недостаточно ботов *ig*: _{ig_bots}_, минимум _{ig_bot_limit}_ \n'
            if ig_proxy < ig_proxy_limit:
                message += f'Недостаточно прокси *ig*: _{ig_proxy}_, минимум _{ig_proxy_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные из *ig* \n'
        try:
            proxy_smi = parsing_data['proxy_smi']
            if proxy_smi < smi_proxy_limit:
                message += f'Недостаточно прокси для *сми*: _{proxy_smi}_, минимум _{smi_proxy_limit}_ \n'
        except Exception:
            message += f'Не могу получить прокси для *сим* \n'

        try:
            proxy_without_smi = parsing_data['proxy_without_smi']
            proxy_all = proxy_without_smi + fb_proxy
            if proxy_all < proxy_all_limit:
                message += f'Недостаточно прокси: _{proxy_all}_, минимум _{proxy_all_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные по всем прокси \n'

    except Exception:
        message += f'Не могу получить данные по *соцсетям* \n'

    if message != '':
        if len(trouble_exist) == 0:
            trouble_exist.append(1)
    else:
        trouble_exist.clear()
    message += parsing_statistic()
    return message


def get_date(date):
    return dateutil.parser.isoparse(date).strftime("%H:%M:%S %d-%m-%Y")


def parsing_statistic():
    text_message = "\nЖдут обновления: \n"

    try:
        tasks_yt_tg_status = requests.get(URL_TG_API + "tasks_yt_tg_status")
        res_json = tasks_yt_tg_status.json()
        text_message += f"парсинг каналов *tg*: {get_date(res_json['tg_sources'])}; \n"
        text_message += f"поиск по ключам *yt*: {get_date(res_json['yt_keys'])}; \n"
        text_message += f"парсинг каналов *yt*: {get_date(res_json['yt_sources'])}; \n"
        # text_message += f"парсинг *СМИ*: {get_date(res_json['yt_sources'])}; \n"
        for site in res_json['sites']:
            text_message += f"парсинг *{site[0]}*: {get_date(site[1])}; \n"
        for site in res_json['sites_keys_res']:
            text_message += f"парсинг *{[*site][0]}*: {get_date([*site.values()][0]['first'])}; \n"
    except Exception as e:
        text_message += "Не удалось собрать статистику" + str(e)

    return text_message


def send_static_an_hour(message=None):
    try:
        if message is None:
            message = statistic()
        if message != '':
            bot.send_message('-535382146', message, parse_mode='Markdown')
    except Exception:
        try:
            bot.send_message('457180576', 'Что-то сломалось', parse_mode='Markdown')
        except Exception:
            pass


def send_static_new():
    if len(trouble_exist) == 0:
        send_static_an_hour()


def get_response_json(attempts=0):
    try:
        tasks_yt_tg_status = requests.get(URL_TG_API + "tasks_yt_tg_status")
        res_json = tasks_yt_tg_status.json()
        return res_json
    except Exception:
        time.sleep(60)
        attempts += 1
        if attempts > 3:
            return None
        else:
            return get_response_json(attempts)

def get_fb_response_json(attempts=0):
    try:
        return requests.get('http://194.50.24.4:7999/api/statistic').json()
    except Exception:
        time.sleep(60)
        attempts += 1
        if attempts > 3:
            return None
        else:
            return get_fb_response_json(attempts)

def checker():
    try:
        text = ""
        res_json = get_response_json()
        if res_json.get("bd") is not None:
            if res_json.get("bd"):
                text += f"*БД не отвечает*  \n"
        else:
            tg = dateutil.parser.isoparse(res_json['tg_last'])
            if tg.replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                text = "*TG не отчевает*  \n"
            yt = dateutil.parser.isoparse(res_json['yt_last'])
            if yt.replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                text = "*YT не отчевает*  \n"
            for site in res_json['sites']:
                if dateutil.parser.isoparse(site[1]).replace(tzinfo=None) < datetime.today() - timedelta(hours=2):
                    text += f"*{site[0]} не отчевает* \n"
            for site in res_json['sites_keys_res']:
                if dateutil.parser.isoparse([*site.values()][0]['last']).replace(tzinfo=None) < datetime.today() - timedelta(hours=2):
                    text += f"*{[*site][0]} не отчевает*  \n"
            fb = get_fb_response_json()
            if fb:
                if dateutil.parser.isoparse(fb['last_update']).replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                    text += f"*FB не отчевает* \n"
            else:
                text += f"*FB не отчевает* \n"
        if text:
            bot.send_message('-535382146', text, parse_mode='Markdown')
    except Exception as e:
        print(e)
        bot.send_message('-535382146', "Проверьте бота", parse_mode='Markdown')


schedule.every(10).minutes.do(send_static_new)
schedule.every(10).minutes.do(checker)
schedule.every().day.at("08:00").do(send_static_an_hour)
schedule.every().day.at("12:00").do(send_static_an_hour)
schedule.every().day.at("16:00").do(send_static_an_hour)
schedule.every().day.at("20:00").do(send_static_an_hour)

# schedule.every().minute.at .at(":30:30").do(send_static)


def start_bot():
    bot.polling(none_stop=True)


def start_sending_message():
    while True:
        schedule.run_pending()
        time.sleep(60)


pool_source = ThreadPoolExecutor(3)
pool_source.submit(start_sending_message)
pool_source.submit(start_bot)
pool_source.shutdown()
