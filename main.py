from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta

import dateutil.parser

import schedule
import time
import telebot
import requests
import asyncio
import httpx

TIMEOUT = 5 * 60

# logging.basicConfig(filename='prod.log', level=logging.INFO)

bot = telebot.TeleBot('1893821469:AAHZdGtFjG9SroQBRuX9YzmH4jjOhM_Pquc')


URL_TG_API = "http://194.50.24.4:8000/api/"


trouble_exist = []


@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, f'Привет, я буду присылать тебе статистику')


@bot.message_handler(commands=['statistic'])
def send_statistic(message):
    bot.reply_to(message, statistic(), parse_mode='Markdown')


fb_proxy_limit = 200
fb_bot_limit = 30
fb_balance_limit = 100

ok_bot_limit = 30

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
    fb_posts = "None"
    tg_posts = "None"
    yt_posts = "None"
    ok_posts = "None"
    # try:
    #     fb = requests.get('http://194.50.24.4:7999/api/statistic').json()
    #     fb_proxy = fb['proxy']
    #     fb_worker = fb['worker']
    #     fb_balance = fb['balance']
    #     fb_posts = fb['count']
    #     if fb_proxy < fb_proxy_limit:
    #         message += f'Недостаточно прокси *fb*: _{fb_proxy}_, минимум _{fb_proxy_limit}_ \n'
    #     if fb_worker < fb_worker_limit:
    #         message += f'Недостаточно ботов *fb*: _{fb_worker}_, минимум _{fb_worker_limit}_ \n'
    #     if fb_balance < fb_balance_limit:
    #         message += f'Недостаточно денег на активаторе *fb*: _{fb_balance}_, минимум _{fb_balance_limit}_ \n'
    # except Exception:
    #     message += f'Не могу получить данные из *fb* \n'
    try:
        parsing_data = requests.get('http://194.50.24.4:8000/api/statistic').json()
        try:
            tg = parsing_data['tg']
            tg_bots = tg['bots']
            tg_proxy = tg['proxy']
            tg_balance = tg['balance']
            tg_posts = tg['count']

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
        try:
            yt = parsing_data['yt']
            yt_posts = yt['count']
        except Exception:
            message += f'Не могу получить данные из *yt* \n'
        try:
            fb = parsing_data['fb']
            fb_posts = fb['count']
            fb_bot = fb['bot']
            if fb_bot < fb_bot_limit:
                message += f'Недостаточно ботов *fb*: _{fb_bot}_, минимум _{fb_bot_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные из *yt* \n'
        try:
            ok = parsing_data['ok']
            ok_posts = ok['count']
            ok_bot = ok['bot']
            if ok_bot < ok_bot_limit:
                message += f'Недостаточно ботов *ok*: _{ok_bot}_, минимум _{ok_bot_limit}_ \n'
        except Exception:
            message += f'Не могу получить данные из *yt* \n'
    except Exception:
        message += f'Не могу получить данные по *соцсетям* \n'

    if message != '':
        if len(trouble_exist) == 0:
            trouble_exist.append(1)
    else:
        trouble_exist.clear()
    message += parsing_statistic()

    message += "\n *Статистика парсинга:* \n"
    message += f"*tg:* {tg_posts} \n"
    message += f"*yt:* {yt_posts} \n"
    message += f"*fb:* {fb_posts} \n"
    message += f"*ok:* {ok_posts} \n"
    return message


def get_date(date):
    try:
        return dateutil.parser.isoparse(date).strftime("%d-%m-%Y %H:%M:%S")
    except Exception:
        return "new"


def parsing_statistic():
    text_message = "\nЖдут обновления: \n"

    try:
        tasks_yt_tg_status = requests.get(URL_TG_API + "tasks_yt_tg_status")
        res_json = tasks_yt_tg_status.json()
        text_message += f"парсинг каналов *tg*: {get_date(res_json['tg_sources'])}; \n"
        text_message += f"парсинг определенных каналов *tg*: {get_date(res_json['tg_sources_special'])}; \n"
        text_message += f"поиск по ключам *yt*: {get_date(res_json['yt_keys'])}; \n"
        text_message += f"парсинг каналов *yt*: {get_date(res_json['yt_sources'])}; \n"
        text_message += f"поиск по ключам *fb*: {get_date(res_json['fb_keys'])}; \n"
        text_message += f"парсинг каналов *fb*: {get_date(res_json['fb_sources'])}; \n"
        text_message += f"парсинг определенных каналов *fb*: {get_date(res_json['fb_sources_special'])}; \n"
        text_message += f"поиск по ключам *ok*: {get_date(res_json['ok_keys'])}; \n"
        text_message += f"парсинг каналов *ok*: {get_date(res_json['ok_sources'])}; \n"
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


def checker(attempt=0):
    try:
        text = ""
        res_json = get_response_json()
        print(res_json)
        print(res_json.get("bd"))
        if res_json.get("bd") is not None:
            if not res_json.get("bd"):
                text += f"*БД не отвечает*  \n"
        else:
            tg = dateutil.parser.isoparse(res_json['tg_last'])
            if tg.replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                text = "*TG не отвечает*  \n"
            yt = dateutil.parser.isoparse(res_json['yt_last'])
            if yt.replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                text = "*YT не отвечает*  \n"

            fb = dateutil.parser.isoparse(res_json['fb_last'])
            if fb.replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                text = "*FB не отвечает*  \n"

            ok = dateutil.parser.isoparse(res_json['ok_last'])
            if ok.replace(tzinfo=None) < datetime.today() - timedelta(hours=1):
                text = "*OK не отвечает*  \n"

            for site in res_json['sites']:
                if dateutil.parser.isoparse(site[1]).replace(tzinfo=None) < datetime.today() - timedelta(hours=2):
                    text += f"*{site[0]} не отвечает* \n"
            for site in res_json['sites_keys_res']:
                if dateutil.parser.isoparse([*site.values()][0]['last']).replace(tzinfo=None) < datetime.today() - timedelta(hours=2):
                    text += f"*{[*site][0]} не отвечает*  \n"
            # fb = get_fb_response_json()
            # print(fb)
            # if fb is not None:
            #     # utc + 3
            #     if dateutil.parser.isoparse(fb['last_update']).replace(tzinfo=None) < datetime.today() - timedelta(hours=5):
            #         text += f"*FB не отвечает* \n"
            # else:
            #     text += f"*FB не отвечает* \n"
        if text:
            bot.send_message('-535382146', text, parse_mode='Markdown')
    except Exception as e:
        if attempt < 5:
            attempt += 1
            return checker(attempt)
        else:
            bot.send_message('457180576', str(e), parse_mode='Markdown')
            bot.send_message('-535382146', "Проверьте бота", parse_mode='Markdown')


def checker_report(attempt=0):
    try:
        session = requests.session()
        session.post("https://api.glassen-it.com/component/socparser/authorization/login",
                     headers={'Content-Type': 'application/json', },
                     json={"login": "java_api", "password": "4yEcwVnjEH7D", }
                     )
        if ".docx" not in session.get(
                "https://api.glassen-it.com/component/socparser/content/getReportBulletin?reference_ids[]=370&thread_id=4188&from=2022-01-01&to=2022-01-2&network_id[]=1&network_id[]=2&network_id[]=3&network_id[]=4&network_id[]=5&network_id[]=7&network_id[]=8").headers.get(
            "Content-Disposition"):
            raise Exception("Проверьте отчет")
        else:
            print("ok")
    except Exception as e:
        bot.send_message('-535382146', "Проверьте отчет", parse_mode='Markdown')
        bot.send_message('457180576', str(e), parse_mode='Markdown')


async def run_api_test_for_threads():
    res = await asyncio.gather(
        *[asyncio.wait_for(run_api_test(thread_id), TIMEOUT * 10) for thread_id in [995, 5759, 6138]])
    # res = []
    # for thread_id in [995, 5759, 6138]:
    #     res.append({"thread_id": await run_api_test(thread_id)})
    res_text = " "
    for thread_res in res:
        for key, value in thread_res.items():
            for v_ in value:
                for k, v in v_.items():
                    try:
                        if v.status_code != 200:
                            res_text += f"* {key} {k} * status code: {v.status_code} \n"
                    except Exception:
                        res_text += f"* {key} {k} *  {str(v)} \n"
    return res_text

async def run_api_test(thread_id):
    async with httpx.AsyncClient() as session:
        try:
            print("res_login")
            res_login = await session.post(
                "https://api.glassen-it.com/component/socparser/authorization/login",
                json={
                    "login": "java_api",
                    "password": "4yEcwVnjEH7D"
                },
                timeout=TIMEOUT
            )
        except Exception as e:
            res_login = str(e)
        try:
            print("res_currentsmi")
            res_currentsmi = await session.get("https://api.glassen-it.com/component/socparser/content/currentsmi")
        except Exception as e:
            res_currentsmi = str(e)
        try:
            res_getTopic = "нет сми"
            for r in res_currentsmi.json():

                res_getTopic_r = await session.post(
                    "https://api.glassen-it.com/component/socparser/content/getTopic",
                    json={
                        "id": r["group_id"],
                        "thread_id": thread_id,
                        "referenceFilter": [
                            1
                        ],
                        "type": "smi",
                        "start": 0,
                        "limit": 10
                    }
                )
                if res_getTopic_r.status_code != 200 or res_getTopic == "нет сми":
                    res_getTopic = res_getTopic_r
        except Exception as e:
            res_getTopic = str(e)
        try:
            res_post = await session.post(
                "https://api.glassen-it.com/component/socparser/content/posts",
                json={
                    "thread_id": thread_id,
                    "from": "2022-12-03 00:00:00",
                    "to": "2022-12-03 23:59:59",
                    "limit": 20,
                    "start": 0,
                    "sort": {
                        "type": "date",
                        "order": "desc",
                        "name": "dateDown"
                    },
                    "filter": {
                        "network_id": ["1", "2", "3", "4", "5", "7", "8", "10"],
                        "repostoption": "whatever"
                    }
                },
                timeout=TIMEOUT
            )
        except Exception as e:
            res_post = str(e)
        try:
            res_getPostCountLight = await session.post(
                "https://api.glassen-it.com/component/socparser/content/getPostCountLight",
                json={"thread_id": thread_id, "from": "2022-12-03 00:00:00", "to": "2022-12-03 23:59:59"},
                timeout=TIMEOUT
            )
        except Exception as e:
            res_getPostCountLight = str(e)
        try:
            res_trusthourly = await session.post(
                "https://api.glassen-it.com/component/socparser/stats/trusthourly",
                json={
                    "thread_id": thread_id, "from": "2022-12-02T21:00:00.091Z", "to": "2022-12-03T20:59:59.091Z"
                },
                timeout=TIMEOUT
            )
        except Exception as e:
            res_trusthourly = str(e)
        try:
            res_allcommentaries = await session.post(
                "https://api.glassen-it.com/component/socparser/content/allcommentaries",
                json={
                    "thread_id": thread_id,
                    "from": "2022-12-03 00:00:00",
                    "to": "2022-12-03 18:20:38",
                    "limit": 20,
                    "start": 0,
                    "sort": {"type": "likes", "order": "desc"},
                    "filter": {}
                },
                timeout=TIMEOUT
            )
        except Exception as e:
            res_allcommentaries = str(e)

        return {thread_id: [
            {"login": res_login},
            {"posts": res_post},
            {"trusthourly": res_trusthourly},
            {"getPostCountLight": res_getPostCountLight},
            {"allcommentaries": res_allcommentaries},
            {"currentsmi": res_currentsmi},
            {"getTopic": res_getTopic}
        ]
        }


def send_static_test(message=None):
    try:
        print("send_static_test")
        loop = asyncio.get_event_loop()
        coroutine = run_api_test_for_threads()
        res_text = loop.run_until_complete(coroutine)
        if res_text != "":
            bot.send_message('-892710448', res_text, parse_mode='Markdown')
        else:
            bot.send_message('-892710448', "Тестирование прошло успешно", parse_mode='Markdown')

    except Exception as e:
        try:
            bot.send_message('457180576', f'Что-то сломалось {e}', parse_mode='Markdown')
        except Exception:
            pass

schedule.every(15).minutes.do(checker)
schedule.every(60).minutes.do(checker_report)
send_static_test()

# schedule.every().day.at("08:00").do(send_static_an_hour)
# schedule.every().day.at("12:00").do(send_static_an_hour)
# schedule.every().day.at("16:00").do(send_static_an_hour)
# schedule.every().day.at("20:00").do(send_static_an_hour)
# time -3 (UTC)
# https://stackoverflow.com/questions/65551967/python-scheduler-schedule-job-in-specific-time-zone-from-any-time-zone
schedule.every().day.at("02:00").do(send_static_test)
schedule.every().day.at("05:00").do(send_static_an_hour)
schedule.every().day.at("09:00").do(send_static_an_hour)
schedule.every().day.at("13:00").do(send_static_an_hour)
schedule.every().day.at("17:00").do(send_static_an_hour)
schedule.every().day.at("15:55").do(send_static_test)

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
