import config
import telebot
from telebot import types
from yadisk import YaDisk
from dotenv import load_dotenv
import logging
import os
import json
import sqlite3
import pandas as pd
from datetime import timedelta, datetime
from telebot.types import CallbackQuery
import plotly.express as px
from matplotlib import pyplot as plt

bot = telebot.TeleBot(config.token)
path_to_site = "../MSU_aerosol_site"
path_db = f"{path_to_site}/msu_aerosol/database.db"
load_dotenv(f"{path_to_site}/.env")
yadisk_token = os.getenv('YADISK_TOKEN', default='FAKE_TOKEN')
disk = YaDisk(token=yadisk_token)
logging.basicConfig(filename='info.log')


def upload_json(path, to_save):
    with open(path, "w") as outfile:
        json.dump(to_save, outfile)


def make_list_short_name_devices():
    return list(
        map(lambda x: x[0], execute_query("SELECT name FROM devices WHERE show=TRUE"))
    )


def execute_query(query: str, method="fetchall"):
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()
    if method == "fetchall":
        value = cursor.execute(query).fetchall()
    else:
        value = cursor.execute(query).fetchone()
    conn.close()
    return value


def short_name_to_full_name_device(short_name):
    return execute_query(
        f'SELECT full_name FROM devices WHERE name="{short_name}"',
        method="fetchone",
    )[0]


def make_list_complexes():
    return list(map(lambda x: x[0], execute_query("SELECT name FROM complexes")))


def get_devices_from_complex(complex_name):
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()
    complex_id = execute_query(
        f'SELECT id FROM complexes WHERE name = "{complex_name}"',
        method="fetchone",
    )[0]
    return list(
        map(
            lambda x: x[0],
            cursor.execute(
                f'SELECT name FROM devices WHERE show=1 AND complex_id="{complex_id}"'
            ).fetchall(),
        )
    )


def get_time_col(graph_id):

    return 'timestamp'

def make_list_cols(device):
    device_id = execute_query(
        f'SELECT id FROM devices WHERE full_name = "{device}"', method="fetchone"
    )[0]
    graph_ids = list(
        map(
            lambda x: x[0],
            execute_query(
                f'SELECT id FROM graphs WHERE device_id = "{device_id}"'
            ),
        )
    )
    graph_ids_str = ', '.join(f'"{graph_id}"' for graph_id in graph_ids)
    return list(set(list(
        map(
            lambda x: x[0],
            execute_query(
                f'SELECT name FROM columns WHERE graph_id IN ({graph_ids_str}) AND use=1'
            ),
        )
    )))


def get_color(col, device):
    device_id = execute_query(
        f'SELECT id FROM devices WHERE full_name = "{device}"', method="fetchone"
    )[0]
    return execute_query(
        f'SELECT color FROM columns WHERE name = "{col}" AND graph_id = "{device_id}"', method="fetchone"
    )[0]


@bot.message_handler(commands=['start'])
def start(message, error=False):
    id_user = message if error else str(message.from_user.id)
    with open("user_info.json", "r") as file:
        user_info_open = json.load(file)
    if id_user not in user_info_open.keys() or error:
        user_info_open[id_user] = {}
    user_info_open[id_user]["update_quick_access"] = False
    user_info_open[id_user].pop("selected_columns", None)
    user_info_open[id_user]["device_to_choose"] = []
    upload_json("user_info.json", user_info_open)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Просмотр данных с приборов"))
    markup.add(types.KeyboardButton("Быстрый доступ"))
    bot.send_message(
        id_user, text=f"Начните работу с приборами", reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text == "Быстрый доступ")
def quick_access(message):
    try:
        id_user = str(message.from_user.id)
        markup = types.ReplyKeyboardMarkup(row_width=1)
        markup.add("Настроить быстрый доступ")
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        if "quick_access" in user_info_open[id_user].keys():
            markup.add("Отрисовка графика")
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup)
    except Exception as e:
        exception_handler(message, e, quick_access)


@bot.message_handler(func=lambda message: message.text == "Отрисовка графика")
def logic_draw_plot(message):
    try:
        concat_files(message)
    except Exception as e:
        exception_handler(message, e, 'logic_draw_plot')


@bot.message_handler(func=lambda message: message.text == "Настроить быстрый доступ")
def update_quick_access(message):
    try:
        id_user = str(message.from_user.id)
        d = json.load(open("user_info.json", "r")).copy()
        d[id_user]["update_quick_access"] = True
        upload_json("user_info.json", d)
        choice_devices_or_complexes(message)
    except Exception as e:
        exception_handler(message, e, 'update_quick_access')


@bot.message_handler(func=lambda message: message.text == "Просмотр данных с приборов")
def choice_devices_or_complexes(message):
    try:
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton("Просмотр всех приборов"))
        markup.add(types.KeyboardButton("Просмотр приборов по комплексам"))
        bot.send_message(
            message.chat.id, text=f"Каким образом выбрать прибор?", reply_markup=markup
        )
    except Exception as e:
        exception_handler(message, e, 'choice_devices_or_complexes')


@bot.message_handler(func=lambda message: message.text == "Просмотр всех приборов")
def all_devices(message):
    try:
        list_short_name_devices = make_list_short_name_devices()
        id_user = str(message.from_user.id)
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        if not user_info_open[id_user]["device_to_choose"]:
            user_info_open[id_user]["device_to_choose"] = list_short_name_devices
            upload_json("user_info.json", user_info_open)
        markup = types.ReplyKeyboardMarkup(row_width=1)
        markup.add(
            *list(
                map(
                    lambda x: types.KeyboardButton(x),
                    user_info_open[id_user]["device_to_choose"],
                )
            )
        )
        bot.send_message(message.chat.id, "Выберите прибор", reply_markup=markup)
    except Exception as e:
        exception_handler(message, e, 'all_devices')


@bot.message_handler(
    func=lambda message: message.text in make_list_short_name_devices())
def choose_device(message):
    try:
        if message.text in make_list_short_name_devices():
            id_user = str(message.from_user.id)
            with open("user_info.json", "r") as file:
                user_info_open = json.load(file)
            user_info_open[id_user]["device"] = short_name_to_full_name_device(message.text)
            upload_json("user_info.json", user_info_open)
        choose_time_delay(message)
    except Exception as e:
        exception_handler(message, e, 'choose_device')


@bot.message_handler(
    func=lambda message: message.text == "Просмотр приборов по комплексам"
)
def all_complexes(message):
    try:
        markup = types.ReplyKeyboardMarkup(row_width=1)
        markup.add(*list(map(lambda x: types.KeyboardButton(x), make_list_complexes())))
        bot.send_message(message.chat.id, "Выберите комплекс", reply_markup=markup)
    except Exception as e:
        exception_handler(message, e, 'all_complexes')


@bot.message_handler(func=lambda message: message.text in make_list_complexes())
def choose_one_complex(message):
    try:
        id_user = str(message.from_user.id)
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        user_info_open[id_user]["device_to_choose"] = get_devices_from_complex(message.text)
        upload_json("user_info.json", user_info_open)
        all_devices(message)
    except Exception as e:
        exception_handler(message, e, 'choose_one_complex')


def choose_time_delay(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("2 дня"), types.KeyboardButton("7 дней"))
    markup.add(types.KeyboardButton("14 дней"), types.KeyboardButton("31 день"))
    markup.add(types.KeyboardButton("Свой временной промежуток"))
    bot.send_message(
        message.chat.id, "Выберите временной промежуток", reply_markup=markup
    )


@bot.message_handler(
    func=lambda message: message.text in ["2 дня", "7 дней", "14 дней", "31 день"]
)
def get_delay(message):
    try:
        id_user = str(message.from_user.id)
        delay = (
            2
            if message.text == "2 дня"
            else 7 if message.text == "7 дней" else 14 if message.text == "14 дней" else 31
        )
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        user_info_open[id_user]["delay"] = delay
        upload_json("user_info.json", user_info_open)
        choose_columns(message)
    except Exception as e:
        exception_handler(message, e, 'get_delay')


def make_range(device):
    list_files = os.listdir(f"{path_to_site}/msu_aerosol/proc_data/{device}")
    last_record_date = pd.to_datetime(
        pd.read_csv(f"{path_to_site}/msu_aerosol/proc_data/{device}/{max(list_files)}")[
            get_time_col(device)
        ].iloc[-1]
    )
    first_record_date = pd.to_datetime(
        pd.read_csv(f"{path_to_site}/msu_aerosol/proc_data/{device}/{min(list_files)}")[
            get_time_col(device)
        ].iloc[0]
    )
    return first_record_date, last_record_date


@bot.message_handler(func=lambda message: message.text == "Свой временной промежуток")
def choose_not_default_delay(message):
    # try:
    choose_not_default_start_date(message)


# except Exception as e:
#     exception_handler(message, e, 'choose_not_default_delay')


def choose_not_default_start_date(message):
    id_user = str(message.from_user.id)
    with open("user_info.json", "r") as file:
        user_info_open = json.load(file)
    user_info_open = user_info_open[id_user]
    device = user_info_open["device"]
    first_record_date, last_record_date = make_range(device)
    first_record_date = first_record_date.strftime("%d.%m.%Y")
    last_record_date = last_record_date.strftime("%d.%m.%Y")
    bot.send_message(
        message.chat.id, f"Данные досупны с {first_record_date} по {last_record_date}"
    )
    msg = bot.send_message(
        message.chat.id,
        "Дата начала отрезка данных (в формате 'день.месяц.год')",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    bot.register_next_step_handler(msg, begin_record_date_choose)


def begin_record_date_choose(message):
    id_user = str(message.from_user.id)
    with open("user_info.json", "r") as file:
        user_info_open = json.load(file)
    device = user_info_open[id_user]["device"]
    first_record_date, last_record_date = make_range(device)
    try:
        begin_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        if not last_record_date.date() >= begin_record_date >= first_record_date.date():
            raise ValueError
        user_info_open[id_user]["delay"] = [str(begin_record_date)]
        upload_json("user_info.json", user_info_open)
        choose_not_default_finish_date(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_start_date(message)


def choose_not_default_finish_date(message):
    msg = bot.send_message(
        message.chat.id,
        "Дата конца отрезка данных (в формате 'день.месяц.год')",
    )
    bot.register_next_step_handler(msg, end_record_date_choose)


def end_record_date_choose(message):
    id_user = str(message.from_user.id)
    with open("user_info.json", "r") as file:
        user_info_open = json.load(file)
    device = user_info_open[id_user]["device"]
    first_record_date, last_record_date = make_range(device)
    try:
        end_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()

        start_date = pd.to_datetime(user_info_open[id_user]['delay'][0]).date()
        if not (last_record_date.date() >= end_record_date >= start_date):
            raise ValueError
        user_info_open[id_user]["delay"] = [str(start_date), str(end_record_date)]
        upload_json("user_info.json", user_info_open)
        choose_columns(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_finish_date(message)


def draw_inline_keyboard(selected_columns, ava_col):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for i in ava_col:
        emoji = " ✔️" if i in selected_columns else " ❌"
        markup.add(
            types.InlineKeyboardButton(
                str(i) + emoji,
                callback_data=f"feature_{str(i)}",
            )
        )
    markup.add(types.InlineKeyboardButton("Построить график", callback_data="next"))
    return markup


@bot.callback_query_handler(func=lambda call: True)
def choose_columns(call):
    try:
        id_user = str(call.from_user.id)
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        ava_col = make_list_cols(user_info_open[str(call.from_user.id)]["device"])
        if isinstance(call, CallbackQuery):
            text = call.data
        else:
            text = call.text
        if text.startswith("feature"):
            feature = "_".join(call.data.split("feature")[1].split("_")[1::])
            selected_features = user_info_open[id_user]["selected_columns"]
            if feature in selected_features:
                selected_features.remove(feature)
                bot.answer_callback_query(call.id, "Вы убрали столбец " + feature)
            else:
                selected_features.append(feature)
                bot.answer_callback_query(call.id, "Вы добавили столбец " + feature)
            user_info_open[id_user]["selected_columns"] = selected_features
            upload_json("user_info.json", user_info_open)
            bot.answer_callback_query(call.id, "Вы выбрали Фичу " + feature)
            selected_columns = user_info_open[str(call.from_user.id)]["selected_columns"]
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text="Нажми",
                reply_markup=draw_inline_keyboard(selected_columns, ava_col),
            )

        elif text == "next":
            if (
                    len(json.load(open("user_info.json", "r"))[id_user]["selected_columns"])
                    != 0
            ):
                concat_files(call)
            else:
                bot.answer_callback_query(call.id, "Ни один параметр не выбран!")
        else:
            with open("user_info.json", "r") as file:
                user_info_open = json.load(file)
            if "selected_columns" not in user_info_open[id_user].keys():
                user_info_open[id_user]["selected_columns"] = []
            upload_json("user_info.json", user_info_open)
            selected_columns = user_info_open[id_user]["selected_columns"]
            bot.send_message(
                call.chat.id,
                "Столбцы для выбора:",
                reply_markup=draw_inline_keyboard(selected_columns, ava_col),
            )
    except Exception as e:
        exception_handler(int(call.from_user.id), e, 'choose_columns')


def concat_files(message):
    id_user = str(message.from_user.id)
    bot.send_message(id_user, 'Строю график')
    if isinstance(message, CallbackQuery):
        text = message.data
    else:
        text = message.text

    with open("user_info.json", "r") as file:
        user_info_open = json.load(file)
    if user_info_open[id_user]["update_quick_access"]:
        user_info_open[id_user]["quick_access"] = user_info_open[id_user].copy()
        user_info_open[id_user]["update_quick_access"] = False
        upload_json("user_info.json", user_info_open)
        bot.send_message(id_user, "Параметры для быстрого доступа выбраны. ")
    if text == "Отрисовка графика":
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        user_id = user_info_open[id_user]["quick_access"]
    else:
        with open("user_info.json", "r") as file:
            user_info_open = json.load(file)
        user_id = user_info_open[id_user]
    device = user_id["device"]
    delay = user_id["delay"]
    if isinstance(delay, int):
        end_record_date = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
        begin_record_date = end_record_date - timedelta(delay)
    else:
        end_record_date = pd.to_datetime(delay[1])
        begin_record_date = pd.to_datetime(delay[0])
    current_date, combined_data = begin_record_date, pd.DataFrame()
    while current_date <= end_record_date + timedelta(days=100):
        try:
            data = pd.read_csv(
                f"{path_to_site}/msu_aerosol/proc_data/{device}/{current_date.strftime('%Y_%m')}.csv"
            )
            combined_data = pd.concat([combined_data, data], ignore_index=True)
            current_date += timedelta(days=29)
        except FileNotFoundError:
            current_date += timedelta(days=29)
    if combined_data.empty:
        fig = px.line(
            combined_data)
    else:
        time_col = "timestamp"
        combined_data[time_col] = pd.to_datetime(
            combined_data[time_col],
            format="%Y-%m-%d %H:%M:%S",
        )
        combined_data = combined_data[
            (combined_data[time_col] >= begin_record_date)
            & (combined_data[time_col] <= end_record_date + timedelta(days=1))
            ]
        combined_data.set_index(time_col, inplace=True)
        combined_data = combined_data.replace(",", ".", regex=True).astype(float)
        cols_to_draw = user_id["selected_columns"]
        combined_data.reset_index(inplace=True)
        combined_data = combined_data.sort_values(by=time_col)
        cols_to_draw = combined_data[cols_to_draw].mean().sort_values(ascending=False).index.tolist()
        fig = px.line(
            combined_data,
            x=time_col,
            y=cols_to_draw,
            color_discrete_sequence=[get_color(i, device) for i in cols_to_draw],
        )
    fig.update_layout(
        title=str(device),
        xaxis=dict(title="Time"),
        plot_bgcolor='white',
        paper_bgcolor='white',
        showlegend=True,
    )
    fig.update_traces(line={'width': 2})
    fig.update_xaxes(
        zerolinecolor='grey',
        zerolinewidth=1,
        gridcolor='grey',
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
        tickformat='%H:%M\n%d.%m.%Y',
        minor_griddash='dot',
    )
    fig.update_yaxes(
        zerolinecolor='grey',
        zerolinewidth=1,
        gridcolor='grey',
        showline=True,
        linewidth=1,
        linecolor='black',
        mirror=True,
    )
    logging.info(f'User {user_id} requested {device} for {begin_record_date} - {end_record_date} at {datetime.now()}')
    fig.write_image(f"graphs_photo/{id_user}.png")
    bot.send_photo(id_user, photo=open(f"graphs_photo/{id_user}.png", "rb"))
    plt.close()
    make_graph_again(id_user)


def make_graph_again(id_user):
    markup = types.ReplyKeyboardMarkup(row_width=1)
    btn1 = types.KeyboardButton("Да")
    btn2 = types.KeyboardButton("Нет")
    markup.add(btn1, btn2)
    bot.send_message(id_user, "Построить график с другим временным диапазоном еще раз?", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text in ['Да', 'Нет'])
def make_graph_again_ind(message):
    try:
        if message.text == 'Да':
            choose_device(message)
        else:
            start(message)
    except Exception as e:
        exception_handler(message, e, 'make_graph_again_ind')


def exception_handler(message, e, name_func):
    user_id = message if isinstance(message, int) else message.from_user.id
    logging.warning(f'Непредвиденная ошибка: {e.__class__.__name__} в {name_func}')
    bot.send_message(user_id,
                     f"Непредвиденная ошибка в {name_func}")
    start(user_id, error=True)


try:
    bot.polling(none_stop=True)
except Exception as error:
    bot.send_message(-1002244175815, "Bot program crashed with the error: " + str(error))