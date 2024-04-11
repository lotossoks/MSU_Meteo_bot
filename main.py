import sqlite3
from datetime import datetime, timedelta
from matplotlib import pyplot as plt
from telebot import types
from telebot.types import CallbackQuery
import plotly.graph_objects as go
import config
import telebot
import json
import os
import pandas as pd
import re
from yadisk import YaDisk
from dotenv import load_dotenv


def load_json(path):
    return json.load(open(path, 'r'))


def upload_json(path, to_save):
    with open(path, 'w') as outfile:
        json.dump(to_save, outfile)


bot = telebot.TeleBot(config.token)
main_path = "data"
path_db = "../MSU_aerosol_site/msu_aerosol/database.db"
load_dotenv("../MSU_aerosol_site/.env")
yadisk_token = os.getenv("YADISK_TOKEN")
disk = YaDisk(token=yadisk_token)


def make_list_short_names_devices():
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()
    list_short_names = list(map(lambda x: x[0], cursor.execute('SELECT link FROM devices WHERE show=TRUE').fetchall()))
    return list_short_names


def make_list_full_names_devices():
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()
    list_full_names = list(map(lambda x: disk.get_public_meta(x[0]),
                               cursor.execute('SELECT link FROM devices WHERE show=TRUE').fetchall()))

    return list_full_names





@bot.message_handler(commands=['start'])
def start(message):
    id_user = str(message.from_user.id)
    user_info_open = load_json('user_info.json')
    if id_user not in user_info_open.keys():
        user_info_open[id_user] = {}
    user_info_open[id_user]['update_quick_access'] = False
    user_info_open[id_user].pop('selected_columns', None)
    user_info_open[id_user]['device_to_choose'] = []
    upload_json('user_info.json', user_info_open)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Просмотр данных с приборов"))
    markup.add(types.KeyboardButton("Быстрый доступ"))
    bot.send_message(message.chat.id,
                     text=f"Начните работу с приборами", reply_markup=markup)


@bot.message_handler(
    func=lambda message: message.text == "Просмотр данных с приборов"
)
def choice_devices_or_complexes(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Просмотр всех приборов"))
    markup.add(types.KeyboardButton("Просмотр приборов по комплексам"))
    bot.send_message(message.chat.id,
                     text=f"Каким образом выбрать прибор?", reply_markup=markup)


@bot.message_handler(func=lambda
        message: message.text == "Быстрый доступ")
def quick_access(message):
    id_user = str(message.from_user.id)
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add('Настроить быстрый доступ')
    if 'quick_access' in load_json('user_info.json')[id_user].keys():
        markup.add('Отрисовка графика')
    bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text == 'Отрисовка графика')
def logic_draw_plot(message):
    concat_files(message)


@bot.message_handler(func=lambda
        message: message.text == "Настроить быстрый доступ")
def update_quick_access(message):
    id_user = str(message.from_user.id)
    d = load_json('user_info.json').copy()
    d[id_user]['update_quick_access'] = True
    upload_json('user_info.json', d)
    choice_devices_or_complexes(message)


@bot.message_handler(func=lambda
        message: message.text == "Просмотр всех приборов")
def all_devices(message):
    list_devices = make_list_short_names_devices()
    id_user = str(message.from_user.id)
    user_info_open = load_json('user_info.json')
    if not user_info_open[id_user]['device_to_choose']:
        user_info_open[id_user]['device_to_choose'] = list_devices
        upload_json('user_info.json', user_info_open)
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add(*list(map(lambda x: types.KeyboardButton(x), user_info_open[id_user]['device_to_choose'])))
    bot.send_message(message.chat.id, "Выберите прибор", reply_markup=markup)


@bot.message_handler(func=lambda
        message: message.text == "Просмотр приборов по комплексам")
def all_complexes(message):
    list_complexes = make_list_complexes()
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add(*list(map(lambda x: types.KeyboardButton(x), list_complexes)))
    bot.send_message(message.chat.id, "Выберите комплекс", reply_markup=markup)


@bot.message_handler(func=lambda
        message: message.text in make_list_complexes())
def choose_one_complex(message):
    id_user = str(message.from_user.id)
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    complex_id = cursor.execute(f"SELECT id FROM complexes WHERE name = {message.text}").fetchone()
    list_devices_full_name = list(map(lambda x: disk.get_public_meta(x[0]),
                                      cursor.execute(f"SELECT link FROM devices WHERE complex_id = {complex_id}")))
    list_devices_short_name =
    cursor.execute(f"SELECT link FROM devices WHERE complex_id = {complex_id}")
    user_info_open = load_json('user_info.json')
    user_info_open[id_user]['device_to_choose'] = list(map(lambda x: x[0], list(cursor.fetchall())))
    upload_json('user_info.json', user_info_open)
    all_devices(message)


@bot.message_handler(func=lambda
        message: message.text in list_devices)
def choose_device(message):
    ID_user = str(message.from_user.id)
    user_info_open = load_json('user_info.json')
    user_info_open[ID_user]['device'] = message.text
    upload_json('user_info.json', user_info_open)
    work_with_latest_file(ID_user)
    work_with_first_file(ID_user)
    choose_time_delay(message)


def work_with_latest_file(user_id):
    user_info_open = load_json('user_info.json')
    device = user_info_open[user_id]['device']
    last_record_file = f"{main_path}/{device}/{max(list(filter(lambda x: '.csv' in x, os.listdir(f'{main_path}/{device}'))))}"
    file_name = pd.read_csv(preprocessing_one_file(last_record_file))
    max_date = str(pd.to_datetime(file_name[load_json('config_devices.json')[device]['time_cols']]).max()).split()[0]
    devices_tech_info_open = load_json('devices_tech_info.json')
    devices_tech_info_open[device] = {'last_record_file': last_record_file}
    user_info_open[user_id]['last_record_date'] = max_date
    upload_json('user_info.json', user_info_open)
    upload_json('devices_tech_info.json', devices_tech_info_open)


def work_with_first_file(user_id):
    device = load_json('user_info.json')[user_id]['device']
    first_record_file = min(list(filter(lambda x: '.csv' in x, os.listdir(f'proc_data/{device}'))))
    df = pd.read_csv(f"proc_data/{device}/{first_record_file}")
    time_col = load_json('config_devices.json')[device]['time_cols']
    devices_tech_info_open = load_json('devices_tech_info.json')
    devices_tech_info_open[device]['first_record_date'] = str(df[time_col].min()).split()[0]
    upload_json('devices_tech_info.json', devices_tech_info_open)


def choose_time_delay(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton('2 дня'), types.KeyboardButton('7 дней'))
    markup.add(types.KeyboardButton('14 дней'), types.KeyboardButton('31 день'))
    markup.add(types.KeyboardButton('Свой временной промежуток'))
    bot.send_message(message.chat.id, "Выберите временной промежуток", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text in ['2 дня', '7 дней', '14 дней', '31 день'])
def choose_default_time_delay(message):
    ID_user = str(message.from_user.id)
    delay = 2 if message.text == '2 дня' else 7 if message.text == '7 дней' else 14 if message.text == '14 дней' else 31
    user_info_open = load_json('user_info.json')
    end_record_date = user_info_open[ID_user]['last_record_date']
    begin_record_date = (datetime.strptime(end_record_date, '%Y-%m-%d') - timedelta(days=delay)).strftime('%Y-%m-%d')
    user_info_open[ID_user]['begin_record_date'] = str(begin_record_date).split()[0]
    upload_json('user_info.json', user_info_open)
    choose_columns(message)


@bot.message_handler(func=lambda message: message.text == 'Свой временной промежуток')
def choose_not_default_delay(message):
    choose_not_default_start_date(message)


def choose_not_default_start_date(message):
    ID_user = str(message.from_user.id)
    devices_tech_info_open = load_json('devices_tech_info.json')
    user_info_open = load_json('user_info.json')[ID_user]
    device = user_info_open['device']
    first_record_date = devices_tech_info_open[device]['first_record_date']
    first_record_date = datetime.strptime(first_record_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    last_record_date = user_info_open['last_record_date']
    last_record_date = datetime.strptime(last_record_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    bot.send_message(message.chat.id, f"Данные досупны с {first_record_date} по {last_record_date}")
    msg = bot.send_message(message.chat.id, "Дата начала отрезка данных (в формате 'день.месяц.год')",
                           reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(msg, begin_record_date_choose)


def begin_record_date_choose(message):
    ID_user = str(message.from_user.id)
    devices_tech_info_open = load_json('devices_tech_info.json')
    user_info_open = load_json('user_info.json')
    device = user_info_open[ID_user]['device']
    first_record_date = devices_tech_info_open[device]['first_record_date']
    first_record_date = datetime.strptime(first_record_date, "%Y-%m-%d").date()
    last_record_date = user_info_open[ID_user]['last_record_date']
    last_record_date = datetime.strptime(last_record_date, "%Y-%m-%d").date()
    try:
        begin_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        if not last_record_date >= begin_record_date >= first_record_date:
            raise ValueError
        user_info_open[ID_user]['begin_record_date'] = str(begin_record_date).split()[0]
        upload_json('user_info.json', user_info_open)
        choose_not_default_finish_date(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_start_date(message)


def choose_not_default_finish_date(message):
    msg = bot.send_message(message.chat.id, "Дата конца отрезка данных (в формате 'день.месяц.год')")
    bot.register_next_step_handler(msg, end_record_date_choose)


def end_record_date_choose(message):
    ID_user = str(message.from_user.id)
    user_info_open = load_json('user_info.json')
    begin_record_date = user_info_open[ID_user]['begin_record_date']
    begin_record_date = datetime.strptime(begin_record_date, "%Y-%m-%d").date()
    last_record_date = user_info_open[ID_user]['last_record_date']
    last_record_date = datetime.strptime(last_record_date, "%Y-%m-%d").date()
    try:
        end_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        if not (last_record_date >= end_record_date >= begin_record_date):
            raise ValueError
        user_info_open[ID_user]['last_record_date'] = str(end_record_date).split()[0]
        upload_json('user_info.json', user_info_open)
        choose_columns(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_finish_date(message)


def draw_inline_keyboard(selected_columns, ava_col):
    markup = types.InlineKeyboardMarkup(row_width=1)
    for i in ava_col:
        emoji = ' ✔️' if i in selected_columns else ' ❌'
        markup.add(types.InlineKeyboardButton(str(i) + emoji, callback_data=f'feature_{str(i)}'))
    markup.add(types.InlineKeyboardButton('Выбрано', callback_data='next'))
    return markup


@bot.callback_query_handler(func=lambda call: True)
def choose_columns(call):
    ID_user = str(call.from_user.id)
    if isinstance(call, CallbackQuery):
        text = call.data
    else:
        text = call.text
    if text.startswith('feature'):
        feature = "_".join(call.data.split('feature')[1].split("_")[1::])
        user_info_open = load_json('user_info.json')
        selected_features = user_info_open[ID_user]['selected_columns']
        if feature in selected_features:
            selected_features.remove(feature)
            bot.answer_callback_query(call.id, 'Вы убрали столбец ' + feature)
        else:
            selected_features.append(feature)
            bot.answer_callback_query(call.id, 'Вы добавили столбец ' + feature)
        user_info_open[ID_user]['selected_columns'] = selected_features
        upload_json('user_info.json', user_info_open)
        bot.answer_callback_query(call.id, 'Вы выбрали Фичу ' + feature)
        selected_columns = user_info_open[str(call.from_user.id)]['selected_columns']
        ava_col = load_json('config_devices.json')[user_info_open[ID_user]['device']]['cols']
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Нажми",
                              reply_markup=draw_inline_keyboard(selected_columns, ava_col))

    elif text == 'next':
        if len(load_json('user_info.json')[ID_user]['selected_columns']) != 0:
            concat_files(call)
        else:
            bot.answer_callback_query(call.id, 'Ни один параметр не выбран!')
    else:
        user_info_open = load_json('user_info.json')
        ava_col = config_devices_open[user_info_open[ID_user]['device']]['cols']
        if 'selected_columns' not in user_info_open[ID_user].keys():
            user_info_open[ID_user]['selected_columns'] = []
        upload_json('user_info.json', user_info_open)
        selected_columns = user_info_open[ID_user]['selected_columns']
        bot.send_message(call.chat.id, 'Столбцы для выбора:',
                         reply_markup=draw_inline_keyboard(selected_columns, ava_col))


def concat_files(message):
    ID_user = str(message.from_user.id)
    if isinstance(message, CallbackQuery):
        text = message.data
    else:
        text = message.text

    user_info_open = load_json('user_info.json')
    if user_info_open[ID_user]['update_quick_access']:
        user_info_open[ID_user]['quick_access'] = user_info_open[ID_user].copy()
        user_info_open[ID_user]['update_quick_access'] = False
        upload_json('user_info.json', user_info_open)
        bot.send_message(ID_user, 'Параметры для быстрого доступа выбраны. ')
    if text == 'Отрисовка графика':
        user_info_open = load_json('user_info.json')
        user_id = user_info_open[ID_user]['quick_access']
    else:
        user_info_open = load_json('user_info.json')
        user_id = user_info_open[ID_user]
    device = user_id['device']
    begin_record_date = datetime.strptime(user_id['begin_record_date'], '%Y-%m-%d')
    end_record_date = datetime.strptime(user_id['last_record_date'], '%Y-%m-%d')
    current_date, combined_data = begin_record_date, pd.DataFrame()
    while current_date <= end_record_date + timedelta(days=100):
        try:
            data = pd.read_csv(f"proc_data/{device}/{current_date.strftime('%Y_%m')}.csv")
            combined_data = pd.concat([combined_data, data], ignore_index=True)
            current_date += timedelta(days=29)
        except FileNotFoundError:
            current_date += timedelta(days=29)
    begin_record_date = pd.to_datetime(begin_record_date)
    end_record_date = pd.to_datetime(end_record_date)
    device_dict = load_json('config_devices.json')[device]
    time_col = device_dict['time_cols']
    combined_data[time_col] = pd.to_datetime(combined_data[time_col], format="%Y-%m-%d %H:%M:%S")
    combined_data = combined_data[
        (combined_data[time_col] >= begin_record_date) & (
                combined_data[time_col] <= end_record_date + timedelta(days=1))]
    combined_data.set_index(time_col, inplace=True)
    combined_data = combined_data.replace(',', '.', regex=True).astype(float)
    if (end_record_date - begin_record_date).days > 2 and len(combined_data) >= 500:
        combined_data = combined_data.resample('60min').mean()
    cols_to_draw = user_id['selected_columns']
    combined_data.reset_index(inplace=True)
    fig = go.Figure()
    fig.update_layout(
        title=str(device),
        xaxis=dict(title="Time"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=True
    )
    fig.update_traces(line={'width': 2})
    fig.update_xaxes(gridcolor='grey',
                     showline=True,
                     linewidth=1,
                     linecolor='black',
                     mirror=True,
                     tickformat='%d.%m.%Y')
    fig.update_yaxes(gridcolor='grey',
                     showline=True,
                     linewidth=1,
                     linecolor='black',
                     mirror=True)
    for col in cols_to_draw:
        fig.add_trace(go.Scatter(x=combined_data[time_col], y=combined_data[col],
                                 mode='lines',
                                 name=col,
                                 line=go.scatter.Line(
                                     color=device_dict['color_dict'][col])))
    fig.write_image(f"graphs_photo/{ID_user}.png")
    bot.send_photo(ID_user, photo=open(f"graphs_photo/{ID_user}.png", 'rb'))
    plt.close()


bot.polling(none_stop=True)
