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
import shutil
import pandas as pd
import re


def load_json(path):
    return json.load(open(path, 'r'))


def upload_json(path, to_save):
    with open(path, 'w') as outfile:
        json.dump(to_save, outfile)


bot = telebot.TeleBot(config.token)
config_devices_open = load_json('config_devices.json')
list_devices = list(config_devices_open.keys())
disk_path = 'external_data'
main_path = 'data'
conn = sqlite3.connect('database.db')
cursor = conn.cursor()
cursor.execute('SELECT name, id FROM complexes')
dict_complexes = {name: ID for name, ID in cursor.fetchall()}


@bot.message_handler(commands=['upload_all_files_from_disk'])
def upload_all_files_from_disk(message):
    for name_folder in list_devices:
        for name_file in os.listdir(f'{disk_path}/{name_folder}'):
            if name_file.endswith('.csv'):
                if not os.path.exists(f'{main_path}/{name_folder}'):
                    os.makedirs(f'{main_path}/{name_folder}')
                shutil.copy(f'{disk_path}/{name_folder}/{name_file}', f'{main_path}/{name_folder}/{name_file}')
    for name_folder in os.listdir(f'{main_path}'):
        for name_file in os.listdir(f'{main_path}/{name_folder}'):
            preprocessing_one_file(f"{main_path}/{name_folder}/{name_file}")


def preprocessing_one_file(path):
    _, device, file_name = path.split('/')
    df = pd.read_csv(path, sep=None, engine='python', decimal=',')
    config_device_open = config_devices_open[device]
    time_col = config_device_open['time_cols']
    df = df[[time_col] + config_device_open['cols']]
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    if not os.path.exists(f'proc_data/{device}'):
        os.makedirs(f'proc_data/{device}')
    df[time_col] = pd.to_datetime(df[time_col], format=config_device_open['format'])
    df = df.sort_values(by=time_col)
    diff_mode = df[time_col].diff().mode().values[0] * 1.1
    new_rows = []
    for i in range(len(df) - 1):
        diff = (df.loc[i + 1, time_col] - df.loc[i, time_col])
        if diff > diff_mode:
            new_date1 = df.loc[i, time_col] + pd.Timedelta(seconds=1)
            new_date2 = df.loc[i + 1, time_col] - pd.Timedelta(seconds=1)
            new_row1 = {time_col: new_date1}
            new_row2 = {time_col: new_date2}
            new_rows.extend([new_row1, new_row2])
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    df = df.sort_values(by=time_col)
    name = re.split("[-_]", file_name)
    df.to_csv(f'proc_data/{device}/{name[0]}_{name[1]}.csv', index=False)
    return f'proc_data/{device}/{name[0]}_{name[1]}.csv'


@bot.message_handler(commands=['start'])
def start(message):
    ID_user = str(message.from_user.id)
    user_info_open = load_json('user_info.json')
    if ID_user not in user_info_open.keys():
        user_info_open[ID_user] = {}
    user_info_open[ID_user]['update_quick_access'] = False
    user_info_open[ID_user].pop('selected_columns', None)
    user_info_open[ID_user]['device_to_choose'] = []
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
    ID_user = str(message.from_user.id)
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add('Настроить быстрый доступ')
    if 'quick_access' in load_json('user_info.json')[ID_user].keys():
        markup.add('Отрисовка графика')
    bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text == 'Отрисовка графика')
def logic_draw_plot(message):
    concat_files(message)


@bot.message_handler(func=lambda
        message: message.text == "Настроить быстрый доступ")
def update_quick_access(message):
    ID_user = str(message.from_user.id)
    d = load_json('user_info.json').copy()
    d[ID_user]['update_quick_access'] = True
    upload_json('user_info.json', d)
    choice_devices_or_complexes(message)


@bot.message_handler(func=lambda
        message: message.text == "Просмотр всех приборов")
def all_devices(message):
    ID_user = str(message.from_user.id)
    user_info_open = load_json('user_info.json')
    if not user_info_open[ID_user]['device_to_choose']:
        user_info_open[ID_user]['device_to_choose'] = list_devices
        upload_json('user_info.json', user_info_open)
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add(*list(map(lambda x: types.KeyboardButton(x), user_info_open[ID_user]['device_to_choose'])))
    bot.send_message(message.chat.id, "Выберите прибор", reply_markup=markup)


@bot.message_handler(func=lambda
        message: message.text == "Просмотр приборов по комплексам")
def all_complexes(message):
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add(*list(map(lambda x: types.KeyboardButton(x), dict_complexes.keys())))
    bot.send_message(message.chat.id, "Выберите комплекс", reply_markup=markup)


@bot.message_handler(func=lambda
        message: message.text in dict_complexes.keys())
def choose_one_complex(message):
    ID_user = str(message.from_user.id)
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    ID_complex = dict_complexes[message.text]
    cursor.execute("SELECT name_on_disk FROM devices WHERE complex_id = ? AND name_on_disk IN ({})".format(
        ','.join('?' for _ in list_devices)), [ID_complex] + list_devices)
    user_info_open = load_json('user_info.json')
    user_info_open[ID_user]['device_to_choose'] = list(map(lambda x: x[0], list(cursor.fetchall())))
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
            user_info_open[ID_user]['selected_columns'] = ava_col
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
        (combined_data[time_col] >= begin_record_date) & (combined_data[time_col] <= end_record_date + timedelta(days=1))]
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
