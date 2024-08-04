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

# Основные константы
bot = telebot.TeleBot(config.token)
path_to_site = "../MSU_aerosol_site"
path_db = f"{path_to_site}/msu_aerosol/database.db"
load_dotenv(f"{path_to_site}/.env")
yadisk_token = os.getenv("YADISK_TOKEN", default="FAKE_TOKEN")
disk = YaDisk(token=yadisk_token)
logging.basicConfig(filename="info.log")


def upload_json(path, to_save):
    """
    Функция для выгрузки данных в json

    :param path: Путь для выгрузки
    :param to_save: Данные для выгрузки
    :return: None
    """
    with open(path, "w") as outfile:
        json.dump(to_save, outfile)


def load_json(path):
    """
    Функция для загрузки данных в json

    :param path: Путь для загрузки
    :return: json файл
    """
    with open(path, "r") as file:
        return json.load(file)


def execute_query(query: str, method="fetchall"):
    """
    Функция для упрощения обращения к базе данных приборов и графиков

    :param query: запрос
    :param method: тип метода fetchall или fetchone
    :return: значение по запросу
    """
    conn = sqlite3.connect(path_db)
    cursor = conn.cursor()
    if method == "fetchall":
        value = cursor.execute(query).fetchall()
    else:
        value = cursor.execute(query).fetchone()
    conn.close()
    return value


def make_list_short_name_devices():
    """
    :return: список имен приборов, которые отображаются на сайте
    """
    return list(
        map(lambda x: x[0], execute_query("SELECT name FROM devices WHERE show=TRUE"))
    )


def short_name_to_full_name_device(short_name):
    """
    :param short_name: короткое имя прибора
    :return: имя прибора по короткому имени прибора
    """
    return execute_query(
        f'SELECT name FROM devices WHERE name="{short_name}"',
        method="fetchone",
    )[0]


def make_list_complexes():
    """
    :return: список всех комплексов
    """
    return list(map(lambda x: x[0], execute_query("SELECT name FROM complexes")))


def get_devices_from_complex(complex_name):
    """
    :param complex_name: имя комплекса
    :return: список используемых приборов в комплексе
    """
    complex_id = execute_query(
        f'SELECT id FROM complexes WHERE name = "{complex_name}"',
        method="fetchone",
    )[0]
    return list(
        map(
            lambda x: x[0],
            execute_query(
                f'SELECT name FROM devices WHERE show=1 AND complex_id="{complex_id}"'
            ),
        )
    )


def make_list_cols(device_name):
    """
    :param device_name: имя прибора
    :return: список столбцов используемых в приборе
    """
    device_id = execute_query(
        f'SELECT id FROM devices WHERE name = "{device_name}"', method="fetchone"
    )[0]
    graph_ids = list(
        map(
            lambda x: x[0],
            execute_query(f'SELECT id FROM graphs WHERE device_id = "{device_id}"'),
        )
    )
    graph_ids_str = ", ".join(f'"{graph_id}"' for graph_id in graph_ids)
    return list(
        set(
            list(
                map(
                    lambda x: x[0],
                    execute_query(
                        f"SELECT name FROM columns WHERE graph_id IN ({graph_ids_str}) AND use=1"
                    ),
                )
            )
        )
    )


def get_color(col, device_name):
    """
    :param col: столбец
    :param device_name: прибор
    :return: цвет столбца в данном приборе
    """
    device_id = execute_query(
        f'SELECT id FROM devices WHERE name = "{device_name}"', method="fetchone"
    )[0]
    graph_ids = list(
        map(
            lambda x: x[0],
            execute_query(f'SELECT id FROM graphs WHERE device_id = "{device_id}"'),
        )
    )
    graph_ids_str = ", ".join(f'"{graph_id}"' for graph_id in graph_ids)
    return execute_query(
        f'SELECT color FROM columns WHERE name = "{col}" AND graph_id IN ({graph_ids_str})',
        method="fetchone",
    )[0]


def exception_decorator(func):
    """
    Декоратор для обработки ошибок, связанных с некорректным поведением пользователя
    """

    def wrapper(message):
        try:
            return func(message)
        except Exception as e:
            user_id = message if isinstance(message, int) else message.from_user.id
            name_func = func.__name__
            logging.warning(
                f"Непредвиденная ошибка: {e.__class__.__name__} в {name_func}"
            )
            bot.send_message(user_id, f"Непредвиденная ошибка в {name_func}")
            start(user_id, error_f=True)

    return wrapper


@bot.message_handler(commands=["start"])
def start(message, error_f=False):
    """
    После запуска бота появляется вывод этой функции (далее экран).
    Здесь можно выбрать получить быстрый доступ к нужному графику или полностью настроить параметры графика заново.
    start("start") -> quick_access("Быстрый доступ") / choice_devices_or_complexes("Просмотр данных с приборов")

    :param message: сообщение пользователя
    :param error_f: флаг откуда была вызвана функция (стандартно (через ТГ) или при ошибке (через exception_decorator))
    """
    user_id = message if error_f else str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    # Обнуление параметров пользователя при крупной ошибке
    if user_id not in user_info_open.keys() or error_f:
        user_info_open[user_id] = {}
    # Задание стартовых параметров пользователя
    user_info_open[user_id]["update_quick_access"] = False
    user_info_open[user_id].pop("selected_columns", None)
    user_info_open[user_id]["device_to_choose"] = []
    upload_json("user_info.json", user_info_open)
    # Создание кнопок действий
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Просмотр данных с приборов"))
    markup.add(types.KeyboardButton("Быстрый доступ"))
    bot.send_message(user_id, text=f"Начните работу с приборами", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text == "Быстрый доступ")
@exception_decorator
def quick_access(message):
    """
    Если пользователь со страницы start выбрал "Быстрый доступ", то он попал сюда.
    Эта функция позволяет настроить быстрый доступ или построить график, если пользователь настроил его ранее
    quick_access("Быстрый доступ") ->
    logic_draw_plot("Отрисовка графика") / update_quick_access("Настроить быстрый доступ")
    """
    user_id = str(message.from_user.id)
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add("Настроить быстрый доступ")
    user_info_open = load_json("user_info.json")
    # Если пользователь уже настроил быстрый доступ
    if "quick_access" in user_info_open[user_id].keys():
        markup.add("Отрисовка графика")
    bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text == "Отрисовка графика")
@exception_decorator
def logic_draw_plot(message):
    """
    Если пользователь со страницы quick_access выбрал "Отрисовка графика", то он попал сюда.
    Эта функция обрабатывает нажатие на кнопку "Отрисовка графика" и перенаправляет на построение графика(make_graph)
    logic_draw_plot("Отрисовка графика") -> make_graph
    """
    make_graph(message)


@bot.message_handler(func=lambda message: message.text == "Настроить быстрый доступ")
@exception_decorator
def update_quick_access(message):
    """
    Если пользователь со страницы quick_access выбрал "Настроить быстрый доступ", то он попал сюда.
    Эта функция задает начало настройки быстрого доступа
    update_quick_access("Настроить быстрый доступ") -> choice_devices_or_complexes("Просмотр данных с приборов")
    """
    user_id = str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    user_info_open[user_id]["update_quick_access"] = True
    upload_json("user_info.json", user_info_open)
    choice_devices_or_complexes(message)


@bot.message_handler(func=lambda message: message.text == "Просмотр данных с приборов")
@exception_decorator
def choice_devices_or_complexes(message):
    """
    Если пользователь нажал "Просмотр данных с приборов" с start или c update_quick_access, то попал сюда.
    Здесь пользователь выбирает каким образом просмотреть приборы: все сразу или по комплексам
    choice_devices_or_complexes("Просмотр данных с приборов") ->
    all_devices("Просмотр всех приборов") / all_complexes("Просмотр приборов по комплексам")
    """
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("Просмотр всех приборов"))
    markup.add(types.KeyboardButton("Просмотр приборов по комплексам"))
    bot.send_message(
        message.chat.id, text=f"Каким образом выбрать прибор?", reply_markup=markup
    )


@bot.message_handler(func=lambda message: message.text == "Просмотр всех приборов")
@exception_decorator
def all_devices(message):
    """
    При нажатии юзер "Просмотр всех приборов" с choice_devices_or_complexes или из choose_one_complex, то попал сюда.
    В этой функции пользователю высвечиваются все доступные для его выбора приборы
    all_devices("Просмотр всех приборов") -> choose_device(один из доступных приборов)
    """
    user_id = str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    # Если пользователь выбирал комплекс, то user_info_open[user_id]["device_to_choose"] уже не пустой
    if not user_info_open[user_id]["device_to_choose"]:
        user_info_open[user_id]["device_to_choose"] = make_list_short_name_devices()
        upload_json("user_info.json", user_info_open)
    # Создание кнопок на которых показаны все приборы, доступные пользователю
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add(
        *list(
            map(
                lambda x: types.KeyboardButton(x),
                user_info_open[user_id]["device_to_choose"],
            )
        )
    )
    bot.send_message(message.chat.id, "Выберите прибор", reply_markup=markup)


@bot.message_handler(
    func=lambda message: message.text in make_list_short_name_devices()
)
@exception_decorator
def choose_device(message):
    """
    Если пользователь выбрал один из приборов в all_devices, то он попал сюда.
    Здесь происходит запись этого прибора в user_info.json
    choose_device(один из доступных приборов) -> choose_time_delay
    """
    # Здесь есть if, тк. choose_device вызывается так же из make_graph_again_ind, где device уже выбран
    if message.text in make_list_short_name_devices():
        user_id = str(message.from_user.id)
        user_info_open = load_json("user_info.json")
        user_info_open[user_id]["device"] = short_name_to_full_name_device(message.text)
        upload_json("user_info.json", user_info_open)
    choose_time_delay(message)


@bot.message_handler(
    func=lambda message: message.text == "Просмотр приборов по комплексам"
)
@exception_decorator
def all_complexes(message):
    """
    Если пользователь выбрал "Просмотр приборов по комплексам" в choice_devices_or_complexes, то попал сюда
    В этой функции пользователю высвечиваются все доступные для его выбора комплексы
    all_complexes("Просмотр приборов по комплексам") -> choose_one_complex(один из доступных комплексов)
    """
    markup = types.ReplyKeyboardMarkup(row_width=1)
    markup.add(*list(map(lambda x: types.KeyboardButton(x), make_list_complexes())))
    bot.send_message(message.chat.id, "Выберите комплекс", reply_markup=markup)


@bot.message_handler(func=lambda message: message.text in make_list_complexes())
@exception_decorator
def choose_one_complex(message):
    """
    Если пользователь выбрал один из комплексов в all_complexes, то попал сюда.
    В этой функции по выбранному комплексу в device_to_choose сохраняются доступные приборы из этого комплекса
    choose_one_complex(один из доступных комплексов) -> all_devices(Один из доступных приборов)
    """
    user_id = str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    user_info_open[user_id]["device_to_choose"] = get_devices_from_complex(message.text)
    upload_json("user_info.json", user_info_open)
    all_devices(message)


def choose_time_delay(message):
    """
    После выбора прибора пользователь попадает сюда.
    Здесь пользователю отображаются временные промежутки (стандартные (2, 7, 14, 31) и НЕ стандартный)
    choose_time_delay -> get_delay(2, 7, 14, 31 день) / choose_not_default_start_date("Свой временной промежуток")
    """
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
@exception_decorator
def get_delay(message):
    """
    Если пользователь выбрал стандартные промежутки ("2 дня", "7 дней", "14 дней", "31 день"), то попал сюда.
    Здесь пользователь выбирает какой стандартный промежуток он хочет
    get_delay(2, 7, 14, 31 день) -> choose_columns
    """
    user_id = str(message.from_user.id)
    delay = (
        2
        if message.text == "2 дня"
        else 7 if message.text == "7 дней" else 14 if message.text == "14 дней" else 31
    )
    user_info_open = load_json("user_info.json")
    # НЕ тривиально: delay может быть int(стандартный диапазон), а может быть tuple(НЕ стандартный)
    user_info_open[user_id]["delay"] = delay
    upload_json("user_info.json", user_info_open)
    choose_columns(message)


def make_range(device):
    """
    Нужна для поиска временных границ прибора
    :param device: прибор
    :return: Начало и конец временного отрезка
    """
    list_files = os.listdir(f"{path_to_site}/msu_aerosol/proc_data/{device}")
    last_record_date = pd.to_datetime(
        pd.read_csv(f"{path_to_site}/msu_aerosol/proc_data/{device}/{max(list_files)}")[
            "timestamp"
        ].iloc[-1]
    )
    first_record_date = pd.to_datetime(
        pd.read_csv(f"{path_to_site}/msu_aerosol/proc_data/{device}/{min(list_files)}")[
            "timestamp"
        ].iloc[0]
    )
    return first_record_date, last_record_date


@bot.message_handler(func=lambda message: message.text == "Свой временной промежуток")
@exception_decorator
def choose_not_default_start_date(message):
    """
    Если пользователь выбрал "Свой временной промежуток", то попадает сюда.
    Здесь пользователю выводятся временные границы прибора, а он вводит начало временного отрезка
    choose_not_default_start_date("Свой временной промежуток") -> begin_record_date_choose
    """
    user_id = str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    user_info_open = user_info_open[user_id]
    device = user_info_open["device"]
    first_record_date, last_record_date = make_range(device)
    first_record_date = first_record_date.strftime("%d.%m.%Y")
    last_record_date = last_record_date.strftime("%d.%m.%Y")
    bot.send_message(
        message.chat.id, f"Данные доступны с {first_record_date} по {last_record_date}"
    )
    msg = bot.send_message(
        message.chat.id,
        "Дата начала отрезка данных (в формате 'день.месяц.год')",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    bot.register_next_step_handler(msg, begin_record_date_choose)


def begin_record_date_choose(message):
    """
    После вывода временных границ пользователь попадает сюда.
    Здесь проверяется корректность введенной даты
    begin_record_date_choose -> choose_not_default_finish_date
    """
    user_id = str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    device = user_info_open[user_id]["device"]
    first_record_date, last_record_date = make_range(device)
    try:  # Проверяем правильность ввода
        begin_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()
        if not last_record_date.date() >= begin_record_date >= first_record_date.date():
            raise ValueError
        user_info_open[user_id]["delay"] = [str(begin_record_date)]
        upload_json("user_info.json", user_info_open)
        choose_not_default_finish_date(message)
    except ValueError:  # При ошибке пользователь вводит дату заново
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_start_date(message)


def choose_not_default_finish_date(message):
    """
    После корректного ввода начальной даты пользователь попадает сюда.
    Здесь выводится информация о дате конца отрезка.
    choose_not_default_finish_date -> end_record_date_choose
    """
    msg = bot.send_message(
        message.chat.id,
        "Дата конца отрезка данных (в формате 'день.месяц.год')",
    )
    bot.register_next_step_handler(msg, end_record_date_choose)


def end_record_date_choose(message):
    """
    После choose_not_default_finish_date пользователь попадает сюда.
    Функция для ввода и проверки конечной даты отрезка.
    """
    user_id = str(message.from_user.id)
    user_info_open = load_json("user_info.json")
    device = user_info_open[user_id]["device"]
    first_record_date, last_record_date = make_range(device)
    try:
        end_record_date = datetime.strptime(message.text, "%d.%m.%Y").date()

        start_date = pd.to_datetime(user_info_open[user_id]["delay"][0]).date()
        if not (last_record_date.date() >= end_record_date >= start_date):
            raise ValueError
        user_info_open[user_id]["delay"] = [str(start_date), str(end_record_date)]
        upload_json("user_info.json", user_info_open)
        choose_columns(message)
    except ValueError:
        bot.send_message(message.chat.id, "Введена некорректная дата")
        choose_not_default_finish_date(message)


def draw_inline_keyboard(selected_columns, ava_col):
    """
    Функция вызывается из choose_columns, когда надо вывести кнопки с ✔️/❌ в зависимости от выбора пользователя
    :param selected_columns: выбранные столбцы
    :param ava_col: доступные столбцы
    :return: "приукрашенные" кнопки
    """
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
# @exception_decorator
def choose_columns(call):
    """
    После выбора временного диапазона пользователь попадает сюда
    В этой функции объеденины все этапы выбора столбцов
    - Стартовая отрисовка столбцов
    - Изменение существующего сообщения после добавления/удаления столбца пользователем
    - Закрепление ответа пользователя и переход дальше
    :param call: сообщение пользователя
    choose_columns -> make_graph
    """
    user_id = str(call.from_user.id)
    user_info_open = load_json("user_info.json")
    ava_col = make_list_cols(user_info_open[str(call.from_user.id)]["device"])
    # НЕ тривиально: тк здесь существуют ответы типа CallbackQuery и у него другой метод получения текста ->
    # надо делать другой обработчик
    if isinstance(call, CallbackQuery):
        text = call.data
    else:
        text = call.text
    if text.startswith("feature"):  # Изменение списка выбранных параметров
        feature = "_".join(call.data.split("feature")[1].split("_")[1::])
        selected_features = user_info_open[user_id]["selected_columns"]
        if feature in selected_features:
            selected_features.remove(feature)
            bot.answer_callback_query(call.id, "Вы убрали столбец " + feature)
        else:
            selected_features.append(feature)
            bot.answer_callback_query(call.id, "Вы добавили столбец " + feature)
        user_info_open[user_id]["selected_columns"] = selected_features
        upload_json("user_info.json", user_info_open)
        bot.answer_callback_query(call.id, "Вы выбрали параметр " + feature)
        selected_columns = user_info_open[str(call.from_user.id)]["selected_columns"]
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text="Нажми",
            reply_markup=draw_inline_keyboard(
                sorted(selected_columns), sorted(ava_col)
            ),
        )

    elif text == "next":  # Сохранение параметром и переход дальше
        if (
            len(json.load(open("user_info.json", "r"))[user_id]["selected_columns"])
            != 0
        ):
            make_graph(call)
        else:
            bot.answer_callback_query(call.id, "Ни один параметр не выбран!")
    else:  # Стартовый вывод столбцов
        user_info_open = load_json("user_info.json")
        if "selected_columns" not in user_info_open[user_id].keys():
            user_info_open[user_id]["selected_columns"] = []
        upload_json("user_info.json", user_info_open)
        selected_columns = user_info_open[user_id]["selected_columns"]
        bot.send_message(
            call.chat.id,
            "Столбцы для выбора:",
            reply_markup=draw_inline_keyboard(
                sorted(selected_columns), sorted(ava_col)
            ),
        )


def make_graph(message):
    """
    После выбора столбцов пользователь оказывается здесь.
    Функция для построения и вывода итогового графика
    """
    user_id = str(message.from_user.id)
    bot.send_message(user_id, "Строю график")
    if isinstance(message, CallbackQuery):
        text = message.data
    else:
        text = message.text
    user_info_open = load_json("user_info.json")
    # Если перешли через "Настроить быстрый доступ"
    if user_info_open[user_id]["update_quick_access"]:
        user_info_open[user_id]["quick_access"] = user_info_open[user_id].copy()
        user_info_open[user_id]["update_quick_access"] = False
        upload_json("user_info.json", user_info_open)
        bot.send_message(user_id, "Параметры для быстрого доступа выбраны. ")
    # Если перешли через "Быстрый доступ" (без настройки)
    if text == "Отрисовка графика":
        user_info_open = load_json("user_info.json")
        id_open = user_info_open[user_id]["quick_access"]
    else:
        user_info_open = load_json("user_info.json")
        id_open = user_info_open[user_id]
    device = id_open["device"]
    delay = id_open["delay"]
    # Если delay int -> стандартный промежуток иначе нет
    if isinstance(delay, int):
        end_record_date = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
        begin_record_date = end_record_date - timedelta(delay)
    else:
        end_record_date = pd.to_datetime(delay[1])
        begin_record_date = pd.to_datetime(delay[0])
    current_date, combined_data = begin_record_date, pd.DataFrame()
    # Соединяю файлы за разные месяца, которые могли попасть в промежуток
    while current_date <= end_record_date + timedelta(days=100):
        try:
            data = pd.read_csv(
                f"{path_to_site}/msu_aerosol/proc_data/{device}/{current_date.strftime('%Y_%m')}.csv"
            )
            combined_data = pd.concat([combined_data, data], ignore_index=True)
            current_date += timedelta(days=29)
        except FileNotFoundError:
            current_date += timedelta(days=29)
    # Если итоговый файл оказался пустым (например, прибор не работает)
    if combined_data.empty:
        fig = px.line(combined_data)
    else:
        # Во время пред обработки всегда создается столбец - timestamp
        time_col = "timestamp"
        combined_data[time_col] = pd.to_datetime(
            combined_data[time_col],
            format="%Y-%m-%d %H:%M:%S",
        )
        # Обрезаем dataframe согласно выбранному ранее временному диапазону
        combined_data = combined_data[
            (combined_data[time_col] >= begin_record_date)
            & (combined_data[time_col] <= end_record_date + timedelta(days=1))
        ]
        combined_data.set_index(time_col, inplace=True)
        combined_data = combined_data.replace(",", ".", regex=True).astype(float)
        cols_to_draw = id_open["selected_columns"]
        combined_data.reset_index(inplace=True)
        combined_data = combined_data.sort_values(by=time_col)
        # Сортируем столбцы таким образом, чтобы более маленькие рисовались позже
        cols_to_draw = (
            combined_data[cols_to_draw]
            .mean()
            .sort_values(ascending=False)
            .index.tolist()
        )
        fig = px.line(
            combined_data,
            x=time_col,
            y=cols_to_draw,
            color_discrete_sequence=[
                get_color(i, device) for i in cols_to_draw
            ],  # цвета столбцов
        )
    fig.update_layout(
        title=str(device),
        xaxis=dict(title="Time"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=True,
    )
    fig.update_traces(line={"width": 2})
    fig.update_xaxes(
        zerolinecolor="grey",
        zerolinewidth=1,
        gridcolor="grey",
        showline=True,
        linewidth=1,
        linecolor="black",
        mirror=True,
        tickformat="%H:%M\n%d.%m.%Y",
        minor_griddash="dot",
    )
    fig.update_yaxes(
        zerolinecolor="grey",
        zerolinewidth=1,
        gridcolor="grey",
        showline=True,
        linewidth=1,
        linecolor="black",
        mirror=True,
    )
    # Логирование
    logging.info(
        f"User {user_id} requested {device} for {begin_record_date} - {end_record_date} at {datetime.now()}"
    )
    # Сохранение картинки
    fig.write_image(f"graphs_photo/{user_id}.png")
    # Отправка картинки
    bot.send_photo(user_id, photo=open(f"graphs_photo/{user_id}.png", "rb"))
    plt.close()
    make_graph_again(user_id)


def make_graph_again(user_id):
    """
    После первого создания графика пользователь попадает сюда
    Отрисовка вариантов для перерисовки (Да, Нет)
    """
    markup = types.ReplyKeyboardMarkup(row_width=1)
    btn1 = types.KeyboardButton("Да")
    btn2 = types.KeyboardButton("Нет")
    markup.add(btn1, btn2)
    bot.send_message(
        user_id,
        "Построить график с другим временным диапазоном еще раз?",
        reply_markup=markup,
    )


@bot.message_handler(func=lambda message: message.text in ["Да", "Нет"])
@exception_decorator
def make_graph_again_ind(message):
    """
    После отрисовки вариантов для выбора пользователь попадает сюда
    Здесь пользователь может выбрать, перерисовать ему еще раз с другим временным диапазоном или нет
    """
    if message.text == "Да":
        choose_device(message)
    else:
        start(message)


# Если существует канал об ошибках ТГ бота
if config.id_alarm_ch != 0:
    while True:  # Чтобы не падало, при временных отключениях от ТГ
        try:
            bot.send_message(config.id_alarm_ch, "Bot started")  # Обращение к каналу о запуске бота
            bot.polling(none_stop=True)
        except Exception as error:  # Обращение к каналу о поломке бота
            bot.send_message(config.id_alarm_ch, "Bot program crashed with the error: " + str(error))
else:
    while True:  # Чтобы не падало, при временных отключениях от ТГ
        bot.polling(none_stop=True)