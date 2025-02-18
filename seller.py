import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров с платформы Ozon.

    Эта функция отправляет запрос к API Ozon для получения списка товаров с
    возможностью пагинации. Она использует параметр `last_id` для получения следующей
    страницы данных, пока все товары не будут загружены.

    Аргументы:
        last_id (str): Идентификатор последнего товара на предыдущей странице,
                        используемый для пагинации.
        client_id (str): Уникальный идентификатор клиента, предоставляемый Ozon.
        seller_token (str): Токен API, который используется для аутентификации при запросах.

    Возвращает:
        dict: Словарь с результатами запроса, содержащий список товаров и другие данные
              (например, количество товаров и идентификаторы для пагинации).

    Пример:
        >>> get_product_list("", "client_id_example", "seller_token_example")
        {
            "result": [
                {"offer_id": "123", "name": "Товар 1"},
                {"offer_id": "124", "name": "Товар 2"}
            ],
            "paging": {"nextPageToken": "next_id"}
        }

    Пример некорректного исполнения:
        >>> get_product_list("", "", "")
        Ошибка: Недействительный токен API или клиентский идентификатор.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает артикулы товаров с платформы Ozon.

    Эта функция использует пагинацию для получения всех товаров магазина Ozon, а затем
    извлекает их артикулы.

    Аргументы:
        client_id (str): Уникальный идентификатор клиента Ozon.
        seller_token (str): Токен API для аутентификации.

    Возвращает:
        list: Список артикулов товаров, которые принадлежат текущему клиенту.

    Пример:
        >>> get_offer_ids("client_id_example", "seller_token_example")
        ["123", "124", "125"]

    Пример некорректного исполнения:
        >>> get_offer_ids("", "")
        Ошибка: Недействительный токен API или клиентский идентификатор.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров на платформе Ozon.

    Эта функция отправляет запрос на обновление цен для списка товаров.

    Аргументы:
        prices (list): Список товаров с новыми ценами, где каждый элемент содержит
                        артикул и цену товара.
        client_id (str): Уникальный идентификатор клиента Ozon.
        seller_token (str): Токен API для аутентификации.

    Возвращает:
        dict: Ответ от API, подтверждающий успешное обновление цен.

    Пример:
        >>> update_price([{"offer_id": "123", "price": 5990}], "client_id_example", "seller_token_example")
        {"status": "success"}

    Пример некорректного исполнения:
        >>> update_price([{"offer_id": "123", "price": 5990}], "", "")
        Ошибка: Недействительный токен API или клиентский идентификатор.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на платформе Ozon.

    Эта функция отправляет запрос на обновление остатков товаров для списка товаров.

    Аргументы:
        stocks (list): Список товаров с новыми остатками, где каждый элемент содержит
                        артикул и количество товара.
        client_id (str): Уникальный идентификатор клиента Ozon.
        seller_token (str): Токен API для аутентификации.

    Возвращает:
        dict: Ответ от API, подтверждающий успешное обновление остатков.

    Пример:
        >>> update_stocks([{"offer_id": "123", "stock": 100}], "client_id_example", "seller_token_example")
        {"status": "success"}

    Пример некорректного исполнения:
        >>> update_stocks([{"offer_id": "123", "stock": 100}], "", "")
        Ошибка: Недействительный токен API или клиентский идентификатор.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл остатков товаров с сайта Casio.

    Функция скачивает архив с остатками товаров с сайта Casio, распаковывает его и
    конвертирует данные в список словарей для дальнейшей обработки.

    Возвращает:
        list: Список остатков товаров в формате словарей.

    Пример:
        >>> download_stock()
        [{'Код': '123', 'Количество': '10'}, {'Код': '124', 'Количество': '>10'}]

    Пример некорректного исполнения:
        Ошибка: Недоступен файл или архив.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков товаров для обновления на платформе Ozon.

    Эта функция создает список остатков товаров, исключая те, которые не были
    загружены в систему. Она также добавляет товары с нулевыми остатками, если
    они присутствуют в данных о товарах.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию
                                о кодах товаров и их количестве.
        offer_ids (list): Список артикулов товаров, загруженных в Ozon.

    Возвращает:
        list: Список словарей с артикулом и остатками товаров.

    Пример:
        >>> create_stocks([{"Код": "123", "Количество": "10"}], ["123", "124"])
        [{'offer_id': '123', 'stock': 10}, {'offer_id': '124', 'stock': 0}]

    Пример некорректного исполнения:
        >>> create_stocks([{"Код": "123", "Количество": "10"}], [])
        [{'offer_id': '123', 'stock': 10}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен для товаров для обновления на платформе Ozon.

    Эта функция создает список цен товаров на основе данных о товарах и их остатках,
    формируя необходимый формат для отправки в Ozon.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию
                                о кодах товаров и их ценах.
        offer_ids (list): Список артикулов товаров, загруженных в Ozon.

    Возвращает:
        list: Список словарей с артикулом и ценой товаров.

    Пример:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], ["123"])
        [{'offer_id': '123', 'price': '5990'}]

    Пример некорректного исполнения:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], [])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в числовое значение.

    Эта функция принимает строковое представление цены, которое может включать
    разделители тысяч (например, апострофы) и символы валюты, и возвращает строку,
    содержащую только числовую часть цены.

    Аргументы:
        price (str): Цена в строковом формате, например, "5'990.00 руб.".

    Возвращает:
        str: Чистое числовое значение цены, например, "5990".

    Пример:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        >>> price_conversion("1'500.50 руб.")
        '1500'

    Исключения:
        ValueError: Если строка не может быть преобразована в цену.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части по n элементов.

    Эта функция делит большой список на несколько подсписков по n элементов в каждом.
    Это полезно для отправки данных в API, где есть ограничение по количеству элементов
    в одном запросе.

    Аргументы:
        lst (list): Список, который необходимо разделить.
        n (int): Количество элементов в каждом подсписке.

    Возвращает:
        generator: Генератор, который возвращает подсписки длиной n.

    Пример:
        >>> list(divide([1, 2, 3, 4, 5, 6], 2))
        [[1, 2], [3, 4], [5, 6]]

    Пример некорректного исполнения:
        >>> list(divide([1, 2, 3], 0))
        Ошибка: Деление на ноль.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает обновленные цены на товары на платформу Ozon.

    Эта асинхронная функция сначала создает список цен для товаров, а затем
    отправляет данные на платформу Ozon по частям.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию о ценах.
        client_id (str): Уникальный идентификатор клиента Ozon.
        seller_token (str): Токен API для аутентификации.

    Возвращает:
        list: Список обновленных цен на товары.

    Пример:
        >>> await upload_prices([{"Код": "123", "Цена": "5'990.00 руб."}], "client_id_example", "seller_token_example")
        [{'offer_id': '123', 'price': '5990'}]

    Пример некорректного исполнения:
        >>> await upload_prices([], "", "")
        Ошибка: Недействительный токен API или клиентский идентификатор.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает обновленные остатки товаров на платформу Ozon.

    Эта асинхронная функция сначала создает список остатков для товаров, а затем
    отправляет данные на платформу Ozon по частям.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию о количестве.
        client_id (str): Уникальный идентификатор клиента Ozon.
        seller_token (str): Токен API для аутентификации.

    Возвращает:
        tuple: Кортеж, содержащий два списка:
               - список остатков товаров, у которых количество больше нуля.
               - полный список остатков товаров.

    Пример:
        >>> await upload_stocks([{"Код": "123", "Количество": "10"}], "client_id_example", "seller_token_example")
        ([{'offer_id': '123', 'stock': 10}], [{'offer_id': '123', 'stock': 10}])

    Пример некорректного исполнения:
        >>> await upload_stocks([], "", "")
        Ошибка: Недействительный токен API или клиентский идентификатор.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для обновления остатков и цен на Ozon.

    Функция загружает актуальные остатки с сайта Casio, создает список остатков
    и цен для товаров, а затем отправляет их в Ozon для обновления.

    Пример:
        >>> main()
        Успешно обновлены остатки и цены на Ozon.

    Пример некорректного исполнения:
        Ошибка: Недоступен файл остатков или некорректный токен API.
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
