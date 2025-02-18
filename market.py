import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
     """Получает список товаров на платформе Яндекс.Маркет.

    Эта функция отправляет запрос на API Яндекс.Маркет для получения списка товаров
    с использованием пагинации. С помощью параметра `page` загружается следующая страница
    товаров, пока все товары не будут загружены.

    Аргументы:
        page (str): Токен страницы для пагинации.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        access_token (str): Токен доступа для аутентификации API.

    Возвращает:
        list: Список товаров с различными характеристиками, такими как артикулы и другие данные.

    Пример:
        >>> get_product_list("", "campaign_id_example", "access_token_example")
        [{"offer_id": "123", "name": "Товар 1"}, {"offer_id": "124", "name": "Товар 2"}]

    Пример некорректного исполнения:
        >>> get_product_list("", "", "")
        Ошибка: Недействительный токен доступа или кампания.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров на Яндекс.Маркет.

    Эта функция отправляет запрос на обновление остатков товаров в Яндекс.Маркет
    с использованием переданных данных об остатках товаров.

    Аргументы:
        stocks (list): Список остатков товаров, содержащий артикулы и их количество.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        access_token (str): Токен доступа для аутентификации API.

    Возвращает:
        dict: Ответ от API, подтверждающий успешное обновление остатков.

    Пример:
        >>> update_stocks([{"sku": "123", "count": 100}], "campaign_id_example", "access_token_example")
        {"status": "success"}

    Пример некорректного исполнения:
        >>> update_stocks([{"sku": "123", "count": 100}], "", "")
        Ошибка: Недействительный токен доступа или кампания.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров на Яндекс.Маркет.

    Эта функция отправляет запрос на обновление цен товаров на платформе Яндекс.Маркет.

    Аргументы:
        prices (list): Список товаров с новыми ценами, где каждый элемент содержит артикул и цену товара.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        access_token (str): Токен доступа для аутентификации API.

    Возвращает:
        dict: Ответ от API, подтверждающий успешное обновление цен.

    Пример:
        >>> update_price([{"offer_id": "123", "price": 5990}], "campaign_id_example", "access_token_example")
        {"status": "success"}

    Пример некорректного исполнения:
        >>> update_price([{"offer_id": "123", "price": 5990}], "", "")
        Ошибка: Недействительный токен доступа или кампания.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы товаров Яндекс.Маркет.

    Эта функция извлекает артикулы товаров, используя пагинацию для получения всех товаров
    и их артикулов в указанной кампании на Яндекс.Маркет.

    Аргументы:
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        market_token (str): Токен доступа к API Яндекс.Маркет.

    Возвращает:
        list: Список артикулов товаров в кампании.

    Пример:
        >>> get_offer_ids("campaign_id_example", "market_token_example")
        ["123", "124", "125"]

    Пример некорректного исполнения:
        >>> get_offer_ids("", "")
        Ошибка: Недействительный токен доступа или кампания.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает список остатков товаров для обновления на Яндекс.Маркет.

    Эта функция создает список остатков товаров, исключая те, которые не были загружены в систему.
    Она добавляет товары с нулевыми остатками, если они присутствуют в данных о товарах.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию о кодах товаров и их количестве.
        offer_ids (list): Список артикулов товаров, загруженных в Яндекс.Маркет.
        warehouse_id (str): Идентификатор склада для товара.

    Возвращает:
        list: Список словарей с артикулом и остатками товаров.

    Пример:
        >>> create_stocks([{"Код": "123", "Количество": "10"}], ["123", "124"], "warehouse_1")
        [{'sku': '123', 'warehouseId': 'warehouse_1', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2022-12-31T12:00:00Z'}]}, {'sku': '124', 'warehouseId': 'warehouse_1', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2022-12-31T12:00:00Z'}]}]

    Пример некорректного исполнения:
        >>> create_stocks([{"Код": "123", "Количество": "10"}], [], "warehouse_1")
        [{'sku': '123', 'warehouseId': 'warehouse_1', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2022-12-31T12:00:00Z'}]}]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен товаров для обновления на Яндекс.Маркет.

    Эта функция создает список цен для товаров, используя данные о товарах и остатках,
    и формирует формат для отправки на Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию о кодах товаров и их ценах.
        offer_ids (list): Список артикулов товаров, загруженных в Яндекс.Маркет.

    Возвращает:
        list: Список словарей с артикулом и ценой товаров.

    Пример:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], ["123"])
        [{'offer_id': '123', 'price': 5990}]

    Пример некорректного исполнения:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], [])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загружает обновленные цены товаров на Яндекс.Маркет.

    Эта асинхронная функция сначала создает список цен для товаров, а затем отправляет данные
    на платформу Яндекс.Маркет по частям, чтобы избежать превышения ограничений по количеству
    товаров в одном запросе.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию о ценах.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        market_token (str): Токен доступа для аутентификации API.

    Возвращает:
        list: Список словарей с ценами для товаров.

    Пример:
        >>> await upload_prices([{"Код": "123", "Цена": "5'990.00 руб."}], "campaign_id_example", "market_token_example")
        [{'offer_id': '123', 'price': 5990}]

    Пример некорректного исполнения:
        >>> await upload_prices([], "", "")
        Ошибка: Недействительный токен доступа или кампания.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загружает обновленные остатки товаров на Яндекс.Маркет.

    Эта асинхронная функция сначала создает список остатков для товаров, а затем отправляет данные
    на платформу Яндекс.Маркет по частям. Остатки товаров с нулевыми значениями исключаются
    из финального списка, отправляемого на платформу.

    Аргументы:
        watch_remnants (list): Список остатков товаров, содержащий информацию о количестве.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        market_token (str): Токен доступа для аутентификации API.
        warehouse_id (str): Идентификатор склада, на котором хранятся товары.

    Возвращает:
        tuple: Кортеж, содержащий два списка:
               - список остатков товаров, у которых количество больше нуля;
               - полный список остатков товаров.

    Пример:
        >>> await upload_stocks([{"Код": "123", "Количество": "10"}], "campaign_id_example", "market_token_example", "warehouse_1")
        ([{'offer_id': '123', 'stock': 10}], [{'offer_id': '123', 'stock': 10}])

    Пример некорректного исполнения:
        >>> await upload_stocks([], "", "", "")
        Ошибка: Недействительный токен доступа, кампания или склад.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для обновления остатков и цен на Яндекс.Маркет.

    Эта функция загружает данные о товарах, остатках и ценах, а затем обновляет эти данные
    на платформе Яндекс.Маркет для двух типов складов: FBS и DBS.

    Пример:
        >>> main()
        Успешно обновлены остатки и цены на Яндекс.Маркет.

    Пример некорректного исполнения:
        Ошибка: Проблемы с подключением или некорректный токен API.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
