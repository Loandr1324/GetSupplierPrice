# Author Loik Andrey mail: loikand@mail.ru
import asyncio

from config import FILE_NAME_LOG
from google_table.google_tb_work import WorkGoogle
from loguru import logger
from api_abcp.abcp_work import WorkABCP
from datetime import datetime as dt

# Задаём параметры логирования
logger.add(FILE_NAME_LOG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 week",
           compression="zip")


def filtered_products_by_flag(products: list[dict]):
    """
    Получаем позиции для выбора правила проценки
    :param products: list[dict]
    В передаваемых в списке словарей обязательно наличие ключа 'select_flag'
    :return:
    """
    filtered_products = [product for product in products if product['select_flag'] == '1']
    return filtered_products


def selected_rule_for_position(products: list[dict], rules: list[dict]) -> list[dict]:
    """
    Добавляем правила к каждой позиции
    :param rules: В словарях обязательно наличие ключей 'id_rule'
    :param products: В словарях обязательно наличие ключей 'id_rule'
    :return: Возвращаем список словарей products с добавленными ключами из правил словаря rules.
    """
    for product in products:
        product['id_rule'] = product['id_rule'].replace(' ', '').split(',')

        product_rules = {}
        for id_rule in product['id_rule']:
            filtered_rule = [rule for rule in rules if rule['id_rule'] == id_rule]
            if filtered_rule and len(filtered_rule) == 1:
                product_rules[id_rule] = filtered_rule[0]

            elif len(filtered_rule) > 1:
                product_rules[id_rule] = ''
                logger.error(f"В таблице не уникальное обозначение ID правила: {id_rule}")
                logger.error(f"Получены правила: {filtered_rule}")
            else:
                product_rules[id_rule] = ''
                logger.error(f"Не определено ни одного правила для позиции по его ID {id_rule}")

        product['id_rule'] = product_rules
    return products


def get_price_supplier(products: list[dict]) -> list[dict]:
    """
    Получение цены по каждой позиции
    :param products:
    :return:
    """
    work_abcp = WorkABCP()
    for product in products:
        result = asyncio.run(work_abcp.get_price_supplier(product['brand'], product['number']))
        product['result'] = {'first_result': len(result)}
        product['result']['id_rule'] = {}
        filtered_res = {}
        for rule in product['id_rule']:
            filtered_res[rule] = []
            product['result']['id_rule'][rule] = {}

            filtered_supplier = []
            for res in result:
                # Определяем правила фильтрации по поставщикам
                if not product['id_rule'][rule]['id_suppliers']:
                    check_supplier = True
                else:
                    rule_white_supplier = (product['id_rule'][rule]['type_select_supplier']
                                           and str(res['distributorId']) in product['id_rule'][rule]['id_suppliers'])
                    rule_black_supplier = (not product['id_rule'][rule]['type_select_supplier']
                                           and str(res['distributorId']) not in product['id_rule'][rule]['id_suppliers'])
                    check_supplier = rule_white_supplier or rule_black_supplier

                if check_supplier:
                    filtered_supplier.append(res)

            filtered_res[rule] = filtered_supplier
            product['result']['id_rule'][rule]['filter_by_supplier'] = len(filtered_supplier)

            # Фильтруем по маршрутам
            filtered_routes = []
            for res in filtered_res[rule]:
                # Определяем правила фильтрации по маршрутам
                if not product['id_rule'][rule]['name_routes']:
                    check_routes = True
                else:
                    rule_white_routes = (product['id_rule'][rule]['type_select_routes']
                                         and product['id_rule'][rule]['name_routes'] in str(res['supplierDescription']))
                    rule_black_routes = (not product['id_rule'][rule]['type_select_routes']
                                         and product['id_rule'][rule]['name_routes'] not in str(
                                res['supplierDescription']))
                    check_routes = rule_white_routes or rule_black_routes

                # Фильтруем позиции по правилу маршрута
                if check_routes:
                    filtered_routes.append(res)

            filtered_res[rule] = filtered_routes
            product['result']['id_rule'][rule]['filter_by_routes'] = len(filtered_res[rule])

            # Фильтруем по складам
            filtered_storage = []
            for res in filtered_res[rule]:
                # Определяем правила фильтрации по складам
                if not product['id_rule'][rule]['supplier_storage']:
                    check_storage = True
                else:
                    rule_white_storage = (product['id_rule'][rule]['type_select_supplier_storage']
                                          and product['id_rule'][rule]['supplier_storage'] in str(
                                res['supplierDescription']))
                    rule_black_storage = (not product['id_rule'][rule]['type_select_supplier_storage']
                                          and product['id_rule'][rule]['supplier_storage'] not in str(
                                res['supplierDescription']))
                    check_storage = rule_white_storage or rule_black_storage

                # Фильтруем позиции по правилу маршрута
                if check_storage:
                    filtered_storage.append(res)

            filtered_res[rule] = filtered_storage
            product['result']['id_rule'][rule]['filter_by_storage'] = len(filtered_res[rule])

            # Фильтруем по минимальному остатку поставщика
            filtered_min_stock = []
            min_stock = product['id_rule'][rule]['supplier_storage_min_stock']
            for res in filtered_res[rule]:
                # Определяем правила фильтрации по минимальному остатку
                if not min_stock:
                    check_min_stock = True
                else:
                    check_min_stock = int(res['availability']) >= int(min_stock)

                # Фильтруем позиции по правилу минимального остатка
                if check_min_stock:
                    filtered_min_stock.append(res)

            filtered_res[rule] = filtered_min_stock
            product['result']['id_rule'][rule]['filter_by_min_stock'] = len(filtered_res[rule])

            # Фильтруем по вероятности поставки поставщика
            filtered_delivery_probability = []
            delivery_probability = product['id_rule'][rule]['delivery_probability']
            for res in filtered_res[rule]:
                # Определяем правила фильтрации по вероятности поставки
                if not delivery_probability:
                    check_delivery_probability = True
                else:
                    check_delivery_probability = int(res['deliveryProbability']) >= int(delivery_probability)

                # Фильтруем позиции по правилу вероятности поставки
                if check_delivery_probability:
                    filtered_delivery_probability.append(res)

            filtered_res[rule] = filtered_delivery_probability
            product['result']['id_rule'][rule]['filter_by_delivery_probability'] = len(filtered_res[rule])

            # Фильтруем по сроку поставки поставщика
            filtered_delivery_period = []
            max_delivery_period = product['id_rule'][rule]['max_delivery_period']
            for res in filtered_res[rule]:
                # Определяем правила фильтрации по сроку поставки
                if not max_delivery_period:
                    check_delivery_period = True
                else:
                    check_delivery_period = int(res['deliveryPeriod']) <= int(max_delivery_period)

                # Фильтруем позиции по правилу сроку поставки
                if check_delivery_period:
                    filtered_delivery_period.append(res)

            filtered_res[rule] = filtered_delivery_period
            product['result']['id_rule'][rule]['filter_by_delivery_period'] = len(filtered_res[rule])

            # Фильтруем по отклонению в цене
            filtered_delivery_period = []
            price_deviation = product['id_rule'][rule]['price_deviation']
            for res in filtered_res[rule]:
                # Определяем правила фильтрации по цене
                if not price_deviation or not product['price']:
                    check_price_deviation = True
                else:
                    calc_deviation = abs(100 - int(res['price']) / int(product['price']) * 100)
                    check_price_deviation = calc_deviation <= int(price_deviation)

                # Фильтруем позиции по цене
                if check_price_deviation:
                    filtered_delivery_period.append(res)

            filtered_res[rule] = filtered_delivery_period
            product['result']['id_rule'][rule]['filter_by_price_deviation'] = len(filtered_res[rule])

            # Выбираем одно предложение по указанному правилу (срок или цена)
            filtered_selection_rule = []
            selection_rule = product['id_rule'][rule]['type_selection_rule']
            selection_rule = "Цена" if not selection_rule else selection_rule
            if filtered_res[rule]:
                if selection_rule.lower() == "цена":
                    filtered_selection_rule = sorted(filtered_res[rule], key=lambda x: float(x['price']), reverse=True)[:1]

                elif selection_rule.lower() == "остаток":
                    filtered_selection_rule = sorted(
                        filtered_res[rule], key=lambda x: float(x['deliveryPeriod']), reverse=True
                    )[:1]
            else:
                filtered_selection_rule = []

            filtered_res[rule] = filtered_selection_rule
            logger.debug(filtered_res[rule])
            product['result']['id_rule'][rule]['select_count_product'] = len(filtered_res[rule])
            product['result']['id_rule'][rule]['select_product'] = filtered_res[rule]

    return products


def sort_price_products(products: list[dict]) -> (list[dict], list[dict]):
    """
    Сортируем результаты проценки полученные от поставщика на позиции с полученной ценой и без
    :param products:
    :return:
    """
    price_product = []
    err_price_product = []
    date_now = dt.now().strftime("%d.%m.%Y")

    for product in products:
        logger.debug(product)
        for id_rule_result in product['result']['id_rule']:
            rule_result = product['result']['id_rule'][id_rule_result]
            if rule_result['select_product']:
                # Записываем результат в таблицу с исходными данными
                price_product.append({
                    'number': product['number'],
                    'brand': product['brand'],
                    'row_product_on_sheet': product['row_product_on_sheet'],
                    'last_update_date': date_now,
                    'new_price': rule_result['select_product'][0]['price']
                })
                break
            else:
                # Записываем ошибки получения цены в таблицу с ошибками
                err_price_product.append({
                    'number': product['number'],
                    'brand': product['brand'],
                    'description': product['description'],
                    'last_update_date': date_now,
                    'id_rule': id_rule_result,
                    'first_result': product['result']['first_result'],
                    'filter_by_supplier': rule_result['filter_by_supplier'],
                    'filter_by_routes': rule_result['filter_by_routes'],
                    'filter_by_storage': rule_result['filter_by_storage'],
                    'filter_by_min_stock': rule_result['filter_by_min_stock'],
                    'filter_by_delivery_probability': rule_result['filter_by_delivery_probability'],
                    'filter_by_delivery_period': rule_result['filter_by_delivery_period'],
                    'filter_by_price_deviation': rule_result['filter_by_price_deviation'],
                    'select_count_product': rule_result['select_count_product']
                })
            if err_price_product:
                price_product.append({
                    'number': product['number'],
                    'brand': product['brand'],
                    'row_product_on_sheet': product['row_product_on_sheet'],
                    'last_update_date': date_now,
                    'new_price': ''
                })

    return price_product, err_price_product


def main():
    """
    Основной процесс программы
    :return:
    """
    logger.info(f"... Запуск программы")
    # Получаем данные из Google таблицы
    wk_g = WorkGoogle()
    products = wk_g.get_products()
    count_row = len(products)
    logger.info(f"Всего позиций на листе: {count_row}")

    products = filtered_products_by_flag(products)
    logger.info(f"Позиций для получения цены: {len(products)}")
    # logger.debug(products)

    # Получаем правила для проценки
    rules = wk_g.get_price_filter_rules()
    # logger.debug(rules)

    # Подставляем правила для отфильтрованных позиций
    products = selected_rule_for_position(products, rules)

    # Получаем цену поставщика согласно правил
    products = get_price_supplier(products)

    # Выбираем позиции с полученной ценой и без цены
    new_price_product, err_price_product = sort_price_products(products)
    logger.debug(f"{new_price_product=}")
    logger.debug(f"{err_price_product=}")

    # # Записываем полученные цены и возвращаем позиции без цены.
    wk_g.set_price_products(new_price_product, count_row, name_column=['E', 'F'])

    # # Записываем в Google таблицу данные по количеству выбранных позиций на каждом этапе
    wk_g.save_new_result_on_sheet(err_price_product, 2, 2)

    logger.info(f"... Окончание работы программы")


if __name__ == "__main__":
    main()
