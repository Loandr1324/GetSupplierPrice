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


def get_price_supplier(products: list[dict], own_warehouses: list) -> list[dict]:
    """
    Получение цены согласно заданных правил
    :param products: Список словарей с товарами для проценки
    :param own_warehouses: Список своих складов
    :return:
    """
    logger.debug(products)
    work_abcp = WorkABCP()
    for product in products:
        result = asyncio.run(work_abcp.get_price_supplier(product['brand'], product['number']))

        logger.info(f"Количество предложений от ABCP: {len(result)}")
        result = [res for res in result if str(res['distributorId']) not in own_warehouses]
        logger.info(f"Количество предложений от ABCP без своих складов: {len(result)}")

        product['result'] = {'first_result': len(result)}
        product['result']['id_rule'] = {}

        for rule in product['id_rule']:
            product['result']['id_rule'][rule] = {}
            product = filtered_result(result, rule, product)
    return products


def pass_filter_by_supplier(result: list[dict], id_rule: str, product: dict) -> (dict, list[dict]):
    """
    Пропускаем фильтр по поставщикам
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: Данные по процениваемому продукту
    :return:
    """
    logger.debug("Не учитываем правила поставщиков")
    product['result']['id_rule'][id_rule]['filter_by_supplier'] = len(result)
    return product, result


def filter_by_black_supplier(result: list[dict], id_rule: str, product: dict) -> (dict, list[dict]):
    """
    Фильтруем по чёрному списку поставщиков
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: Данные по процениваемому продукту
    :return:
    """
    logger.info("Фильтруем по чёрному списку поставщиков")
    rule_details = product.get('id_rule', {}).get(id_rule, {})
    black_list = rule_details.get('id_suppliers', '').replace(' ', '').split(',') \
        if isinstance(rule_details.get('id_suppliers'), str) else rule_details.get('id_suppliers', [])

    filtered_by_supplier = [res for res in result if str(res.get('distributorId')) not in black_list]
    count_matches = len(filtered_by_supplier)
    logger.debug(f"Получили {count_matches} результат(ов) после фильтрации по чёрному списку поставщиков")

    product['result']['id_rule'][id_rule]['filter_by_supplier'] = count_matches
    return product, filtered_by_supplier


def filter_by_white_supplier(result: list[dict], id_rule: str, product: dict) -> (dict, list[dict]):
    """
    Фильтруем по белому списку поставщиков
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: Данные по процениваемому продукту
    :return: (product, filtered_by_white_supplier)
    """
    logger.info(f"Фильтруем по белому списку поставщиков")
    filtered_by_white_supplier = []
    white_list = product['id_rule'][id_rule]['id_suppliers'].replace(' ', '').split(',')
    logger.debug(f"Список поставщиков: {white_list}")
    for supplier in white_list:
        if supplier == "*":
            filtered_by_white_supplier = [res for res in result if str(res['distributorId']) not in white_list]
        else:
            filtered_by_white_supplier = [res for res in result if str(res['distributorId']) == supplier]
        count_matches = len(filtered_by_white_supplier)

        filter_key = 'filter_by_supplier'
        existing_value = product['result']['id_rule'].get(id_rule, {}).get(filter_key, "")
        new_value = f"{supplier}: {count_matches}"
        if not product['result'].get('id_rule'):
            product['result']['id_rule'] = {}
        if not product['result']['id_rule'].get(id_rule):
            product['result']['id_rule'][id_rule] = {}
        product['result']['id_rule'][id_rule][filter_key] = \
            f"{existing_value}, {new_value}" if existing_value else new_value

        logger.error(product['result']['id_rule'][id_rule])
        logger.debug(f"Получили {len(filtered_by_white_supplier)} результат(ов) по поставщику {supplier}")

        # Фильтруем по маршруту
        product, filtered_by_white_supplier = filter_by_routes(filtered_by_white_supplier, id_rule, product, supplier)
        logger.warning(f"Количество после фильтрации по маршруту: {product['result']['id_rule'][id_rule]}")

        # Фильтруем по складам
        product, filtered_by_white_supplier = filter_by_storage(filtered_by_white_supplier, id_rule, product, supplier)
        logger.warning(f"Количество после фильтраций по белому списку поставщика {supplier}: "
                       f"{product['result']['id_rule'][id_rule]}")

        # Если позиция найдена, то выходим
        if filtered_by_white_supplier:
            logger.warning(f"Нашли {len(filtered_by_white_supplier)} предложения по поставщику: {supplier}. "
                           f"Прерываем дальнейшую фильтрацию")
            break

    return product, filtered_by_white_supplier


def filter_by_routes(result: list[dict], id_rule: str, product: dict, supplier: str = '') -> (dict, list[dict]):
    """
    Фильтруем по маршрутам
    :param result: В списке словарей обязательно наличие ключа 'supplierDescription'
    :param id_rule: Номер правила
    :param product: В словаре обязательно наличие ключа ['result':'{id_rule': ...}]
    :param supplier: Идентификатор поставщика
    :return: (product, filtered_by_routes)
    """
    logger.info("Фильтруем по маршрутам")
    filtered_by_routes = []
    rule_details = product['id_rule'][id_rule]
    name_routes = rule_details.get('name_routes', [])

    for res in result:
        supplier_description = str(res['supplierDescription'])
        if name_routes:
            # matches = name_routes in supplier_description
            matches = any(route in supplier_description for route in name_routes if route)
            check_routes = matches if rule_details.get('type_select_routes', False) else not matches
        else:
            check_routes = True

        if check_routes:
            filtered_by_routes.append(res)

    count_matches = len(filtered_by_routes)
    filter_key = 'filter_by_routes'
    result_id_rule = product['result'].setdefault('id_rule', {}).setdefault(id_rule, {})
    existing_value = result_id_rule.get(filter_key, "")
    new_value = f"{supplier}: {count_matches}" if supplier else str(count_matches)

    logger.info(f"Получили {count_matches} результат(ов) по маршрутам")

    result_id_rule[filter_key] = f"{existing_value}, {new_value}" if existing_value else new_value
    return product, filtered_by_routes


def pass_filter_by_storage(result: list[dict], id_rule: str, product: dict, supplier: str = '') -> (dict, list[dict]):
    """
    Пропускаем правила по складам и записываем количество результатов на этом этапе фильтрации
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: Данные по процениваемому продукту
    :param supplier: Идентификатор поставщика
    :return: (product, result)
    """
    logger.debug(f"Пропускаем правила складов для правила {id_rule} и поставщика {supplier}")
    count_matches = len(result)
    filter_key = 'filter_by_storage'
    result_id_rule = product['result'].setdefault('id_rule', {}).setdefault(id_rule, {})
    existing_value = result_id_rule.get(filter_key, "")
    new_value = f"{supplier}: {count_matches}" if supplier else str(count_matches)

    logger.debug(f"Получили {count_matches} результат(ов) по складам для правила {id_rule}")
    result_id_rule[filter_key] = f"{existing_value}, {new_value}" if existing_value else new_value
    return product, result


def filter_by_black_storage(result: list[dict], id_rule: str, product: dict, supplier: str = '') -> (dict, list[dict]):
    """
    Исключаем позиции со складов из чёрного списка
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: Данные по процениваемому продукту
    :param supplier: Идентификатор поставщика
    :return: (product, result)
    """
    logger.debug("Фильтруем по чёрному списку складов")
    rule_details = product['id_rule'][id_rule]
    black_list = rule_details.get('supplier_storage', '').replace(' ', '').split(',') \
        if isinstance(rule_details.get('supplier_storage'), str) else rule_details.get('supplier_storage', [])

    filtered_by_storage = [res for res in result if str(res['supplierCode']) not in black_list]
    count_matches = len(filtered_by_storage)
    filter_key = 'filter_by_storage'
    result_id_rule = product['result'].setdefault('id_rule', {}).setdefault(id_rule, {})
    existing_value = result_id_rule.get(filter_key, "")
    new_value = f"{supplier}: {count_matches}" if supplier else str(count_matches)

    logger.debug(f"Получили {count_matches} результат(ов) по складам для правила {id_rule}")
    result_id_rule[filter_key] = f"{existing_value}, {new_value}" if existing_value else new_value
    return product, filtered_by_storage


def filter_by_white_storage(result: list[dict], id_rule: str, product: dict, supplier: str = '') -> (dict, list[dict]):
    """
    Фильтруем результат по белому списку
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: Данные по процениваемому продукту
    :param supplier: Идентификатор поставщика
    :return: (product, result)
    """
    rule_details = product['id_rule'][id_rule]
    white_list = rule_details.get('supplier_storage', '').replace(' ', '').split(',') \
        if isinstance(rule_details.get('supplier_storage'), str) else rule_details.get('supplier_storage', [])
    filtered_by_white_storage = []
    for storage in white_list:
        if storage == "*":
            filtered_by_white_storage = [res for res in result if str(res['supplierCode']) not in white_list]
        else:
            filtered_by_white_storage = [res for res in result if str(res['supplierCode']) == storage]
        count_matches = len(filtered_by_white_storage)
        filter_key = 'filter_by_storage'
        result_id_rule = product['result'].setdefault('id_rule', {}).setdefault(id_rule, {})
        existing_value = result_id_rule.get(filter_key, "")
        new_value = f"{supplier}: {count_matches}" if supplier else str(count_matches)

        logger.debug(f"Получили {count_matches} результат(ов) по складам для правила {id_rule}")
        result_id_rule[filter_key] = f"{existing_value}, {new_value}" if existing_value else new_value

        product, filtered_by_white_storage = finalize_filters(
            filtered_by_white_storage, id_rule, product, supplier, storage
        )

        # Если позиция найдена, то выходим
        if filtered_by_white_storage:
            break
    return product, filtered_by_white_storage


def finalize_filters(
        result: list[dict], id_rule: str, product: dict, supplier: str = '', storage: str = ''
) -> (dict, list[dict]):
    """

    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: В словаре обязательно наличие ключа ['result':'{id_rule': ...}]
    :param supplier: Идентификатор поставщика
    :param storage: Идентификатор склада
    :return: (product, finalize_filtered)
    """
    logger.debug(f"Применяем общие оставшиеся фильтры")
    # Фильтруем по остаткам
    product, finalize_result = filter_by_criteria(result, id_rule, product, 'min_stock', supplier, storage)
    # Фильтруем по вероятности поставки
    product, finalize_result = filter_by_criteria(
        finalize_result, id_rule, product, 'delivery_probability', supplier, storage
    )
    # Фильтруем по срокам поставки
    product, finalize_result = filter_by_criteria(
        finalize_result, id_rule, product, 'delivery_period', supplier, storage
    )
    # Фильтруем по цене
    product, finalize_result = filter_by_criteria(
        finalize_result, id_rule, product, 'price_deviation', supplier, storage
    )
    product, finalize_result = select_best_offer(finalize_result, id_rule, product, supplier, storage)

    return product, finalize_result


def filter_by_criteria(
        result: list[dict], id_rule: str, product: dict, criteria: str, supplier: str = '', storage: str = ''
) -> (dict, list[dict]):
    """
    Фильтруем по заданному критерию
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: В словаре обязательно наличие ключа ['result':'{id_rule': ...}]
    :param criteria: Критерий фильтрации ('min_stock', 'delivery_probability', 'max_delivery_period', 'price_deviation')
    :param supplier: Идентификатор поставщика
    :param storage: Идентификатор склада
    :return: product, filtered_results
    """
    # Маппинги для ключей в словарях
    product_criteria_keys = {
        'min_stock': 'supplier_storage_min_stock',
        'delivery_probability': 'delivery_probability',
        'delivery_period': 'max_delivery_period',
        'price_deviation': 'price_deviation'
    }
    res_criteria_keys = {
        'min_stock': 'availability',
        'delivery_probability': 'deliveryProbability',
        'delivery_period': 'deliveryPeriod',
        'price_deviation': 'priceIn'
    }

    filtered_results = []
    criteria_key = product_criteria_keys.get(criteria)
    res_criteria_key = res_criteria_keys.get(criteria)
    criteria_value = product['id_rule'][id_rule].get(criteria_key)
    base_price = product.get('price', None)  # Добавляем базовую цену продукта

    for res in result:
        if criteria == 'price_deviation' and criteria_value:
            if base_price:
                calc_deviation = abs(100 - int(res[res_criteria_key]) / base_price * 100)
                check_criteria = calc_deviation <= int(criteria_value)
            else:
                check_criteria = True  # Если base_price равно None, то не проверяем и пропускаем этот результат
        elif not criteria_value:
            check_criteria = True
        else:
            if criteria == 'delivery_period':
                check_criteria = int(res[res_criteria_key]) <= int(criteria_value)
            elif criteria == 'delivery_probability':
                # Если вероятность поставки больше заданного критерия, или равна 0(не указана), то добавляем в результат
                check_criteria = int(res[res_criteria_key]) >= int(criteria_value) or int(res[res_criteria_key]) == 0
            else:
                check_criteria = int(res[res_criteria_key]) >= int(criteria_value)

        if check_criteria:
            filtered_results.append(res)

    count_matches = len(filtered_results)
    filter_key = f'filter_by_{criteria}'
    result_id_rule = product['result'].setdefault('id_rule', {}).setdefault(id_rule, {})
    existing_value = result_id_rule.get(filter_key, "")

    parts = [f"п{supplier}" if supplier else "", f"с{storage}" if storage else ""]
    parts = [part for part in parts if part] # Убираем пустые элементы
    new_value = " - ".join(parts) + (f": {count_matches}" if parts else str(count_matches))

    logger.debug(f"Получили {count_matches} результат(ов) по {criteria} для правила {id_rule}")
    result_id_rule[filter_key] = f"{existing_value}, {new_value}" if existing_value else new_value

    return product, filtered_results


def select_best_offer(
        filtered_res: list[dict], id_rule: str, product: dict, supplier: str = '', storage: str = ''
) -> (dict, list[dict]):
    """
    Выбирает лучшее предложение на основе заданного критерия (цена или срок).
    :param filtered_res: Список отфильтрованных результатов
    :param id_rule: Идентификатор правила
    :param product: В словаре обязательно наличие ключа ['result':'{id_rule': ...}]
    :param supplier: Идентификатор поставщика
    :param storage: Идентификатор склада
    :return: product, filtered_results
    :return: Лучшее предложение
    """
    filtered_results = []
    criteria_value = product['id_rule'][id_rule].get('type_selection_rule')
    selection_criteria = "Цена" if not criteria_value else criteria_value

    if not filtered_res:
        filtered_results = []

    if selection_criteria.lower() == "цена":
        # filtered_results = sorted(filtered_res, key=lambda x: float(x['priceIn']), reverse=False)[:1]
        # Сортируем по цене, а затем по сроку поставки
        filtered_results = sorted(filtered_res, key=lambda x: (float(x['priceIn']), float(x['deliveryPeriod'])))[:1]
    elif selection_criteria.lower() == "срок":
        # filtered_results = sorted(filtered_res, key=lambda x: float(x['deliveryPeriod']), reverse=False)[:1]
        # Сортируем по сроку поставки, а затем по цене
        filtered_results = sorted(filtered_res, key=lambda x: (float(x['deliveryPeriod']), float(x['priceIn'])))[:1]
    else:
        filtered_results = []

    count_matches = len(filtered_results)
    filter_key = f'select_count_product'
    result_id_rule = product['result'].setdefault('id_rule', {}).setdefault(id_rule, {})
    existing_value = result_id_rule.get(filter_key, "")

    parts = [f"п{supplier}" if supplier else "", f"с{storage}" if storage else ""]
    parts = [part for part in parts if part]  # Убираем пустые элементы
    new_value = " - ".join(parts) + (f": {count_matches}" if parts else str(count_matches))

    logger.debug(f"Получили {count_matches} результат(ов) в выборе последней позиции по правилу {id_rule}")
    result_id_rule[filter_key] = f"{existing_value}, {new_value}" if existing_value else new_value

    product['result']['id_rule'][id_rule]['select_product'] = filtered_results

    return product, filtered_results


def filter_by_storage(result: list[dict], id_rule: str, product: dict, supplier: str = '') -> (dict, list[dict]):
    """
    Фильтруем по складам
    :param result: Список результатов проценки
    :param id_rule: Идентификатор правила
    :param product: В словаре обязательно наличие ключа ['result':'{id_rule': ...}]
    :param supplier: Идентификатор поставщика
    :return:
    """
    logger.info("Фильтруем по складам")
    rule_details = product['id_rule'][id_rule]
    supplier_storage = rule_details.get('supplier_storage')
    type_select = rule_details.get('type_select_supplier_storage', False)

    if supplier_storage:
        if type_select:
            logger.debug("Применяем фильтр белого списка по складам")
            product, filtered_by_storage = filter_by_white_storage(result, id_rule, product, supplier)
        else:
            logger.debug("Применяем фильтр черного списка по складам")
            product, filtered_by_storage = filter_by_black_storage(result, id_rule, product, supplier)
            logger.warning(f"Количество после фильтрации по складам: {product['result']['id_rule'][id_rule]}")
            product, filtered_by_storage = finalize_filters(filtered_by_storage, id_rule, product, supplier)
    else:
        logger.debug("Фильтрация по складам не требуется")
        product, filtered_by_storage = pass_filter_by_storage(result, id_rule, product, supplier)
        logger.warning(f"Количество после фильтрации по складам: {product['result']['id_rule'][id_rule]}")
        product, filtered_by_storage = finalize_filters(filtered_by_storage, id_rule, product, supplier)

    return product, filtered_by_storage


def filtered_result(result: list[dict], id_rule: str, product: dict) -> dict:
    """
    Фильтруем результат ответа по позиции согласно заданных правил.

    :param result: Список с результатами от поставщиков.
    :param id_rule: Идентификатор применяемого правила.
    :param product: Данные по продукту для фильтрации включая его правила.
    :return: Возвращаем исходные данные по продукту с добавленными результатами фильтрации
    """
    logger.info(f"Обрабатываем фильтрацию по продукту {product['number']}: {product['brand']}")
    # Обрабатываем правила по белому списку поставщиков
    if product['id_rule'][id_rule]['id_suppliers'] and product['id_rule'][id_rule]['type_select_supplier']:
        product, result = filter_by_white_supplier(result, id_rule, product)

    # Обрабатываем правила по чёрному списку поставщиков
    elif product['id_rule'][id_rule]['id_suppliers'] and not product['id_rule'][id_rule]['type_select_supplier']:
        product, result = filter_by_black_supplier(result, id_rule, product)
        logger.warning(f"Количество после фильтрации по чёрному списку поставщиков: "
                       f"{product['result']['id_rule'][id_rule]}")
        product, result = filter_by_routes(result, id_rule, product)
        logger.warning(f"Количество после фильтрации по маршруту: {product['result']['id_rule'][id_rule]}")
        product, result = filter_by_storage(result, id_rule, product)

    # Игнорируем правило по поставщикам
    else:
        product, result = pass_filter_by_supplier(result, id_rule, product)
        logger.warning(f"Количество после пропуска фильтра поставщикам: {product['result']['id_rule'][id_rule]}")
        product, result = filter_by_routes(result, id_rule, product)
        logger.warning(f"Количество после по маршруту: {product['result']['id_rule'][id_rule]}")
        product, result = filter_by_storage(result, id_rule, product)

    return product


def get_price_supplier2(products: list[dict]) -> list[dict]:
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

                # TODO Сделать проверку поставщиков по списку с учётом возможной "звёздочки" в конце списка
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
                                res['supplierCode']))
                    rule_black_storage = (not product['id_rule'][rule]['type_select_supplier_storage']
                                          and product['id_rule'][rule]['supplier_storage'] not in str(
                                res['supplierCode']))
                    check_storage = rule_white_storage or rule_black_storage

                # Фильтруем позиции по правилу склада
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

                elif selection_rule.lower() == "срок":
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
                    'new_price': rule_result['select_product'][0]['priceIn'],
                    'distributor_result': f"{id_rule_result}; "
                                          f"Поставщик: {rule_result['select_product'][0]['distributorId']}; "
                                          f"Описание маршрута: "
                                          f"{rule_result['select_product'][0]['supplierDescription']}",
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
                    'new_price': '',
                    'distributor_result': 'предложение не найдено см. вкладку ошибки',
                })

    return price_product, err_price_product


def save_error(new_error: list[dict], wk_g: WorkGoogle) -> None:
    """
    Считываем ошибки за последние 7 дней и добавляем новые
    :param wk_g: Класс WorkGoogle для работы с Google таблицей
    :param new_error: Список словарей продуктов с ошибками
    :return: None
    """
    logger.info(f"Считываем ошибки за последние 7 дней и добавляем новые")
    list_error, days_log = wk_g.get_error()
    logger.debug(f"Кол-во ошибок на листе: {len(list_error)=}")
    # list_error = [error for error in list_error if (dt.now() - error['last_update_date']).days <= days_log]
    new_list_error = []
    for error in list_error:
        if (dt.now() - error['last_update_date']).days <= days_log:
            error['last_update_date'] = error['last_update_date'].strftime('%d.%m.%Y')
            new_list_error.append(error)
    logger.debug(f"Количество ошибок после фильтрации по дням: {len(new_list_error)=}")
    new_list_error.extend(new_error)
    wk_g.save_new_result_on_sheet(new_list_error, 2, 4)


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
    rules, own_warehouses = wk_g.get_price_filter_rules()
    # logger.debug(f"{own_warehouses=}")

    # Подставляем правила для отфильтрованных позиций
    products = selected_rule_for_position(products, rules)

    # Получаем цену поставщика согласно правил
    products = get_price_supplier(products, own_warehouses)

    # Выбираем позиции с полученной ценой и без цены
    new_price_product, err_price_product = sort_price_products(products)
    logger.debug(f"{new_price_product=}")
    logger.debug(f"{err_price_product=}")

    # Записываем полученные цены и возвращаем позиции без цены.
    wk_g.set_price_products(new_price_product, count_row, name_column=['E', 'F', 'M'])

    # Записываем в Google таблицу данные с ошибками по количеству выбранных позиций на каждом этапе
    save_error(err_price_product, wk_g)

    logger.info(f"... Окончание работы программы")


if __name__ == "__main__":
    main()
