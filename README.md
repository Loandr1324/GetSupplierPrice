# GetSupplierPrice
Получение цены от поставщика по API ABCP

### Описание

------------
Данный скрипт берёт позиции товаров из Google таблицы, берёт правила выбора цены позиции.
Получает по API ABCP предложения от поставщиков. По правилам выбирает одно предложение.
Выбранное предложение с ценой помещает в Google таблицу

Позиции товаров расположены в Google таблице1, состоящей из 11 колонок с данными:

`Каталожный номер`,	`Бренд`, `Описание`, `Наличие`, `Цена`, `дата проценки`, `коэф оборачиваемости`,
`норма наличия`, `правило проценки товара`, `отбор для проценки`, `ID выбранное правило проценки`

Правила выбора предложения от поставщика находятся в Google таблице2 из 14 колонок:

`ID правила`, `тип правила`, `бренд`, `тип отбора поставщиков`, `поставщики`, `тип отбора маршрута`, 
`маршруты`, `тип отбора складов`, `склады`, `минимальный остаток`, `вероятность`, 
`Допустимый срок, дней`, `Цена или срок`, `отклонение цены, %`

Для работы с google таблицей требуется создать сервисный аккаунт google.
[Инструкция по созданию google аккаунта](https://dvsemenov.ru/google-tablicy-i-python-podrobnoe-rukovodstvo-s-primerami/)

### Доступы

------------
Все данные для доступа к API платформы ABCP находятся на странице в вашей ПУ:

[Данные для доступа к API](https://cp.abcp.ru/?page=allsettings&systemsettings&apiInformation)

Данные для доступа скрипта размещаем в файл config.py:  
```python
AUTH_API: dict = {
    'HOST_API': 'abcpxxxxx.public.api.abcp.ru - ваш хост со страницы доступа к API',
    'USER_API': 'api@idxxxxx - ваш логин страницы доступа к API',
    'PASSWORD_API': 'xxxxxxxxxxxx - ваш MD5-пароль страницы доступа к API'
}

AUTH_GOOGLE: dict = {
    'GOOGLE_CLIENT_ID': 'ваш google клиент id',
    'GOOGLE_CLIENT_SECRET': 'ваш google ',
    'KEY_WORKBOOK': 'id вашей google таблицы'
}
FILE_NAME_LOG: str = 'имя вашего лог файла'
```
Так же в папке проекта [services/google_table](services/google_table) необходимо расположить файл 
`credentials.json` с параметрами подключения к Google таблице
Подробная инструкция по настройке работы с Google таблицами по 
[ссылке](https://dvsemenov.ru/google-tablicy-i-python-podrobnoe-rukovodstvo-s-primerami/)
Содержимое файла:
```python
{
  "type": "service_account",
  "project_id": "ваше наименование проeкта",
  "private_key_id": "ваш id",
  "private_key": "ваш ключ",
  "client_email": "email вашего сервиса, который добавляется как редактор к google таблице",
  "client_id": "id вашего сервиса",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "url сертификата вашего сервиса",
  "universe_domain": "googleapis.com"
}
```

### Примечание 

------------
Для логирования используется библиотека [logguru](https://loguru.readthedocs.io/en/stable/overview.html)
Наименование лог файла прописывается в файле config.py в переменную `FILE_NAME_LOG`