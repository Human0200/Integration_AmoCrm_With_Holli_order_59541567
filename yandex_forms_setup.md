# Интеграция Яндекс.Форм и amoCRM

## Что добавлено

- [yandex_forms_api.php](./yandex_forms_api.php) — API для amo-виджета, получает список форм из Яндекс.Форм и возвращает готовые ссылки с `amo_lead_id`.
- [yandex_forms_webhook.php](./yandex_forms_webhook.php) — webhook-обработчик ответов формы.
- [yandex_forms_result.php](./yandex_forms_result.php) — публичная страница с результатом анкеты.
- [yandex_forms_common.php](./yandex_forms_common.php) — общий helper для API Яндекс.Форм и обновления amoCRM.
- [widget/yandex_forms_widget](./widget/yandex_forms_widget) — исходники private widget для карточки сделки.

## Что нужно настроить в `.env`

Добавьте переменные:

```env
YANDEX_FORMS_TOKEN=oauth_token_iz_yandex_forms
YANDEX_FORMS_ORG_ID=id_organizacii_esli_ispolzuetsya
YANDEX_FORMS_PUBLIC_BASE_URL=https://srm.chinatutor.ru
YANDEX_FORMS_WIDGET_SERVER_URL=https://srm.chinatutor.ru/yandex_forms_api.php
YANDEX_FORMS_RESULTS_FIELD_ID=ID_edinogo_polya_v_amocrm
YANDEX_FORMS_LEAD_PARAM_NAME=amo_lead_id
```

## Как настраивать каждую форму

### 1. Скрытое поле с ID сделки

В каждой форме нужно создать скрытый вопрос типа `Короткий текст`:

- Название: любое
- `Скрытый вопрос`: включить
- `Идентификатор вопроса`: `amo_lead_id`

Именно в этот параметр виджет будет подставлять ID сделки.

### 2. Webhook формы

В интеграциях формы нужно указать URL:

```text
https://srm.chinatutor.ru/yandex_forms_webhook.php
```

Правильный вариант по документации Яндекс.Форм:

- тип интеграции: `Запрос заданным методом`
- HTTP-метод: `POST`
- тело запроса: `JSON`

В тело запроса нужно добавить поля через переменные Яндекс.Форм:

- `answer_id` → переменная `form.answer_id`
- `answer_url` → переменная `form.answer_url`
- `survey_id` → переменная `form.id`
- `survey_name` → переменная `form.name`
- `query_params` → переменная `request.query_params` с фильтром `json`
- `answers_json` → переменная `form.questions_answers_json` с фильтром `json`

Итоговая структура тела должна быть такой:

```json
{
  "answer_id": 123456,
  "answer_url": "https://forms.yandex.ru/admin/answers/...",
  "survey_id": "66f1b7f7eb6146f1b7f7eb61",
  "survey_name": "Анкета клиента",
  "query_params": {
    "amo_lead_id": "29505455"
  },
  "answers_json": {
    "amo_lead_id": {
      "value": "29505455"
    }
  }
}
```

`answer_id` или `answer_key` обязателен: сервер получает детальный ответ формы через API `GET /v1/answers`.

### 3. Важные условия Яндекс.Форм

- HTTP-интеграция Яндекс.Форм работает только по `IPv6`.
- На один ответ может прийти два одинаковых HTTP-запроса.
- Для дедупликации сервер использует заголовок `x-delivery-id`.

## Как это работает

1. Менеджер в карточке сделки выбирает форму в виджете.
2. Виджет получает формы с сервера и формирует ссылку вида:

```text
https://forms.yandex.ru/u/<form_id>/?amo_lead_id=<lead_id>
```

3. Клиент заполняет форму.
4. Яндекс.Формы отправляют webhook в `yandex_forms_webhook.php`.
5. Сервер получает ответ, строит публичную страницу результата и записывает ссылку в единое поле amoCRM.

## Виджет

После сборки нужно загрузить zip-архив private widget в amoCRM.

Исходники лежат в:

- [widget/yandex_forms_widget](./widget/yandex_forms_widget)

Сборка архива:

```bash
./build_yandex_forms_widget.sh
```
