# Эндпоинты интеграции

Базовый URL: `https://srm.chinatutor.ru/`

## Основные (production)

| Эндпоинт | Вход от | Подключает |
|---|---|---|
| `index.php` | amoCRM (leads), OkiDoki (signed), внутренний вызов `?payment_webhook=1` | amoCRM (director) → `add_student.php` → Hollyhop; Zoom (`zoom_integration`); Salesbot reminders; обновляет поля сделки |
| `add_student.php` | `index.php`, `okidoki_hook.php`, POST | Hollyhop API V2 (AddStudent, GetStudents, EditContacts, EditPersonal, EditUserExtraFields…) |
| `add_payment.php` | amoCRM (transactions / catalogs / leads) | amoCRM + Hollyhop (AddPayment / возврат через web-форму) + при необходимости `index.php?payment_webhook=1` |
| `hook.php` | OAuth redirect amoCRM | amoCRM OAuth → пишет `tokens.json` |
| `sourceHook.php` | amoCRM webhook | Salebot (`chatter.salebot.pro`) → поле «Канал» в amoCRM; тик Zoom Salesbot; часть update пробрасывает в `index.php` |
| `okidoki_hook.php` | OkiDoki (подписание договора) | `add_student.php` → Hollyhop |
| `integrationSalebot.php` | Salebot | amoCRM — канал + UTM-поля |
| `amo_coordinators_hook.php` | Hollyhop (смена статуса студента) | Hollyhop → amoCRM support (маршрутизация по координаторам) |
| `cross_portal_closed_status_handler.php` | amoCRM support (закрытие сделки) | amoCRM support → amoCRM director (перенос в «Выбывшие») |

## Яндекс.Формы (production)

| Эндпоинт | Вход от | Подключает |
|---|---|---|
| `yandex_forms_webhook.php` | Яндекс.Формы webhook | Яндекс.Формы API + amoCRM (ссылка на ответ в сделку) |
| `yandex_forms_api.php` | виджет amoCRM | Яндекс.Формы API — список форм + ссылки с `amo_lead_id` |
| `yandex_forms_result.php` | браузер (`?id=…`) | локальный snapshot ответа (HTML) |

## Zoom (production)

| Эндпоинт | Вход от | Подключает |
|---|---|---|
| `zoom/zoom_webhook.php` | Zoom (`recording.completed`) | Zoom API → amoCRM (ссылка на запись) |
| `zoom_salesbot_reminders.php` | cron (`--run`) / из `index` / `sourceHook` | amoCRM Salesbot API (напоминания о встрече) |

## Telegram (production)

| Эндпоинт | Вход от | Подключает |
|---|---|---|
| `telegram_consultation_webhook.php` | amoCRM (воронка консультаций) | amoCRM → Telegram (через relay / Bot API) |
| `telegram_topics_webhook.php` | Telegram Bot webhook | Telegram Bot API (топики/ответы) |
| `telegram_polling.php` | systemd / CLI long-poll | Telegram Bot API |

## Диагностика и утилиты (`diagnostics/`)

### `diagnostics/yandex_forms/`

| Скрипт | Назначение |
|---|---|
| `yandex_forms_echo.php` | сохраняет входящий payload (отладка webhook) |
| `yandex_forms_create_test.php` | создание тестовой формы |
| `yandex_forms_delete_test.php` | удаление тестовых форм |
| `yandex_forms_configure_test.php` | настройка тестовой формы |
| `yandex_forms_create_subscription.php` | создание подписки (webhook) |
| `yandex_forms_list_hooks.php` | список hooks формы |
| `yandex_forms_cleanup_hooks.php` | очистка hooks |
| `yandex_forms_probe_config.php` | проба API-конфига |
| `yandex_forms_debug_surveys.php` | отладка списка опросов |
| `yandex_forms_debug_answer.php` | отладка ответа |
| `debug_yandex_forms_config.php` | проверка наличия токена/конфига |

### `diagnostics/zoom/`

| Скрипт | Назначение |
|---|---|
| `backfill_recording.php` | CLI: дозапись Zoom-записи в сделку по lead_id |
| `create_amo_fields.php` | разовый setup: создание полей Zoom в amoCRM |
| `run_salesbot_now.php` | ручной запуск Salesbot для тестовой сделки |
| `run_zoom_agent_check.php` | ручная проверка Zoom-агента напоминаний |

### `diagnostics/amocrm/`

| Скрипт | Назначение |
|---|---|
| `amo_get_ids.php` | справочник ID полей/воронок (director) |
| `amo_get_ids_support.php` | справочник ID полей/воронок (support) |
| `export_source_closed_statuses.php` | выгрузка закрытых статусов support → JSON |
| `batch_import_source_closed_leads.php` | пакетный перенос закрытых сделок support → director |

## Не эндпоинты (библиотеки)

| Файл | Роль |
|---|---|
| `config.php` | загрузка `.env`, общая конфигурация |
| `logger.php` | единое логирование |
| `amo_func.php` | клиент amoCRM API v4 + OAuth |
| `yandex_forms_common.php` | общая логика Яндекс.Форм |
| `zoom/zoom_integration.php` | логика Zoom ↔ amoCRM |

## Схема связей

```
amoCRM (director) ──► index.php ──► add_student.php ──► Hollyhop
                   └► add_payment.php ──► Hollyhop (+ index?payment_webhook=1)
                   └► sourceHook ──► Salebot + index
                   └► telegram_consultation ──► Telegram
                   └► (из index) Zoom + Salesbot reminders

OkiDoki ──► okidoki_hook / index ──► add_student ──► Hollyhop
Hollyhop ──► amo_coordinators_hook ──► amoCRM (support)
amoCRM (support) ──► cross_portal_* ──► amoCRM (director)
Яндекс.Формы ──► yandex_forms_webhook ──► amoCRM
Zoom ──► zoom/zoom_webhook ──► amoCRM
```
