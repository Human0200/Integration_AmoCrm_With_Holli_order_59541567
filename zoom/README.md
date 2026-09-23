# Интеграция amoCRM и Zoom

Все файлы этой интеграции находятся в папке `zoom/`.

## Что уже реализовано

- amoCRM webhook вызывает Zoom-синхронизацию из `index.php`.
- При заполненных полях начала и окончания создается или обновляется Zoom Meeting.
- При создании передается `settings.auto_recording = cloud`.
- Связь сделки и встречи хранится в `data/zoom/lead_<lead_id>.json`.
- Повторный одинаковый webhook не создает вторую встречу.
- Известные пересечения времени блокируются со статусом `conflict`.
- История завершенных встреч сохраняется в `data/zoom/history.json`.
- `recording.completed` обрабатывается endpoint `zoom/zoom_webhook.php`.
- Встречи без mapping с amoCRM принимаются без изменения сделок.

## Поля amoCRM

Нужно создать отдельные поля сделки, не используя существующие поля пробного урока:

| Назначение | Тип | Переменная |
|---|---|---|
| Zoom - начало встречи | Дата и время | `ZOOM_START_FIELD_ID` |
| Zoom - окончание встречи | Дата и время | `ZOOM_END_FIELD_ID` |
| Zoom - ссылка на встречу | Ссылка или текст | `ZOOM_JOIN_URL_FIELD_ID` |
| Zoom - Meeting ID | Текст | `ZOOM_MEETING_ID_FIELD_ID` |
| Zoom - ссылка на запись | Ссылка или текст | `ZOOM_RECORDING_URL_FIELD_ID` |
Ошибки и конфликты записываются заметкой в историю сделки amoCRM.

После создания полей нужно вписать их числовые ID в `.env`.

## Переменные `.env`

```dotenv
ZOOM_ENABLED=1
ZOOM_ACCOUNT_ID=...
ZOOM_CLIENT_ID=...
ZOOM_CLIENT_SECRET=...
ZOOM_HOST_USER=support@chinatutor.ru
ZOOM_WEBHOOK_SECRET_TOKEN=...
ZOOM_START_FIELD_ID=...
ZOOM_END_FIELD_ID=...
ZOOM_JOIN_URL_FIELD_ID=...
ZOOM_MEETING_ID_FIELD_ID=...
ZOOM_RECORDING_URL_FIELD_ID=...
ZOOM_STORAGE_DIR=/var/www/evrasia/data/www/srm.chinatutor.ru/data/zoom
```

Секреты не должны попадать в Git, frontend или логи.

## Zoom scopes

Для текущего кода нужны:

- `meeting:write:meeting:admin` - создание и обновление встреч;
- `cloud_recording:read:recording:admin` - получение данных записи;
- `cloud_recording:read:list_recording_files:admin` - получение файлов записи и play URL.

Для приема `recording.completed` отдельный API scope не нужен, но требуется настроенный Event Subscription и Secret Token.

## Webhook Zoom

Публичный endpoint:

```text
https://srm.chinatutor.ru/zoom/zoom_webhook.php
```

В Zoom Marketplace нужно добавить Event Subscription с событием:

```text
recording.completed
```

Также нужно включить URL validation и перенести Secret Token в `ZOOM_WEBHOOK_SECRET_TOKEN`.

## Timezone

Внутри mapping timestamps хранятся в UTC. Поля amoCRM с Unix timestamp интерпретируются как timestamp, а встреча передается в Zoom в UTC с timezone `Europe/Moscow`. Длительность рассчитывается из разницы `end - start`.
