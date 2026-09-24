<?php

declare(strict_types=1);

require_once dirname(__DIR__) . '/config.php';
require_once dirname(__DIR__) . '/logger.php';
require_once dirname(__DIR__) . '/amo_func.php';

const ZOOM_API_BASE_URL = 'https://api.zoom.us/v2';
const ZOOM_OAUTH_URL = 'https://zoom.us/oauth/token';
const ZOOM_TIMEZONE = 'Europe/Moscow';
const ZOOM_TELEGRAM_PIPELINES = [8117846, 10361250];
const ZOOM_TELEGRAM_API_IP = '149.154.167.220';

function zoom_process_amo_lead(int $leadId, array $lead, string $eventType = 'unknown'): void
{
    $config = zoom_config();
    if (!$config['enabled']) {
        return;
    }

    $interval = zoom_lead_interval($lead, $config);
    if ($interval === null) {
        zoom_log('INFO', 'Zoom: дата начала/окончания не заполнена, пропуск', ['lead_id' => $leadId]);
        return;
    }

    // amoCRM may deliver an update while its API still returns the previous
    // snapshot. Refresh update events before comparing them with the mapping.
    if ($eventType === 'update') {
        $freshInterval = zoom_refresh_lead_interval($leadId, $config, $interval);
        if ($freshInterval !== null && $freshInterval !== $interval) {
            zoom_log('INFO', 'Zoom: использованы актуальные даты после повторного чтения сделки', [
                'lead_id' => $leadId,
                'old_interval' => $interval,
                'fresh_interval' => $freshInterval,
            ]);
            $interval = $freshInterval;
        }
    }

    if ($interval['end'] <= $interval['start']) {
        zoom_log('ERROR', 'Zoom: окончание встречи должно быть позже начала', ['lead_id' => $leadId]);
        zoom_add_amo_note($leadId, 'Zoom: ошибка создания встречи. Окончание встречи должно быть позже начала.');
        return;
    }

    try {
        $mapping = zoom_load_mapping($leadId, $config);
        if (zoom_has_conflict($leadId, $interval, $config)) {
            $message = 'Время Zoom-встречи пересекается с другой встречей, известной системе.';
            zoom_log('ERROR', 'Конфликт времени Zoom-встреч', ['lead_id' => $leadId]);
            zoom_add_amo_note($leadId, 'Zoom: ' . $message);
            return;
        }

        if ($mapping !== null && !empty($mapping['zoom_meeting_id'])
            && empty($mapping['recording_url'])
            && ((int) $mapping['start_at'] !== $interval['start']
                || (int) $mapping['end_at'] !== $interval['end'])) {
            zoom_update_meeting($config, (string) $mapping['zoom_meeting_id'], $interval);
            $mapping['start_at'] = $interval['start'];
            $mapping['end_at'] = $interval['end'];
            $mapping['zoom_status'] = 'updated';
            $mapping['updated_at'] = gmdate('c');
            zoom_save_mapping($leadId, $mapping, $config);
            zoom_update_amo_fields($leadId, $config, $mapping);
            zoom_send_telegram_notification($leadId, $lead, $interval, $mapping, $config);
            zoom_log('INFO', 'Zoom-встреча обновлена', ['lead_id' => $leadId]);
            return;
        }

        if ($mapping !== null
            && (int) ($mapping['start_at'] ?? 0) === $interval['start']
            && (int) ($mapping['end_at'] ?? 0) === $interval['end']) {
            zoom_log('INFO', 'Повторное событие Zoom пропущено по mapping', ['lead_id' => $leadId]);
            return;
        }

        if ($mapping !== null && !empty($mapping['recording_url'])) {
            zoom_archive_mapping($mapping, $config);
        }

        $meeting = zoom_create_meeting($config, $lead, $interval);
        $mapping = [
            'amo_deal_id' => $leadId,
            'amo_contact_id' => $lead['_embedded']['contacts'][0]['id'] ?? null,
            'zoom_meeting_id' => (string) ($meeting['id'] ?? ''),
            'zoom_meeting_uuid' => $meeting['uuid'] ?? null,
            'start_at' => $interval['start'],
            'end_at' => $interval['end'],
            'join_url' => $meeting['join_url'] ?? null,
            'recording_url' => null,
            'zoom_status' => 'scheduled',
            'sync_status' => 'scheduled',
            'last_error' => null,
            'created_at' => gmdate('c'),
            'updated_at' => gmdate('c'),
        ];

        if ($mapping['zoom_meeting_id'] === '') {
            throw new RuntimeException('Zoom API не вернул meeting ID.');
        }

        zoom_save_mapping($leadId, $mapping, $config);
        zoom_update_amo_fields($leadId, $config, $mapping);
        zoom_send_telegram_notification($leadId, $lead, $interval, $mapping, $config);
        zoom_log('INFO', 'Zoom-встреча создана', [
            'lead_id' => $leadId,
            'zoom_meeting_id' => $mapping['zoom_meeting_id'],
            'event_type' => $eventType,
        ]);
    } catch (Throwable $exception) {
        zoom_log('ERROR', 'Ошибка синхронизации Zoom-встречи', [
            'lead_id' => $leadId,
            'error' => $exception->getMessage(),
        ]);
        zoom_add_amo_note($leadId, 'Zoom: ошибка синхронизации. ' . $exception->getMessage());
    }
}

function zoom_refresh_lead_interval(int $leadId, array $config, array $currentInterval): ?array
{
    global $subdomain, $data;

    $latestInterval = null;
    for ($attempt = 1; $attempt <= 3; $attempt++) {
        try {
            $freshLead = get($subdomain, '/api/v4/leads/' . $leadId, $data);
            $interval = zoom_lead_interval($freshLead, $config);
            if ($interval !== null) {
                $latestInterval = $interval;
                if ($interval !== $currentInterval) {
                    return $interval;
                }
            }
        } catch (Throwable $exception) {
            zoom_log('WARNING', 'Zoom: не удалось повторно прочитать сделку', [
                'lead_id' => $leadId,
                'attempt' => $attempt,
                'error' => $exception->getMessage(),
            ]);
        }

        if ($attempt < 3) {
            usleep(700000);
        }
    }

    return $latestInterval;
}

function zoom_send_telegram_notification(int $leadId, array $lead, array $interval, array $mapping, array $config): void
{
    $pipelineId = (int) ($lead['pipeline_id'] ?? 0);
    if (!in_array($pipelineId, ZOOM_TELEGRAM_PIPELINES, true)) {
        return;
    }

    $relayUrl = trim((string) getenv('TELEGRAM_RELAY_URL'));
    $relaySecret = trim((string) getenv('TELEGRAM_RELAY_SECRET'));
    if ($relayUrl === '' || $relaySecret === '') {
        zoom_log('ERROR', 'Telegram relay для Zoom не настроен', ['lead_id' => $leadId]);
        return;
    }

    $statePath = rtrim($config['storage_dir'], '/') . '/telegram_' . $leadId . '.state';
    $eventKey = $leadId . ':' . $interval['start'] . ':' . $interval['end'];
    if (is_file($statePath) && trim((string) file_get_contents($statePath)) === $eventKey) {
        zoom_log('INFO', 'Telegram-уведомление о Zoom пропущено как повтор', ['lead_id' => $leadId]);
        return;
    }

    $date = (new DateTimeImmutable('@' . $interval['start']))->setTimezone(new DateTimeZone(ZOOM_TIMEZONE));
    $leadName = trim((string) ($lead['name'] ?? ('Сделка #' . $leadId)));
    $amoLink = 'https://' . $GLOBALS['subdomain'] . '.amocrm.ru/leads/detail/' . $leadId;
    $message = "Zoom-встреча\n"
        . 'Дата и время: ' . $date->format('d.m.Y H:i') . " (МСК)\n"
        . 'Сделка: ' . $leadName . "\n"
        . 'amoCRM: ' . $amoLink . "\n"
        . 'Zoom: ' . (string) ($mapping['join_url'] ?? '');

    $chatId = trim((string) (getenv('TELEGRAM_CONSULTATION_CHAT_ID') ?: '-1003492856244'));
    $threadId = trim((string) (getenv('TELEGRAM_CONSULTATION_THREAD_ID') ?: '932'));
    $payload = ['chat_id' => $chatId, 'text' => $message, 'disable_web_page_preview' => true];
    if ($threadId !== '' && ctype_digit($threadId) && (int) $threadId > 0) {
        $payload['message_thread_id'] = $threadId;
    }

    $ch = curl_init($relayUrl);
    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Authorization: Bearer ' . $relaySecret,
        ],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 20,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);
    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    $decoded = json_decode((string) $response, true);
    if ($error !== '' || $httpCode >= 400 || !is_array($decoded) || ($decoded['ok'] ?? false) !== true) {
        zoom_log('ERROR', 'Ошибка Telegram-уведомления о Zoom', [
            'lead_id' => $leadId,
            'http_code' => $httpCode,
            'error' => $error !== '' ? $error : substr((string) $response, 0, 500),
        ]);
        return;
    }

    if (!is_dir(dirname($statePath))) {
        @mkdir(dirname($statePath), 0750, true);
    }
    @file_put_contents($statePath, $eventKey, LOCK_EX);
    zoom_log('INFO', 'Telegram-уведомление о Zoom отправлено', [
        'lead_id' => $leadId,
        'pipeline_id' => $pipelineId,
        'message_thread_id' => $payload['message_thread_id'] ?? null,
        'telegram_message_id' => $decoded['result']['message_id'] ?? null,
    ]);
}

function zoom_handle_webhook(array $payload, array $headers = []): array
{
    $config = zoom_config();
    $eventType = (string) ($payload['event'] ?? '');

    if ($eventType === 'endpoint.url_validation') {
        $plainToken = (string) ($payload['payload']['plainToken'] ?? '');
        if ($plainToken === '' || $config['webhook_secret'] === '') {
            throw new RuntimeException('Zoom webhook secret или plainToken не настроен.');
        }

        return [
            'plainToken' => $plainToken,
            'encryptedToken' => hash_hmac('sha256', $plainToken, $config['webhook_secret']),
        ];
    }

    if (!zoom_verify_webhook_signature($payload, $headers, $config['webhook_secret'])) {
        throw new RuntimeException('Невалидная подпись Zoom webhook.');
    }

    if ($eventType !== 'recording.completed') {
        zoom_log('INFO', 'Zoom webhook принят без обработки', ['event' => $eventType]);
        return ['success' => true, 'status' => 'ignored'];
    }

    $object = is_array($payload['payload']['object'] ?? null) ? $payload['payload']['object'] : [];
    $meetingId = (string) ($object['id'] ?? '');
    $meetingUuid = (string) ($object['uuid'] ?? '');
    $eventId = (string) ($payload['id'] ?? ($payload['event_ts'] ?? ''));
    $dedupeKey = hash('sha256', $eventId . '|' . $meetingId . '|' . $meetingUuid);

    if ($eventId !== '' && zoom_event_seen($dedupeKey, $config)) {
        return ['success' => true, 'status' => 'duplicate'];
    }

    $mapping = zoom_find_mapping($meetingId, $meetingUuid, $config);
    if ($mapping === null) {
        zoom_log('INFO', 'Запись Zoom без mapping с amoCRM', [
            'zoom_meeting_id' => $meetingId,
            'zoom_meeting_uuid' => $meetingUuid,
        ]);
        return ['success' => true, 'status' => 'unmapped_zoom_meeting'];
    }

    $recording = zoom_get_recording($config, $meetingId, $meetingUuid);
    $recordingUrls = zoom_recording_urls($recording);
    $recordingUrl = $recordingUrls[0] ?? '';
    if ($recordingUrl === '') {
        throw new RuntimeException('Zoom не вернул ссылку на запись.');
    }

    $mapping['recording_url'] = $recordingUrl;
    $mapping['recording_urls'] = $recordingUrls;
    $mapping['zoom_status'] = 'recorded';
    $mapping['sync_status'] = 'recorded';
    $mapping['updated_at'] = gmdate('c');
    zoom_save_mapping((int) $mapping['amo_deal_id'], $mapping, $config);
    zoom_update_amo_fields((int) $mapping['amo_deal_id'], $config, $mapping);

    if ($eventId !== '') {
        zoom_mark_event($dedupeKey, $config);
    }

    return ['success' => true, 'status' => 'recorded'];
}

function zoom_sync_recording_for_lead(int $leadId, array $config): array
{
    $mapping = zoom_load_mapping($leadId, $config);
    if ($mapping === null || empty($mapping['zoom_meeting_id'])) {
        throw new RuntimeException('Для сделки не найден Zoom mapping.');
    }

    $recording = zoom_get_recording(
        $config,
        (string) $mapping['zoom_meeting_id'],
        (string) ($mapping['zoom_meeting_uuid'] ?? '')
    );
    $recordingUrls = zoom_recording_urls($recording);
    $recordingUrl = $recordingUrls[0] ?? '';
    if ($recordingUrl === '') {
        throw new RuntimeException('Zoom не вернул ссылку на запись.');
    }

    $mapping['recording_url'] = $recordingUrl;
    $mapping['recording_urls'] = $recordingUrls;
    $mapping['zoom_status'] = 'recorded';
    $mapping['sync_status'] = 'recorded';
    $mapping['updated_at'] = gmdate('c');
    zoom_save_mapping($leadId, $mapping, $config);
    zoom_update_amo_fields($leadId, $config, $mapping);

    return [
        'lead_id' => $leadId,
        'zoom_meeting_id' => $mapping['zoom_meeting_id'],
        'recording_url_saved' => true,
    ];
}

function zoom_config(): array
{
    return [
        'enabled' => getenv('ZOOM_ENABLED') === '1',
        'account_id' => trim((string) getenv('ZOOM_ACCOUNT_ID')),
        'client_id' => trim((string) getenv('ZOOM_CLIENT_ID')),
        'client_secret' => trim((string) getenv('ZOOM_CLIENT_SECRET')),
        'host_user' => trim((string) (getenv('ZOOM_HOST_USER') ?: 'support@chinatutor.ru')),
        'webhook_secret' => trim((string) getenv('ZOOM_WEBHOOK_SECRET_TOKEN')),
        'start_field_id' => (int) getenv('ZOOM_START_FIELD_ID'),
        'end_field_id' => (int) getenv('ZOOM_END_FIELD_ID'),
        'join_url_field_id' => (int) getenv('ZOOM_JOIN_URL_FIELD_ID'),
        'meeting_id_field_id' => (int) getenv('ZOOM_MEETING_ID_FIELD_ID'),
        'recording_url_field_id' => (int) getenv('ZOOM_RECORDING_URL_FIELD_ID'),
        'recording_history_field_id' => (int) getenv('ZOOM_RECORDING_HISTORY_FIELD_ID'),
        'storage_dir' => rtrim((string) (getenv('ZOOM_STORAGE_DIR') ?: dirname(__DIR__) . '/data/zoom'), '/'),
    ];
}

function zoom_lead_interval(array $lead, array $config): ?array
{
    $start = zoom_field_timestamp($lead, $config['start_field_id']);
    $end = zoom_field_timestamp($lead, $config['end_field_id']);
    return $start !== null && $end !== null ? ['start' => $start, 'end' => $end] : null;
}

function zoom_field_timestamp(array $lead, int $fieldId): ?int
{
    if ($fieldId <= 0) {
        return null;
    }
    foreach ($lead['custom_fields_values'] ?? [] as $field) {
        if ((int) ($field['field_id'] ?? 0) !== $fieldId) {
            continue;
        }
        $value = $field['values'][0]['value'] ?? null;
        if (is_numeric($value) && (int) $value > 0) {
            return (int) $value;
        }
        if (is_string($value) && $value !== '') {
            $parsed = strtotime($value);
            return $parsed === false ? null : $parsed;
        }
    }
    return null;
}

function zoom_create_meeting(array $config, array $lead, array $interval): array
{
    return zoom_api_request($config, 'POST', '/users/' . rawurlencode($config['host_user']) . '/meetings', [
        'topic' => trim((string) ($lead['name'] ?? 'amoCRM consultation')),
        'type' => 2,
        'start_time' => gmdate('Y-m-d\TH:i:s\Z', $interval['start']),
        'duration' => max(1, (int) ceil(($interval['end'] - $interval['start']) / 60)),
        'timezone' => ZOOM_TIMEZONE,
        'settings' => ['auto_recording' => 'cloud'],
    ]);
}

function zoom_update_meeting(array $config, string $meetingId, array $interval): array
{
    return zoom_api_request($config, 'PATCH', '/meetings/' . rawurlencode($meetingId), [
        'start_time' => gmdate('Y-m-d\TH:i:s\Z', $interval['start']),
        'duration' => max(1, (int) ceil(($interval['end'] - $interval['start']) / 60)),
        'timezone' => ZOOM_TIMEZONE,
        'settings' => ['auto_recording' => 'cloud'],
    ]);
}

function zoom_get_recording(array $config, string $meetingId, string $meetingUuid): array
{
    // Для scheduled-встреч (type=2) список записей одинаков для id и uuid,
    // а id не содержит спецсимволов — запрашиваем по нему в первую очередь.
    if ($meetingId !== '') {
        try {
            return zoom_api_request($config, 'GET', '/meetings/' . rawurlencode($meetingId) . '/recordings');
        } catch (Throwable $exception) {
            if ($meetingUuid === '') {
                throw $exception;
            }
        }
    }

    if ($meetingUuid === '') {
        throw new RuntimeException('Zoom: не указан ни meeting id, ни uuid для получения записи.');
    }

    // Zoom требует двойного URL-кодирования uuid, начинающегося с "/"
    // или содержащего "//", иначе uuid ломает путь запроса (ошибка 3301
    // "This recording does not exist").
    $identifier = ($meetingUuid[0] === '/' || str_contains($meetingUuid, '//'))
        ? rawurlencode(rawurlencode($meetingUuid))
        : rawurlencode($meetingUuid);

    return zoom_api_request($config, 'GET', '/meetings/' . $identifier . '/recordings');
}

function zoom_recording_url(array $recording): string
{
    return zoom_recording_urls($recording)[0] ?? '';
}

function zoom_recording_urls(array $recording): array
{
    $urls = [];
    foreach ($recording['recording_files'] ?? [] as $file) {
        if (!is_array($file)) {
            continue;
        }
        foreach ([$file['play_url'] ?? null, $file['download_url'] ?? null] as $url) {
            $url = is_string($url) ? trim($url) : '';
            if ($url !== '' && !in_array($url, $urls, true)) {
                $urls[] = $url;
                break;
            }
        }
    }

    if ($urls === [] && is_string($recording['share_url'] ?? null) && trim($recording['share_url']) !== '') {
        $urls[] = trim($recording['share_url']);
    }

    return $urls;
}

function zoom_api_request(array $config, string $method, string $path, ?array $payload = null): array
{
    $token = zoom_access_token($config);
    $ch = curl_init(ZOOM_API_BASE_URL . $path);
    $options = [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => $method,
        CURLOPT_HTTPHEADER => ['Authorization: Bearer ' . $token, 'Content-Type: application/json'],
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_SSL_VERIFYPEER => true,
    ];
    if ($payload !== null) {
        $options[CURLOPT_POSTFIELDS] = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
    curl_setopt_array($ch, $options);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    if ($error !== '') {
        throw new RuntimeException('Zoom cURL: ' . $error);
    }
    $decoded = json_decode((string) $body, true);
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException('Zoom API HTTP ' . $status . ': ' . substr((string) $body, 0, 500));
    }
    return is_array($decoded) ? $decoded : [];
}

function zoom_access_token(array $config): string
{
    if ($config['account_id'] === '' || $config['client_id'] === '' || $config['client_secret'] === '') {
        throw new RuntimeException('Zoom Server-to-Server OAuth не настроен.');
    }
    $ch = curl_init(ZOOM_OAUTH_URL . '?grant_type=account_credentials&account_id=' . rawurlencode($config['account_id']));
    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_USERPWD => $config['client_id'] . ':' . $config['client_secret'],
        CURLOPT_HTTPAUTH => CURLAUTH_BASIC,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 20,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    $decoded = json_decode((string) $body, true);
    if ($error !== '' || $status < 200 || $status >= 300 || !is_array($decoded) || empty($decoded['access_token'])) {
        throw new RuntimeException('Zoom OAuth HTTP ' . $status . ': token не получен.');
    }
    return (string) $decoded['access_token'];
}

function zoom_verify_webhook_signature(array $payload, array $headers, string $secret): bool
{
    $timestamp = zoom_header($headers, 'x-zm-request-timestamp');
    $signature = zoom_header($headers, 'x-zm-signature');
    if ($secret === '' || $timestamp === '' || $signature === '') {
        return false;
    }
    $rawBody = (string) ($payload['_raw_body'] ?? json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    $expected = 'v0=' . hash_hmac('sha256', 'v0:' . $timestamp . ':' . $rawBody, $secret);
    return hash_equals($expected, $signature);
}

function zoom_header(array $headers, string $name): string
{
    foreach ($headers as $key => $value) {
        if (strtolower((string) $key) === strtolower($name)) {
            return trim((string) $value);
        }
    }
    return '';
}

function zoom_mapping_path(int $leadId, array $config): string
{
    return $config['storage_dir'] . '/lead_' . $leadId . '.json';
}

function zoom_load_mapping(int $leadId, array $config): ?array
{
    $path = zoom_mapping_path($leadId, $config);
    if (!is_file($path)) {
        return null;
    }
    $mapping = json_decode((string) file_get_contents($path), true);
    return is_array($mapping) ? $mapping : null;
}

function zoom_save_mapping(int $leadId, array $mapping, array $config): void
{
    if (!is_dir($config['storage_dir'])) {
        @mkdir($config['storage_dir'], 0750, true);
    }
    file_put_contents(
        zoom_mapping_path($leadId, $config),
        json_encode($mapping, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT),
        LOCK_EX
    );
}

function zoom_archive_mapping(array $mapping, array $config): void
{
    if (!is_dir($config['storage_dir'])) {
        @mkdir($config['storage_dir'], 0750, true);
    }
    $historyPath = $config['storage_dir'] . '/history.json';
    $history = is_file($historyPath) ? json_decode((string) file_get_contents($historyPath), true) : [];
    if (!is_array($history)) {
        $history = [];
    }
    $history[] = $mapping;
    file_put_contents($historyPath, json_encode($history, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT), LOCK_EX);
}

function zoom_has_conflict(int $leadId, array $interval, array $config): bool
{
    foreach (glob($config['storage_dir'] . '/lead_*.json') ?: [] as $path) {
        $mapping = json_decode((string) file_get_contents($path), true);
        if (!is_array($mapping) || (int) ($mapping['amo_deal_id'] ?? 0) === $leadId) {
            continue;
        }
        if (!in_array((string) ($mapping['zoom_status'] ?? ''), ['scheduled', 'updated'], true)) {
            continue;
        }
        $existingStart = (int) ($mapping['start_at'] ?? 0);
        $existingEnd = (int) ($mapping['end_at'] ?? 0);
        if ($existingStart > 0 && $existingEnd > $existingStart
            && $interval['start'] < $existingEnd
            && $interval['end'] > $existingStart) {
            return true;
        }
    }
    return false;
}

function zoom_find_mapping(string $meetingId, string $meetingUuid, array $config): ?array
{
    foreach (glob($config['storage_dir'] . '/lead_*.json') ?: [] as $path) {
        $mapping = json_decode((string) file_get_contents($path), true);
        if (!is_array($mapping)) {
            continue;
        }
        if (($meetingId !== '' && (string) ($mapping['zoom_meeting_id'] ?? '') === $meetingId)
            || ($meetingUuid !== '' && (string) ($mapping['zoom_meeting_uuid'] ?? '') === $meetingUuid)) {
            return $mapping;
        }
    }
    return null;
}

function zoom_event_seen(string $key, array $config): bool
{
    $path = $config['storage_dir'] . '/events.json';
    $events = is_file($path) ? json_decode((string) file_get_contents($path), true) : [];
    return is_array($events) && !empty($events[$key]);
}

function zoom_mark_event(string $key, array $config): void
{
    if (!is_dir($config['storage_dir'])) {
        @mkdir($config['storage_dir'], 0750, true);
    }
    $path = $config['storage_dir'] . '/events.json';
    $events = is_file($path) ? json_decode((string) file_get_contents($path), true) : [];
    if (!is_array($events)) {
        $events = [];
    }
    $events[$key] = gmdate('c');
    file_put_contents($path, json_encode($events, JSON_UNESCAPED_SLASHES), LOCK_EX);
}

function zoom_update_amo_fields(int $leadId, array $config, array $mapping): void
{
    $fields = [];
    zoom_add_amo_field($fields, $config['join_url_field_id'], $mapping['join_url'] ?? null);
    zoom_add_amo_field($fields, $config['meeting_id_field_id'], $mapping['zoom_meeting_id'] ?? null);
    zoom_add_amo_field($fields, $config['recording_url_field_id'], $mapping['recording_url'] ?? null);
    $recordingUrls = is_array($mapping['recording_urls'] ?? null)
        ? $mapping['recording_urls']
        : (!empty($mapping['recording_url']) ? [(string) $mapping['recording_url']] : []);
    if ($recordingUrls !== [] && $config['recording_history_field_id'] > 0) {
        $history = zoom_recording_history($leadId, $config, $recordingUrls);
        zoom_add_amo_field($fields, $config['recording_history_field_id'], $history);
    }
    if ($fields === []) {
        return;
    }
    global $subdomain, $data;
    amo_zoom_request($subdomain, (string) ($data['access_token'] ?? ''), 'PATCH', '/api/v4/leads/' . $leadId, [
        'custom_fields_values' => array_values($fields),
    ]);
}

function zoom_recording_history(int $leadId, array $config, array $recordingUrls): string
{
    global $subdomain, $data;
    $history = [];
    try {
        $lead = amo_zoom_request($subdomain, (string) ($data['access_token'] ?? ''), 'GET', '/api/v4/leads/' . $leadId, []);
        foreach ($lead['custom_fields_values'] ?? [] as $field) {
            if ((int) ($field['field_id'] ?? 0) !== $config['recording_history_field_id']) {
                continue;
            }
            $value = (string) ($field['values'][0]['value'] ?? '');
            $history = array_values(array_filter(array_map('trim', preg_split('/\R+/', $value) ?: [])));
        }
    } catch (Throwable $exception) {
        zoom_log('WARNING', 'Не удалось прочитать историю записей Zoom', ['lead_id' => $leadId, 'error' => $exception->getMessage()]);
    }
    foreach ($recordingUrls as $recordingUrl) {
        $recordingUrl = trim((string) $recordingUrl);
        if ($recordingUrl !== '' && !in_array($recordingUrl, $history, true)) {
            $history[] = $recordingUrl;
        }
    }
    return implode("\n", $history);
}

function zoom_add_amo_field(array &$fields, int $fieldId, ?string $value): void
{
    if ($fieldId <= 0 || $value === null || $value === '') {
        return;
    }
    $fields[] = ['field_id' => $fieldId, 'values' => [['value' => $value]]];
}

function zoom_add_amo_note(int $leadId, string $text): void
{
    global $subdomain, $data;

    try {
        amo_zoom_request(
            $subdomain,
            (string) ($data['access_token'] ?? ''),
            'POST',
            '/api/v4/leads/' . $leadId . '/notes',
            [[
                'note_type' => 'common',
                'params' => ['text' => $text],
            ]]
        );
    } catch (Throwable $exception) {
        zoom_log('ERROR', 'Не удалось добавить ошибку Zoom в историю сделки', [
            'lead_id' => $leadId,
            'error' => $exception->getMessage(),
        ]);
    }
}

function amo_zoom_request(string $subdomain, string $accessToken, string $method, string $path, array $payload): array
{
    if ($accessToken === '') {
        throw new RuntimeException('amoCRM access token не настроен.');
    }
    $ch = curl_init('https://' . $subdomain . '.amocrm.ru' . $path);
    curl_setopt_array($ch, [
        CURLOPT_CUSTOMREQUEST => $method,
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_HTTPHEADER => ['Authorization: Bearer ' . $accessToken, 'Content-Type: application/json'],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);
    if ($error !== '') {
        throw new RuntimeException('amoCRM cURL: ' . $error);
    }
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException('amoCRM HTTP ' . $status . ': ' . substr((string) $body, 0, 500));
    }
    return json_decode((string) $body, true) ?: [];
}

function zoom_log(string $level, string $message, array $data = []): void
{
    if (function_exists('log_message')) {
        log_message($message, $data, $level, 'zoom_integration.php');
    }
}
