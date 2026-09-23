<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/logger.php';
require_once __DIR__ . '/amo_func.php';

const TELEGRAM_CONSULTATION_FIELD_ID = 1583083;
const TELEGRAM_CONSULTATION_PIPELINE_ID = 8117846;
const TELEGRAM_API_IP = '149.154.167.220';

$source = 'telegram_consultation_webhook.php';
$relayUrl = trim((string) (getenv('TELEGRAM_RELAY_URL') ?: ''));
$relaySecret = trim((string) (getenv('TELEGRAM_RELAY_SECRET') ?: ''));
$chatId = trim((string) (getenv('TELEGRAM_CONSULTATION_CHAT_ID') ?: '-1003492856244'));
$messageThreadId = extractPositiveInt(getenv('TELEGRAM_CONSULTATION_THREAD_ID'));

header('Content-Type: application/json; charset=utf-8');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'error' => 'Используйте POST.'], JSON_UNESCAPED_UNICODE);
    exit;
}

if ($relayUrl === '' || $relaySecret === '') {
    http_response_code(500);
    log_error('Не задан TELEGRAM_BOT_TOKEN', null, $source);
    echo json_encode(['success' => false, 'error' => 'Telegram bot is not configured.']);
    exit;
}

$leadId = extractLeadIdFromWebhook($_POST);
if ($leadId === null) {
    respondOk('Событие без сделки пропущено.');
}

try {
    $lead = get($subdomain, '/api/v4/leads/' . $leadId, $data);
    $pipelineId = (int) ($lead['pipeline_id'] ?? 0);

    if ($pipelineId !== TELEGRAM_CONSULTATION_PIPELINE_ID) {
        log_info('Сделка не из целевой воронки, пропущена', [
            'lead_id' => $leadId,
            'pipeline_id' => $pipelineId,
        ], $source);
        respondOk('Сделка не из целевой воронки.');
    }

    $consultationTimestamp = extractDateTimeField($lead, TELEGRAM_CONSULTATION_FIELD_ID);
    if ($consultationTimestamp === null) {
        log_info('Дата консультации не заполнена, пропущено', ['lead_id' => $leadId], $source);
        respondOk('Дата консультации не заполнена.');
    }

    $stateFile = __DIR__ . '/locks/telegram_consultation_' . $leadId . '.state';
    $eventKey = $leadId . ':' . $consultationTimestamp;
    $previousKey = is_file($stateFile) ? trim((string) file_get_contents($stateFile)) : '';
    if ($previousKey === $eventKey) {
        log_info('Повторное уведомление пропущено', ['lead_id' => $leadId], $source);
        respondOk('Повторное уведомление пропущено.');
    }

    $date = (new DateTimeImmutable('@' . $consultationTimestamp))
        ->setTimezone(new DateTimeZone('Europe/Moscow'));
    $leadName = trim((string) ($lead['name'] ?? ('Сделка #' . $leadId)));
    $amoLink = 'https://' . $subdomain . '.amocrm.ru/leads/detail/' . $leadId;
    $message = "Напоминание о консультации\n"
        . "Дата и время: " . $date->format('d.m.Y H:i') . " (МСК)\n"
        . "Сделка: " . $leadName . "\n"
        . "amoCRM: " . $amoLink;

    $telegramResponse = sendTelegramMessage($relayUrl, $relaySecret, $chatId, $message, $messageThreadId);
    @mkdir(dirname($stateFile), 0755, true);
    @file_put_contents($stateFile, $eventKey, LOCK_EX);

    log_info('Напоминание о консультации отправлено в Telegram', [
        'lead_id' => $leadId,
        'consultation_at' => $date->format(DateTimeInterface::ATOM),
        'chat_id' => $chatId,
        'message_thread_id' => $messageThreadId,
        'telegram_message_id' => $telegramResponse['result']['message_id'] ?? null,
    ], $source);
    respondOk('Напоминание отправлено.');
} catch (Throwable $e) {
    log_error('Ошибка отправки напоминания о консультации', [
        'lead_id' => $leadId,
        'error' => $e->getMessage(),
    ], $source);
    // amoCRM disables webhooks after invalid HTTP responses. Delivery errors are logged
    // without persisting the state, so the same lead can be retried safely.
    respondOk('Событие принято, но уведомление не доставлено.');
}

function extractLeadIdFromWebhook(array $payload): ?int
{
    foreach (['update', 'add', 'status'] as $event) {
        $value = $payload['leads'][$event][0]['id'] ?? null;
        if (is_numeric($value) && (int) $value > 0) {
            return (int) $value;
        }
    }

    return null;
}

function extractDateTimeField(array $lead, int $fieldId): ?int
{
    foreach ($lead['custom_fields_values'] ?? [] as $field) {
        if ((int) ($field['field_id'] ?? 0) !== $fieldId) {
            continue;
        }

        $value = $field['values'][0]['value'] ?? null;
        if (is_numeric($value) && (int) $value > 0) {
            return (int) $value;
        }

        $timestamp = is_string($value) ? strtotime($value) : false;
        return $timestamp !== false ? $timestamp : null;
    }

    return null;
}

function extractPositiveInt($value): ?int
{
    if ($value === false || $value === null) {
        return null;
    }

    $value = trim((string) $value);
    if ($value === '' || !preg_match('/^\d+$/', $value)) {
        return null;
    }

    $intValue = (int) $value;
    return $intValue > 0 ? $intValue : null;
}

function sendTelegramMessage(string $relayUrl, string $relaySecret, string $chatId, string $message, ?int $messageThreadId = null): array
{
    $payload = [
        'chat_id' => $chatId,
        'text' => $message,
        'disable_web_page_preview' => true,
    ];
    if ($messageThreadId !== null) {
        $payload['message_thread_id'] = $messageThreadId;
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
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Telegram cURL: ' . $curlError);
    }

    $decoded = json_decode((string) $response, true);
    if ($httpCode >= 400 || !is_array($decoded) || ($decoded['ok'] ?? false) !== true) {
        throw new RuntimeException('Telegram API HTTP ' . $httpCode . ': ' . substr((string) $response, 0, 500));
    }

    return $decoded;
}

function respondOk(string $message): never
{
    http_response_code(200);
    echo json_encode(['success' => true, 'message' => $message], JSON_UNESCAPED_UNICODE);
    exit;
}
