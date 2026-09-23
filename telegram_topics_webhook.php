<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/logger.php';

const TELEGRAM_TOPICS_API_IP = '149.154.167.220';

$source = 'telegram_topics_webhook.php';
$botToken = trim((string) (getenv('TELEGRAM_BOT_TOKEN') ?: ''));

header('Content-Type: application/json; charset=utf-8');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['success' => false, 'error' => 'Используйте POST.'], JSON_UNESCAPED_UNICODE);
    exit;
}

$rawInput = file_get_contents('php://input');
$update = json_decode((string) $rawInput, true);

if (!is_array($update)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'error' => 'Некорректный JSON.'], JSON_UNESCAPED_UNICODE);
    exit;
}

telegram_topics_log_info('Получено обновление Telegram', [
    'update_id' => $update['update_id'] ?? null,
    'update' => $update,
], $source);

$message = telegram_topics_extract_message($update);
if ($message === null) {
    telegram_topics_log_info('Telegram update без message/channel_post пропущен', [
        'update_id' => $update['update_id'] ?? null,
        'keys' => array_keys($update),
    ], $source);
    telegram_topics_acknowledge(['success' => true, 'message' => 'Update skipped.']);
}

if (!telegram_topics_is_getthemes_command($message)) {
    telegram_topics_log_info('Telegram update без команды /getthemes пропущен', [
        'update_id' => $update['update_id'] ?? null,
        'text' => (string) ($message['text'] ?? ''),
    ], $source);
    telegram_topics_acknowledge(['success' => true, 'message' => 'Command skipped.']);
}

$context = telegram_topics_extract_context($message);

telegram_topics_log_info('Получена команда /getthemes', [
    'update_id' => $update['update_id'] ?? null,
    'chat_id' => $context['chat_id'],
    'chat_type' => $context['chat_type'],
    'chat_title' => $context['chat_title'],
    'message_thread_id' => $context['message_thread_id'],
    'topic_name' => $context['topic_name'],
    'from_id' => $context['from_id'],
    'from_username' => $context['from_username'],
    'text' => $context['text'],
], $source);

telegram_topics_acknowledge([
    'success' => true,
    'chat_id' => $context['chat_id'],
    'message_thread_id' => $context['message_thread_id'],
]);

function telegram_topics_extract_message(array $update): ?array
{
    foreach (['message', 'edited_message', 'channel_post', 'edited_channel_post'] as $key) {
        if (isset($update[$key]) && is_array($update[$key])) {
            return $update[$key];
        }
    }

    return null;
}

function telegram_topics_acknowledge(array $payload): void
{
    http_response_code(200);
    echo json_encode($payload, JSON_UNESCAPED_UNICODE);

    if (function_exists('fastcgi_finish_request')) {
        fastcgi_finish_request();
        return;
    }

    if (ob_get_level() > 0) {
        @ob_end_flush();
    }

    flush();
}

function telegram_topics_log_info(string $message, $data = null, ?string $source = null): void
{
    telegram_topics_log('INFO', $message, $data, $source);
}

function telegram_topics_log_error(string $message, $data = null, ?string $source = null): void
{
    telegram_topics_log('ERROR', $message, $data, $source);
}

function telegram_topics_log(string $level, string $message, $data = null, ?string $source = null): void
{
    $logDir = __DIR__ . '/logs';
    $logFile = $logDir . '/telegramApp.log';

    if (!is_dir($logDir)) {
        @mkdir($logDir, 0755, true);
    }

    if ($source === null) {
        $source = basename(__FILE__);
    }

    $timestamp = date('Y-m-d H:i:s');
    $entry = "[{$timestamp}] [{$level}] [{$source}] {$message}";
    if ($data !== null) {
        $entry .= "\n";
        $entry .= is_string($data)
            ? $data
            : json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
    $entry .= "\n" . str_repeat('-', 80) . "\n";

    @file_put_contents($logFile, $entry, FILE_APPEND | LOCK_EX);
}

function telegram_topics_is_getthemes_command(array $message): bool
{
    $text = trim((string) ($message['text'] ?? ''));
    if ($text === '') {
        return false;
    }

    $command = strtok($text, " \n\r\t");
    if ($command === false) {
        return false;
    }

    $command = ltrim(trim($command), '/');
    $command = strtolower(explode('@', $command)[0]);

    return $command === 'getthemes';
}

function telegram_topics_extract_context(array $message): array
{
    $chat = is_array($message['chat'] ?? null) ? $message['chat'] : [];
    $from = is_array($message['from'] ?? null) ? $message['from'] : [];
    $forumTopicCreated = is_array($message['forum_topic_created'] ?? null) ? $message['forum_topic_created'] : [];

    return [
        'chat_id' => (int) ($chat['id'] ?? 0),
        'chat_type' => trim((string) ($chat['type'] ?? '')),
        'chat_title' => trim((string) ($chat['title'] ?? $chat['username'] ?? '')),
        'message_thread_id' => isset($message['message_thread_id']) ? (int) $message['message_thread_id'] : null,
        'topic_name' => trim((string) ($forumTopicCreated['name'] ?? '')),
        'from_id' => (int) ($from['id'] ?? 0),
        'from_username' => trim((string) ($from['username'] ?? '')),
        'text' => trim((string) ($message['text'] ?? '')),
    ];
}
