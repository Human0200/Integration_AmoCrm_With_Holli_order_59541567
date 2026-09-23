<?php

declare(strict_types=1);

const TELEGRAM_POLLING_API_IP = '149.154.167.220';

$token = trim((string) getenv('TELEGRAM_BOT_TOKEN'));
$logFile = getenv('TELEGRAM_POLLING_LOG') ?: __DIR__ . '/logs/telegramApp.log';
$stateFile = getenv('TELEGRAM_POLLING_STATE') ?: __DIR__ . '/telegram_polling.offset';

if ($token === '') {
    fwrite(STDERR, "TELEGRAM_BOT_TOKEN is not configured.\n");
    exit(1);
}

@mkdir(dirname($logFile), 0755, true);
$offset = is_file($stateFile) ? (int) trim((string) file_get_contents($stateFile)) : 0;

pollingLog($logFile, 'INFO', 'Telegram polling started', ['offset' => $offset]);

while (true) {
    $response = telegramPollingRequest($token, 'getUpdates', [
        'offset' => $offset,
        'timeout' => 25,
        'allowed_updates' => json_encode([
            'message',
            'edited_message',
            'channel_post',
            'edited_channel_post',
            'my_chat_member',
        ], JSON_UNESCAPED_UNICODE),
    ]);

    if ($response === null) {
        sleep(3);
        continue;
    }

    foreach ($response as $update) {
        $updateId = (int) ($update['update_id'] ?? 0);
        if ($updateId > 0) {
            $offset = $updateId + 1;
            @file_put_contents($stateFile, (string) $offset, LOCK_EX);
        }

        pollingLog($logFile, 'INFO', 'Получено обновление Telegram', $update);

        $message = extractPollingMessage($update);
        if ($message !== null && isGetThemesCommand($message)) {
            pollingLog($logFile, 'INFO', 'Получена команда /getthemes', extractPollingContext($message));
        }
    }
}

function telegramPollingRequest(string $token, string $method, array $params): ?array
{
    $ch = curl_init('https://api.telegram.org/bot' . rawurlencode($token) . '/' . $method);
    curl_setopt_array($ch, [
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => http_build_query($params),
        CURLOPT_HTTPHEADER => ['Content-Type: application/x-www-form-urlencoded'],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 40,
        CURLOPT_NOPROXY => '*',
        CURLOPT_IPRESOLVE => CURL_IPRESOLVE_V4,
        CURLOPT_RESOLVE => ['api.telegram.org:443:' . TELEGRAM_POLLING_API_IP],
        CURLOPT_SSL_VERIFYPEER => true,
    ]);

    $body = curl_exec($ch);
    $error = curl_error($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    if ($error !== '') {
        pollingLog(getenv('TELEGRAM_POLLING_LOG') ?: __DIR__ . '/logs/telegramApp.log', 'ERROR', 'Telegram cURL error', [
            'error' => $error,
        ]);
        return null;
    }

    $decoded = json_decode((string) $body, true);
    if ($httpCode >= 400 || !is_array($decoded) || ($decoded['ok'] ?? false) !== true) {
        pollingLog(getenv('TELEGRAM_POLLING_LOG') ?: __DIR__ . '/logs/telegramApp.log', 'ERROR', 'Telegram API error', [
            'http_code' => $httpCode,
            'response' => substr((string) $body, 0, 1000),
        ]);
        sleep(3);
        return null;
    }

    return is_array($decoded['result'] ?? null) ? $decoded['result'] : [];
}

function extractPollingMessage(array $update): ?array
{
    foreach (['message', 'edited_message', 'channel_post', 'edited_channel_post'] as $key) {
        if (isset($update[$key]) && is_array($update[$key])) {
            return $update[$key];
        }
    }

    return null;
}

function isGetThemesCommand(array $message): bool
{
    $text = trim((string) ($message['text'] ?? ''));
    $command = strtok($text, " \n\r\t");
    if ($command === false) {
        return false;
    }

    return strtolower(explode('@', ltrim($command, '/'))[0]) === 'getthemes';
}

function extractPollingContext(array $message): array
{
    $chat = is_array($message['chat'] ?? null) ? $message['chat'] : [];
    $from = is_array($message['from'] ?? null) ? $message['from'] : [];
    $topic = is_array($message['forum_topic_created'] ?? null) ? $message['forum_topic_created'] : [];

    return [
        'chat_id' => $chat['id'] ?? null,
        'chat_title' => $chat['title'] ?? $chat['username'] ?? null,
        'chat_type' => $chat['type'] ?? null,
        'message_thread_id' => $message['message_thread_id'] ?? null,
        'topic_name' => $topic['name'] ?? null,
        'from_id' => $from['id'] ?? null,
        'from_username' => $from['username'] ?? null,
        'text' => $message['text'] ?? '',
    ];
}

function pollingLog(string $file, string $level, string $message, $data = null): void
{
    $entry = '[' . date('Y-m-d H:i:s') . '] [' . $level . '] ' . $message;
    if ($data !== null) {
        $entry .= "\n" . json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
    $entry .= "\n" . str_repeat('-', 80) . "\n";
    @file_put_contents($file, $entry, FILE_APPEND | LOCK_EX);
}
