<?php

declare(strict_types=1);

require_once __DIR__ . '/zoom_integration.php';

header('Content-Type: application/json; charset=utf-8');
$rawBody = (string) file_get_contents('php://input');
$payload = json_decode($rawBody, true);

if (!is_array($payload)) {
    http_response_code(400);
    echo json_encode(['success' => false, 'error' => 'Некорректный JSON.'], JSON_UNESCAPED_UNICODE);
    exit;
}

$payload['_raw_body'] = $rawBody;
$headers = function_exists('getallheaders') ? getallheaders() : [];

try {
    http_response_code(200);
    echo json_encode(zoom_handle_webhook($payload, $headers), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $exception) {
    zoom_log('ERROR', 'Ошибка Zoom webhook', ['error' => $exception->getMessage()]);
    http_response_code(401);
    echo json_encode(['success' => false, 'error' => 'Webhook rejected.'], JSON_UNESCAPED_UNICODE);
}
