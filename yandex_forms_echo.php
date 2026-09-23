<?php

declare(strict_types=1);

$storageDir = __DIR__ . '/data/yandex_forms';
if (!is_dir($storageDir)) {
    mkdir($storageDir, 0775, true);
}

$payload = [
    'time' => date(DATE_ATOM),
    'method' => $_SERVER['REQUEST_METHOD'] ?? '',
    'headers' => function_exists('getallheaders') ? (getallheaders() ?: []) : [],
    'query' => $_GET,
    'raw' => file_get_contents('php://input') ?: '',
    'json' => json_decode(file_get_contents('php://input') ?: '', true),
];

file_put_contents(
    $storageDir . '/last_echo_request.json',
    json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT)
);

header('Content-Type: application/json; charset=utf-8');
echo json_encode([
    'success' => true,
], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
