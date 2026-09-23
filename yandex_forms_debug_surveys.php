<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

header('Content-Type: application/json; charset=utf-8');

try {
    $response = yandex_forms_request('GET', '/surveys');

    echo json_encode([
        'success' => true,
        'response' => $response,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
}
