<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

header('Content-Type: application/json; charset=utf-8');

try {
    $answerId = 2486783577;
    $answer = yandex_forms_fetch_answer($answerId, null);

    echo json_encode([
        'success' => true,
        'answer' => $answer,
        'lead_id_detected' => yandex_forms_extract_lead_id_from_payload($answer),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
}
