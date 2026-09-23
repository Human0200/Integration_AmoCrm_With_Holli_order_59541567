<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

header('Content-Type: application/json; charset=utf-8');

function yandex_forms_delete_test_request(string $method, string $path, bool $expectJson = true): array
{
    $config = yandex_forms_config();
    $token = trim((string)($config['token'] ?? ''));
    $orgId = trim((string)($config['org_id'] ?? ''));
    $apiBaseUrl = rtrim((string)($config['api_base_url'] ?? 'https://api.forms.yandex.net/v1'), '/');

    if ($token === '') {
        throw new RuntimeException('Не задан YANDEX_FORMS_TOKEN.');
    }

    $headers = [
        'Authorization: OAuth ' . $token,
        'Accept: application/json',
    ];

    if ($orgId !== '') {
        $headers[] = 'X-Org-Id: ' . $orgId;
    }

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $apiBaseUrl . '/' . ltrim($path, '/'),
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST => strtoupper($method),
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка запроса к API Яндекс Форм: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode >= 300) {
        throw new RuntimeException('API Яндекс Форм вернул HTTP ' . $httpCode . ': ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }

    if (!$expectJson) {
        return [
            'status' => $httpCode,
            'raw' => (string)$response,
        ];
    }

    $decoded = json_decode((string)$response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('API Яндекс Форм вернул некорректный JSON: ' . mb_substr((string)$response, 0, 1000, 'UTF-8'));
    }

    return $decoded;
}

try {
    $response = yandex_forms_delete_test_request('GET', '/surveys');
    $forms = yandex_forms_extract_forms_from_response($response);
    $deleted = [];

    foreach ($forms as $form) {
        if (!is_array($form)) {
            continue;
        }

        $name = trim((string)($form['name'] ?? ''));
        $formId = trim((string)($form['id'] ?? ''));
        if ($name !== 'Тестовая форма amoCRM' || $formId === '') {
            continue;
        }

        $result = yandex_forms_delete_test_request('DELETE', '/surveys/' . rawurlencode($formId) . '/', false);
        $deleted[] = [
            'id' => $formId,
            'status' => (int)($result['status'] ?? 0),
        ];
    }

    echo json_encode([
        'success' => true,
        'deleted' => $deleted,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
}
