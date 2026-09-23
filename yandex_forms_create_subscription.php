<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

header('Content-Type: application/json; charset=utf-8');

function yandex_forms_subscription_request(string $method, string $path, ?array $body = null, bool $expectJson = true): array
{
    $config = yandex_forms_config();
    $token = trim((string)($config['token'] ?? ''));
    $orgId = trim((string)($config['org_id'] ?? ''));
    $apiBaseUrl = rtrim((string)($config['api_base_url'] ?? 'https://api.forms.yandex.net/v1'), '/');

    $headers = [
        'Authorization: OAuth ' . $token,
        'Accept: application/json',
    ];

    if ($orgId !== '') {
        $headers[] = 'X-Org-Id: ' . $orgId;
    }

    if ($body !== null) {
        $headers[] = 'Content-Type: application/json';
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

    if ($body !== null) {
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($body, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES));
    }

    $response = curl_exec($ch);
    $httpCode = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    curl_close($ch);

    if ($curlError !== '') {
        throw new RuntimeException('Ошибка запроса к API Яндекс Форм: ' . $curlError);
    }

    if ($httpCode < 200 || $httpCode >= 300) {
        throw new RuntimeException('API Яндекс Форм вернул HTTP ' . $httpCode . ': ' . mb_substr((string)$response, 0, 2000, 'UTF-8'));
    }

    if (!$expectJson) {
        return [
            'status' => $httpCode,
            'raw' => (string)$response,
        ];
    }

    $decoded = json_decode((string)$response, true);
    if (!is_array($decoded)) {
        throw new RuntimeException('API Яндекс Форм вернул некорректный JSON: ' . mb_substr((string)$response, 0, 2000, 'UTF-8'));
    }

    return $decoded;
}

try {
    $surveyId = '6a738889381ea61914181597';
    $answerVarId = '507f1f77bcf86cd799439012';
    $hook = yandex_forms_subscription_request('POST', '/surveys/' . $surveyId . '/hooks', [
        'name' => 'amoCRM webhook',
        'active' => true,
    ]);

    $hookId = (int)($hook['id'] ?? 0);
    if ($hookId <= 0) {
        throw new RuntimeException('API не вернул ID группы действий.');
    }

    $subscriptionBody = [
        'type' => 'http',
        'active' => true,
        'name' => 'POST to webhook',
        'url' => 'https://srm.chinatutor.ru/yandex_forms_echo.php',
        'method' => 'post',
        'body' => '{"answer_id":"{' . $answerVarId . '}"}',
        'headers' => [],
        'variables' => [
            [
                'id' => $answerVarId,
                'type' => 'form.answer_id',
            ],
        ],
    ];

    $subscription = yandex_forms_subscription_request(
        'POST',
        '/surveys/' . $surveyId . '/hooks/' . $hookId . '/subscriptions',
        $subscriptionBody
    );

    echo json_encode([
        'success' => true,
        'hook' => $hook,
        'subscription' => $subscription,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
}
