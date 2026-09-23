<?php

declare(strict_types=1);

require_once __DIR__ . '/yandex_forms_common.php';

header('Content-Type: application/json; charset=utf-8');

function yandex_forms_probe_request(string $method, string $path, ?array $body = null): array
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

    return [
        'status' => $httpCode,
        'curl_error' => $curlError,
        'body' => $response,
    ];
}

try {
    $surveyId = '6a738889381ea61914181597';

    $result = [
        'hidden_question_candidates' => [],
        'hook_group_candidates' => [],
    ];

    $questionCandidates = [
        [
            'type' => 'string',
            'label' => 'ID сделки',
            'slug' => 'amo_lead_id',
            'hidden' => true,
            'multiline' => false,
        ],
        [
            'type' => 'string',
            'label' => 'ID сделки',
            'slug' => 'amo_lead_id',
            'hide' => true,
            'multiline' => false,
        ],
        [
            'type' => 'string',
            'label' => 'ID сделки',
            'slug' => 'amo_lead_id',
            'hidden' => true,
        ],
    ];

    foreach ($questionCandidates as $candidate) {
        $result['hidden_question_candidates'][] = [
            'payload' => $candidate,
            'response' => yandex_forms_probe_request('POST', '/surveys/' . $surveyId . '/questions/', $candidate),
        ];
    }

    $hookCandidates = [
        [
            'title' => 'amoCRM webhook',
            'active' => true,
        ],
        [
            'name' => 'amoCRM webhook',
            'active' => true,
        ],
        [
            'active' => true,
        ],
        [],
    ];

    foreach ($hookCandidates as $candidate) {
        $result['hook_group_candidates'][] = [
            'payload' => $candidate,
            'response' => yandex_forms_probe_request('POST', '/surveys/' . $surveyId . '/hooks', $candidate),
        ];
    }

    echo json_encode([
        'success' => true,
        'result' => $result,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
} catch (Throwable $e) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
}
