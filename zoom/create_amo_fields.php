<?php

declare(strict_types=1);

require_once dirname(__DIR__) . '/config.php';
require_once dirname(__DIR__) . '/amo_func.php';

$subdomain = (string) ($GLOBALS['subdomain'] ?? '');
$accessToken = (string) (($GLOBALS['data'] ?? [])['access_token'] ?? '');
$fieldDefinitions = [
    'Zoom - начало встречи' => ['type' => 'date_time'],
    'Zoom - окончание встречи' => ['type' => 'date_time'],
    'Zoom - Meeting ID' => ['type' => 'text'],
    'Zoom - ссылка на запись' => ['type' => 'url'],
    'Zoom - статус' => ['type' => 'text'],
    'Zoom - ошибка' => ['type' => 'text'],
];

if ($subdomain === '' || $accessToken === '') {
    throw new RuntimeException('Не найден amoCRM subdomain/access token.');
}

$existing = amo_zoom_fields_request($subdomain, $accessToken, 'GET', '/api/v4/leads/custom_fields');
$existingByName = [];
foreach ($existing['_embedded']['custom_fields'] ?? [] as $field) {
    $name = trim((string) ($field['name'] ?? ''));
    if ($name !== '') {
        $existingByName[$name] = (int) ($field['id'] ?? 0);
    }
}

$created = [];
$reused = ['Zoom - ссылка на встречу' => 1609467];
foreach ($fieldDefinitions as $name => $definition) {
    if (isset($existingByName[$name]) && $existingByName[$name] > 0) {
        $reused[$name] = $existingByName[$name];
        continue;
    }

    $response = amo_zoom_fields_request($subdomain, $accessToken, 'POST', '/api/v4/leads/custom_fields', [
        'name' => $name,
        'type' => $definition['type'],
        'sort' => 0,
    ]);
    $fieldId = (int) ($response['id'] ?? 0);
    if ($fieldId <= 0) {
        throw new RuntimeException("amoCRM не вернул ID поля: {$name}");
    }
    $created[$name] = $fieldId;
}

echo json_encode([
    'created' => $created,
    'reused' => $reused,
], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT) . PHP_EOL;

function amo_zoom_fields_request(string $subdomain, string $accessToken, string $method, string $path, ?array $payload = null): array
{
    $ch = curl_init('https://' . $subdomain . '.amocrm.ru' . $path);
    $options = [
        CURLOPT_CUSTOMREQUEST => $method,
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $accessToken,
            'Content-Type: application/json',
        ],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT => 30,
        CURLOPT_SSL_VERIFYPEER => true,
    ];
    if ($payload !== null) {
        $options[CURLOPT_POSTFIELDS] = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    }
    curl_setopt_array($ch, $options);
    $body = curl_exec($ch);
    $error = curl_error($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    if ($error !== '') {
        throw new RuntimeException('amoCRM cURL: ' . $error);
    }
    if ($status < 200 || $status >= 300) {
        throw new RuntimeException('amoCRM HTTP ' . $status . ': ' . substr((string) $body, 0, 1000));
    }
    return json_decode((string) $body, true) ?: [];
}
