<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';

header('Content-Type: text/plain; charset=utf-8');

function callHolly(string $method, array $params): array
{
    $apiConfig = get_config('api');

    $url = rtrim((string) $apiConfig['base_url'], '/') . '/' . ltrim($method, '/');
    $params['authkey'] = (string) $apiConfig['auth_key'];

    $json = json_encode($params, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $json,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Content-Length: ' . strlen($json),
        ],
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT => 60,
        CURLOPT_CONNECTTIMEOUT => 15,
        CURLOPT_SSL_VERIFYPEER => true,
    ]);

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);

    if ($error !== '') {
        throw new RuntimeException('cURL error: ' . $error);
    }

    echo "METHOD: {$method}\n";
    echo "HTTP: {$httpCode}\n";
    echo "RAW: {$response}\n\n";

    $decoded = json_decode((string) $response, true);
    return is_array($decoded) ? $decoded : [];
}

try {
    $clientId = 6982;

    callHolly('EditUserExtraFields', [
        'studentClientId' => $clientId,
        'fields' => [
            [
                'name' => 'Ответственный',
                'value' => 'Алина',
            ],
            [
                'name' => 'Тип обучения',
                'value' => 'Индивидуально',
            ],
            [
                'name' => 'Языковой клуб',
                'value' => 'Да',
            ],
            [
                'name' => 'Дата окончания действия языкового клуба',
                'value' => '31.12.2026',
            ],
            [
                'name' => 'Комбо платформа (ОС)',
                'value' => 'Да',
            ],
        ],
    ]);

    echo "DONE\n";
} catch (Throwable $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}
