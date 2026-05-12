<?php

declare(strict_types=1);

header('Content-Type: text/plain; charset=utf-8');

$hook = file_get_contents(__DIR__ . '/amo_coordinators_hook.php');

function extractConst(string $code, string $name): string
{
    if (preg_match("/const\s+" . preg_quote($name, '/') . "\s*=\s*'([^']*)';/", $code, $m)) {
        return $m[1];
    }
    if (preg_match("/const\s+" . preg_quote($name, '/') . "\s*=\s*(\d+);/", $code, $m)) {
        return $m[1];
    }
    return '';
}

$baseUrl = extractConst($hook, 'SOURCE_AMO_BASE_URL');
$token = extractConst($hook, 'SOURCE_AMO_ACCESS_TOKEN');
$fieldId = 1775805; // Уровень

if ($baseUrl === '' || $token === '') {
    echo "Не нашел SOURCE_AMO_BASE_URL или SOURCE_AMO_ACCESS_TOKEN\n";
    exit;
}

$ch = curl_init();
curl_setopt_array($ch, [
    CURLOPT_URL => rtrim($baseUrl, '/') . '/api/v4/leads/custom_fields/' . $fieldId,
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HTTPGET => true,
    CURLOPT_HTTPHEADER => [
        'Authorization: Bearer ' . $token,
        'Content-Type: application/json',
    ],
    CURLOPT_TIMEOUT => 60,
]);

$response = curl_exec($ch);
$httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
$error = curl_error($ch);
curl_close($ch);

if ($error !== '') {
    echo "cURL error: {$error}\n";
    exit;
}

echo "HTTP: {$httpCode}\n\n";

$data = json_decode((string) $response, true);
if (!is_array($data)) {
    echo $response;
    exit;
}

echo "FIELD ID: " . ($data['id'] ?? '') . "\n";
echo "FIELD NAME: " . ($data['name'] ?? '') . "\n";
echo "FIELD TYPE: " . ($data['type'] ?? '') . "\n\n";

echo "ENUMS:\n";
foreach (($data['enums'] ?? []) as $enum) {
    echo ($enum['id'] ?? '') . " | " . ($enum['value'] ?? '') . "\n";
}
