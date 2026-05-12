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
$leadId = 42048053;

$ch = curl_init();
curl_setopt_array($ch, [
    CURLOPT_URL => rtrim($baseUrl, '/') . '/api/v4/leads/' . $leadId,
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

echo "HTTP: {$httpCode}\n";
if ($error !== '') {
    echo "ERROR: {$error}\n";
    exit;
}

$data = json_decode((string) $response, true);
if (!is_array($data)) {
    echo $response;
    exit;
}

echo "LEAD: " . ($data['id'] ?? '') . " | " . ($data['name'] ?? '') . "\n\n";

foreach (($data['custom_fields_values'] ?? []) as $field) {
    echo "FIELD_ID: " . ($field['field_id'] ?? '') . "\n";
    echo "FIELD_NAME: " . ($field['field_name'] ?? '') . "\n";
    echo "FIELD_TYPE: " . ($field['field_type'] ?? '') . "\n";
    echo "VALUES:\n";
    print_r($field['values'] ?? []);
    echo str_repeat('-', 60) . "\n";
}
