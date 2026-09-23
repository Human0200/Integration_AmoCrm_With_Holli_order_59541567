<?php
require_once dirname(__DIR__) . '/config.php';
$tokens = json_decode((string) file_get_contents(dirname(__DIR__) . '/tokens.json'), true);
$ch = curl_init('https://directorchinatutorru.amocrm.ru/api/v4/bots/25585/run');
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_CUSTOMREQUEST => 'POST',
    CURLOPT_POSTFIELDS => json_encode(['entity_id' => 31579761, 'entity_type' => 'leads']),
    CURLOPT_HTTPHEADER => ['Authorization: Bearer ' . $tokens['access_token'], 'Content-Type: application/json'],
]);
$body = curl_exec($ch);
$result = ['http' => curl_getinfo($ch, CURLINFO_HTTP_CODE), 'body' => $body, 'curl_error' => curl_error($ch)];
curl_close($ch);
echo json_encode($result, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) . PHP_EOL;
