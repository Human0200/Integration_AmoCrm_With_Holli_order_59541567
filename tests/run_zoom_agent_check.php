<?php
declare(strict_types=1);

require_once dirname(__DIR__) . '/config.php';

$leadId = 31579761;
$tokens = json_decode((string) file_get_contents(dirname(__DIR__) . '/tokens.json'), true);
$now = time();
$start = $now + 23 * 3600;
$end = $start + 3600;
$payload = [[
    'id' => $leadId,
    'custom_fields_values' => [
        ['field_id' => 1639961, 'values' => [['value' => $start]]],
        ['field_id' => 1639963, 'values' => [['value' => $end]]],
    ],
]];

$ch = curl_init('https://directorchinatutorru.amocrm.ru/api/v4/leads');
curl_setopt_array($ch, [
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_CUSTOMREQUEST => 'PATCH',
    CURLOPT_POSTFIELDS => json_encode($payload),
    CURLOPT_HTTPHEADER => [
        'Authorization: Bearer ' . $tokens['access_token'],
        'Content-Type: application/json',
    ],
]);
$body = curl_exec($ch);
$code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);
echo json_encode(['http' => $code, 'start' => $start, 'end' => $end, 'response' => json_decode((string) $body, true)], JSON_UNESCAPED_SLASHES) . PHP_EOL;

foreach ([$now, $now + 1] as $modified) {
    $ch = curl_init('https://srm.chinatutor.ru/index.php');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => http_build_query(['leads' => ['update' => [['id' => (string) $leadId, 'updated_at' => (string) $modified, 'last_modified' => (string) $modified]]]]),
    ]);
    echo 'webhook=' . curl_getinfo($ch, CURLINFO_HTTP_CODE) . ' body=' . curl_exec($ch) . PHP_EOL;
    curl_close($ch);
    usleep(500000);
}
