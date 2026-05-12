<?php

declare(strict_types=1);

header('Content-Type: text/plain; charset=utf-8');

$hookFile = __DIR__ . '/amo_coordinators_hook.php';
$logFile = __DIR__ . '/logs/coordinators.log';

$hook = file_get_contents($hookFile);
$log = file_exists($logFile) ? file_get_contents($logFile) : '';

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

function amoGet(string $baseUrl, string $token, string $path): array
{
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => rtrim($baseUrl, '/') . $path,
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
        throw new RuntimeException('cURL error: ' . $error);
    }

    $decoded = json_decode((string) $response, true);

    if ($httpCode < 200 || $httpCode >= 300) {
        throw new RuntimeException("amo HTTP {$httpCode}: " . mb_substr((string) $response, 0, 1500, 'UTF-8'));
    }

    return is_array($decoded) ? $decoded : [];
}

function fieldValueById(array $lead, int $fieldId): string
{
    foreach (($lead['custom_fields_values'] ?? []) as $field) {
        if ((int) ($field['field_id'] ?? 0) !== $fieldId) {
            continue;
        }

        $values = [];
        foreach (($field['values'] ?? []) as $value) {
            $values[] = (string) ($value['value'] ?? '');
        }

        return implode(', ', array_filter($values));
    }

    return '';
}

$baseUrl = extractConst($hook, 'SOURCE_AMO_BASE_URL');
$token = extractConst($hook, 'SOURCE_AMO_ACCESS_TOKEN');

if ($baseUrl === '' || $token === '') {
    throw new RuntimeException('Не нашел SOURCE_AMO_BASE_URL / SOURCE_AMO_ACCESS_TOKEN в amo_coordinators_hook.php');
}

if (!preg_match_all('/"amo_lead_id"\s*:\s*(\d+)|amo_lead_id[^0-9]+(\d+)/u', $log, $matches)) {
    throw new RuntimeException('Не нашел amo_lead_id в coordinators.log');
}

$ids = [];
foreach ($matches[1] as $i => $v) {
    $id = (int) ($v ?: $matches[2][$i] ?? 0);
    if ($id > 0) {
        $ids[] = $id;
    }
}

$leadId = end($ids);
if (!$leadId) {
    throw new RuntimeException('Не удалось определить последний amo_lead_id');
}

$lead = amoGet($baseUrl, $token, '/api/v4/leads/' . $leadId . '?with=contacts');
$pipelineId = (int) ($lead['pipeline_id'] ?? 0);
$statusId = (int) ($lead['status_id'] ?? 0);
$responsibleUserId = (int) ($lead['responsible_user_id'] ?? 0);

$contacts = $lead['_embedded']['contacts'] ?? [];

$fieldIds = [
    'Язык' => (int) extractConst($hook, 'AMO_FIELD_DISCIPLINE'),
    'Уровень' => (int) extractConst($hook, 'AMO_FIELD_LEVEL'),
    'Вид занятий' => (int) extractConst($hook, 'AMO_FIELD_LEARNING_FORMAT'),
    'Филиал' => (int) extractConst($hook, 'AMO_FIELD_OFFICE'),
    'Ссылка на ХХ' => (int) extractConst($hook, 'AMO_FIELD_HOLLY_LINK'),
    'Дата рождения' => (int) extractConst($hook, 'AMO_FIELD_BIRTHDATE'),
    'Языковой клуб' => (int) extractConst($hook, 'AMO_FIELD_LANGUAGE_CLUB'),
    'Дата окончания языкового клуба' => (int) extractConst($hook, 'AMO_FIELD_LANGUAGE_CLUB_UNTIL'),
    'Комбо платформа' => (int) extractConst($hook, 'AMO_FIELD_COMBO_PLATFORM_OS'),
];

echo "=== LAST CREATED/UPDATED LEAD ===\n";
echo "lead_id: {$leadId}\n";
echo "name: " . ($lead['name'] ?? '') . "\n";
echo "url: " . rtrim($baseUrl, '/') . "/leads/detail/{$leadId}\n";
echo "pipeline_id: {$pipelineId}\n";
echo "status_id: {$statusId}\n";
echo "responsible_user_id: {$responsibleUserId}\n";

echo "\n=== EXPECTED ROUTING IDS ===\n";
echo "GENERAL: pipeline " . extractConst($hook, 'AMO_COORD_GENERAL_PIPELINE_ID') . " / status " . extractConst($hook, 'AMO_COORD_GENERAL_STATUS_ID') . "\n";
echo "ALINA:   user " . extractConst($hook, 'AMO_COORD_ALINA_USER_ID') . " / pipeline " . extractConst($hook, 'AMO_COORD_ALINA_PIPELINE_ID') . " / status " . extractConst($hook, 'AMO_COORD_ALINA_STATUS_ID') . "\n";
echo "IRINA:   user " . extractConst($hook, 'AMO_COORD_IRINA_USER_ID') . " / pipeline " . extractConst($hook, 'AMO_COORD_IRINA_PIPELINE_ID') . " / status " . extractConst($hook, 'AMO_COORD_IRINA_STATUS_ID') . "\n";
echo "SHABO:   user " . extractConst($hook, 'AMO_COORD_SHABO_USER_ID') . " / pipeline " . extractConst($hook, 'AMO_COORD_SHABO_PIPELINE_ID') . " / status " . extractConst($hook, 'AMO_COORD_SHABO_STATUS_ID') . "\n";

echo "\n=== FIELDS ===\n";
foreach ($fieldIds as $name => $fieldId) {
    echo $name . " [" . $fieldId . "]: " . fieldValueById($lead, $fieldId) . "\n";
}

echo "\n=== CONTACTS LINKED ===\n";
echo "contacts_count: " . count($contacts) . "\n";
foreach ($contacts as $contact) {
    echo "- contact_id: " . ($contact['id'] ?? '') . ", is_main: " . (($contact['is_main'] ?? false) ? 'Y' : 'N') . "\n";
}

echo "\n=== RAW CHECK ===\n";
echo "По ТЗ должно быть: Алина/Ирина/Шабо -> своя воронка + статус Новый студент; пустой/другой ответственный -> Общая. Также должны быть поля, ссылка на Холи и контакты студента/плательщика.\n";
