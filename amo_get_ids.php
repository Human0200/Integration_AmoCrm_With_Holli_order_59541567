<?php

declare(strict_types=1);

require_once __DIR__ . '/config.php';
require_once __DIR__ . '/amo_func.php';

header('Content-Type: text/plain; charset=utf-8');

function amo_api_get(string $path): array
{
    $subdomain = $GLOBALS['subdomain'] ?? '';
    $data = $GLOBALS['data'] ?? [];

    $accessToken = $data['access_token'] ?? $data['accessToken'] ?? null;
    if (!$subdomain || !$accessToken) {
        throw new RuntimeException('Нет subdomain/access_token в config.php/amo_func.php');
    }

    $url = 'https://' . $subdomain . '.amocrm.ru' . $path;

    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPGET => true,
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $accessToken,
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
        throw new RuntimeException("amo API HTTP {$httpCode}: " . mb_substr((string) $response, 0, 1000));
    }

    return is_array($decoded) ? $decoded : [];
}

function norm(string $s): string
{
    $s = mb_strtolower(trim($s), 'UTF-8');
    $s = str_replace('ё', 'е', $s);
    return preg_replace('/\s+/u', ' ', $s) ?? $s;
}

try {
    echo "=== USERS ===\n";
    $users = amo_api_get('/api/v4/users');
    foreach (($users['_embedded']['users'] ?? []) as $user) {
        echo ($user['id'] ?? '') . " | " . ($user['name'] ?? '') . " | " . ($user['email'] ?? '') . "\n";
    }

    echo "\n=== PIPELINES / STATUSES ===\n";
    $pipelines = amo_api_get('/api/v4/leads/pipelines');
    foreach (($pipelines['_embedded']['pipelines'] ?? []) as $pipeline) {
        echo "\nPIPELINE: " . ($pipeline['id'] ?? '') . " | " . ($pipeline['name'] ?? '') . "\n";
        foreach (($pipeline['_embedded']['statuses'] ?? []) as $status) {
            echo "  STATUS: " . ($status['id'] ?? '') . " | " . ($status['name'] ?? '') . "\n";
        }
    }

    echo "\n=== LEAD CUSTOM FIELDS ===\n";
    $leadFields = amo_api_get('/api/v4/leads/custom_fields');
    foreach (($leadFields['_embedded']['custom_fields'] ?? []) as $field) {
        $name = (string) ($field['name'] ?? '');
        $n = norm($name);

        if (
            str_contains($n, 'дата рождения') ||
            str_contains($n, 'языковой клуб') ||
            str_contains($n, 'комбо') ||
            str_contains($n, 'ссылка') ||
            str_contains($n, 'ответственный') ||
            str_contains($n, 'вид занятий') ||
            str_contains($n, 'филиал') ||
            str_contains($n, 'уровень') ||
            str_contains($n, 'язык')
        ) {
            echo ($field['id'] ?? '') . " | " . $name . " | type=" . ($field['type'] ?? '') . "\n";
        }
    }

    echo "\n=== CONTACT CUSTOM FIELDS ===\n";
    $contactFields = amo_api_get('/api/v4/contacts/custom_fields');
    foreach (($contactFields['_embedded']['custom_fields'] ?? []) as $field) {
        $name = (string) ($field['name'] ?? '');
        $n = norm($name);

        if (
            str_contains($n, 'телефон') ||
            str_contains($n, 'email') ||
            str_contains($n, 'e-mail') ||
            str_contains($n, 'должность') ||
            str_contains($n, 'кем является')
        ) {
            echo ($field['id'] ?? '') . " | " . $name . " | type=" . ($field['type'] ?? '') . "\n";
        }
    }

} catch (Throwable $e) {
    echo "ERROR: " . $e->getMessage() . "\n";
}
